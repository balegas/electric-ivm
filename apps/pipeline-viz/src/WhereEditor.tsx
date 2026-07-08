import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'

/**
 * Schema-aware, syntax-highlighting editor for the create-shape WHERE clause. It stays free-text
 * (a plain textarea the user types into) but layers two aids on top:
 *
 *   1. Live syntax highlighting — the text is tokenized on every keystroke and a scroll-synced
 *      overlay renders the same characters as colored <span>s behind a transparent-text textarea
 *      (the classic "highlighted overlay" trick — no editor dependency).
 *   2. Column/keyword/operator autocomplete — a dropdown scoped to the shape-predicate grammar the
 *      engine accepts (leaf comparisons, AND/OR/NOT, parens, IS [NOT] NULL, IN (SELECT …)).
 *
 * The grammar the engine's shape predicate allows:
 *   predicate := leaf | predicate AND/OR predicate | NOT predicate | ( predicate )
 *   leaf      := col <op> literal | col IS [NOT] NULL | col IN ( SELECT col FROM table [WHERE predicate] )
 */

/** Column shape from GET /engine/table/{table}/schema. */
export interface TableColumn {
  name: string
  type: string
  pgType: string | null
  pk: boolean
  hasDefault?: boolean
}

export type TokKind =
  | 'ws'
  | 'column' // identifier that matches a real column of the selected table
  | 'table' // identifier that matches a known table (subquery FROM target)
  | 'ident' // identifier that matches neither → flagged as unknown
  | 'keyword' // AND OR NOT IS IN SELECT FROM
  | 'literal' // TRUE FALSE NULL
  | 'op' // = <> != < <= > >=
  | 'string' // '…'
  | 'number' // 123 / 1.5
  | 'punct' // ( ) ,
  | 'unknown' // any stray character

export interface Tok {
  kind: TokKind
  text: string
  start: number
  end: number // exclusive
}

const KEYWORDS = new Set(['AND', 'OR', 'NOT', 'IS', 'IN', 'SELECT', 'FROM', 'WHERE'])
const LITERALS = new Set(['TRUE', 'FALSE', 'NULL'])
const IDENT_START = /[A-Za-z_]/
const IDENT_PART = /[A-Za-z0-9_]/

const isIdentLike = (k: TokKind) =>
  k === 'column' || k === 'table' || k === 'ident' || k === 'keyword' || k === 'literal'

/**
 * Tokenize a WHERE clause. Identifiers are classified against the selected table's columns
 * (case-insensitively, matching Postgres unquoted-identifier folding) and the known table set, so
 * the overlay can color real columns distinctly from unknown identifiers. This is a pure function —
 * it drives both the highlight overlay and the completion engine.
 */
export function tokenize(src: string, columns: Set<string>, tables: Set<string>): Tok[] {
  const toks: Tok[] = []
  const n = src.length
  let i = 0
  const classifyWord = (text: string, quoted: boolean): TokKind => {
    if (!quoted) {
      const up = text.toUpperCase()
      if (KEYWORDS.has(up)) return 'keyword'
      if (LITERALS.has(up)) return 'literal'
    }
    const key = quoted ? text : text.toLowerCase()
    if (columns.has(key)) return 'column'
    if (tables.has(key)) return 'table'
    return 'ident'
  }
  while (i < n) {
    const c = src[i]!
    if (/\s/.test(c)) {
      let j = i + 1
      while (j < n && /\s/.test(src[j]!)) j++
      toks.push({ kind: 'ws', text: src.slice(i, j), start: i, end: j })
      i = j
      continue
    }
    // single-quoted string literal, with '' escapes
    if (c === "'") {
      let j = i + 1
      while (j < n) {
        if (src[j] === "'") {
          if (src[j + 1] === "'") {
            j += 2
            continue
          }
          j++
          break
        }
        j++
      }
      toks.push({ kind: 'string', text: src.slice(i, j), start: i, end: j })
      i = j
      continue
    }
    // double-quoted identifier
    if (c === '"') {
      let j = i + 1
      while (j < n && src[j] !== '"') j++
      const closed = j < n
      const inner = src.slice(i + 1, j)
      if (closed) j++ // consume closing quote
      toks.push({ kind: classifyWord(inner, true), text: src.slice(i, j), start: i, end: j })
      i = j
      continue
    }
    // number literal
    if (/[0-9]/.test(c) || (c === '.' && /[0-9]/.test(src[i + 1] ?? ''))) {
      let j = i + 1
      while (j < n && /[0-9.]/.test(src[j]!)) j++
      toks.push({ kind: 'number', text: src.slice(i, j), start: i, end: j })
      i = j
      continue
    }
    // identifier / keyword
    if (IDENT_START.test(c)) {
      let j = i + 1
      while (j < n && IDENT_PART.test(src[j]!)) j++
      const text = src.slice(i, j)
      toks.push({ kind: classifyWord(text, false), text, start: i, end: j })
      i = j
      continue
    }
    // comparison operators (two-char first)
    const two = src.slice(i, i + 2)
    if (two === '<>' || two === '!=' || two === '<=' || two === '>=') {
      toks.push({ kind: 'op', text: two, start: i, end: i + 2 })
      i += 2
      continue
    }
    if (c === '=' || c === '<' || c === '>') {
      toks.push({ kind: 'op', text: c, start: i, end: i + 1 })
      i++
      continue
    }
    if (c === '(' || c === ')' || c === ',') {
      toks.push({ kind: 'punct', text: c, start: i, end: i + 1 })
      i++
      continue
    }
    toks.push({ kind: 'unknown', text: c, start: i, end: i + 1 })
    i++
  }
  return toks
}

export interface Suggestion {
  label: string
  /** Text inserted in place of the word under the caret. */
  insert: string
  kind: 'column' | 'keyword' | 'op' | 'literal' | 'snippet'
  detail?: string
  /** Append a space after inserting (unless the caret is already followed by whitespace). */
  space?: boolean
}

export interface Completion {
  items: Suggestion[]
  /** Replace [from, to) — the identifier/word currently under the caret (empty span = pure insert). */
  from: number
  to: number
}

const OPERATORS: Suggestion[] = [
  { label: '=', insert: '=', kind: 'op', detail: 'equals', space: true },
  { label: '<>', insert: '<>', kind: 'op', detail: 'not equal', space: true },
  { label: '<', insert: '<', kind: 'op', detail: 'less than', space: true },
  { label: '<=', insert: '<=', kind: 'op', detail: 'less or equal', space: true },
  { label: '>', insert: '>', kind: 'op', detail: 'greater than', space: true },
  { label: '>=', insert: '>=', kind: 'op', detail: 'greater or equal', space: true },
  { label: 'IS', insert: 'IS', kind: 'keyword', detail: 'IS [NOT] NULL', space: true },
  { label: 'IN', insert: 'IN', kind: 'keyword', detail: 'IN (SELECT …)', space: true },
]
const CONNECTORS: Suggestion[] = [
  { label: 'AND', insert: 'AND', kind: 'keyword', space: true },
  { label: 'OR', insert: 'OR', kind: 'keyword', space: true },
]
const CLAUSE_KW: Suggestion[] = [
  { label: 'NOT', insert: 'NOT', kind: 'keyword', detail: 'negate', space: true },
  { label: '(', insert: '(', kind: 'snippet', detail: 'group' },
]
const RHS_LITERALS: Suggestion[] = [
  { label: 'TRUE', insert: 'TRUE', kind: 'literal', space: true },
  { label: 'FALSE', insert: 'FALSE', kind: 'literal', space: true },
  { label: 'NULL', insert: 'NULL', kind: 'literal', space: true },
]

const kwUp = (t: Tok | null, w: string) => !!t && t.kind === 'keyword' && t.text.toUpperCase() === w

/**
 * Compute the completion menu for the caret position, scoped to the shape-predicate grammar. The
 * common (top-level) case is fully wired: a column at clause start, an operator/IS/IN after a
 * column, connectors after a completed leaf, literals on a comparison RHS. Subquery completion is
 * handled to a first approximation — after FROM we offer known tables; inside the SELECT list we
 * offer the outer columns (see the note in the component doc) — while highlighting stays exact.
 */
export function complete(
  src: string,
  caret: number,
  cols: TableColumn[],
  colSet: Set<string>,
  tables: Set<string>,
): Completion {
  const toks = tokenize(src, colSet, tables)
  // The word being edited: an identifier-like token the caret sits inside or at the end of.
  let word: Tok | null = null
  for (const t of toks) {
    if (isIdentLike(t.kind) && caret > t.start && caret <= t.end) {
      word = t
      break
    }
  }
  const from = word ? word.start : caret
  const to = word ? word.end : caret
  const prefix = src.slice(from, caret)

  // Significant (non-whitespace) tokens strictly before the word/caret.
  const sig = toks.filter((t) => t.kind !== 'ws' && t.end <= from)
  const prev = sig[sig.length - 1] ?? null
  const prev2 = sig[sig.length - 2] ?? null

  const columnItems: Suggestion[] = cols.map((c) => ({
    label: c.name,
    insert: /^[A-Za-z_][A-Za-z0-9_]*$/.test(c.name) ? c.name : `"${c.name}"`,
    kind: 'column',
    detail: c.pgType ?? c.type,
    space: true,
  }))
  const tableItems: Suggestion[] = [...tables].map((t) => ({
    label: t,
    insert: t,
    kind: 'keyword',
    detail: 'table',
    space: true,
  }))
  const clauseStart = [...columnItems, ...CLAUSE_KW]

  let items: Suggestion[]
  if (prev === null || prev.kind === 'punct' || kwUp(prev, 'AND') || kwUp(prev, 'OR') || kwUp(prev, 'WHERE')) {
    // Start of a predicate / group / after a connector, comma, or subquery WHERE → an operand.
    items = prev && prev.kind === 'punct' && prev.text === ')' ? CONNECTORS : clauseStart
  } else if (prev.kind === 'table') {
    // Inside a subquery FROM: offer the subquery's WHERE or the closing paren.
    items = [
      { label: 'WHERE', insert: 'WHERE', kind: 'keyword', space: true },
      { label: ')', insert: ')', kind: 'snippet' },
    ]
  } else if (kwUp(prev, 'IS')) {
    items = [
      { label: 'NULL', insert: 'NULL', kind: 'literal', space: true },
      { label: 'NOT NULL', insert: 'NOT NULL', kind: 'snippet', space: true },
    ]
  } else if (kwUp(prev, 'IN')) {
    items = [{ label: '(SELECT', insert: '(SELECT ', kind: 'snippet', detail: 'subquery' }]
  } else if (kwUp(prev, 'NOT')) {
    // IS NOT NULL, or NOT <predicate>.
    items = kwUp(prev2, 'IS') ? [{ label: 'NULL', insert: 'NULL', kind: 'literal', space: true }] : clauseStart
  } else if (kwUp(prev, 'SELECT')) {
    // Subquery projection: outer columns are the best guess without the inner schema.
    items = columnItems
  } else if (kwUp(prev, 'FROM')) {
    items = tableItems
  } else if (prev.kind === 'op') {
    // Comparison RHS: a literal. We can only enumerate the keyword literals; strings/numbers are typed.
    items = RHS_LITERALS
  } else if (prev.kind === 'column' || prev.kind === 'ident') {
    // An operand awaiting an operator / IS / IN.
    items = OPERATORS
  } else {
    // After a completed literal (string/number/TRUE/FALSE/NULL) → connectors or close-paren.
    items = CONNECTORS
  }

  const pfx = prefix.toLowerCase()
  const filtered = pfx ? items.filter((s) => s.label.toLowerCase().startsWith(pfx)) : items
  return { items: filtered, from, to }
}

const TOKEN_CLASS: Record<TokKind, string> = {
  ws: '',
  column: 'we-column',
  table: 'we-table',
  ident: 'we-ident',
  keyword: 'we-keyword',
  literal: 'we-literal',
  op: 'we-op',
  string: 'we-string',
  number: 'we-number',
  punct: 'we-punct',
  unknown: 'we-unknown',
}

/**
 * The smart WHERE editor. `value`/`onChange` keep the clause in the parent form (unchanged create
 * flow); `onSubmit` fires on Enter only when the dropdown is closed (so Enter never both accepts a
 * suggestion and submits). Fetches the selected table's schema to drive highlighting + completion.
 */
export function WhereEditor({
  value,
  onChange,
  onSubmit,
  table,
  tables,
  placeholder,
}: {
  value: string
  onChange: (v: string) => void
  onSubmit: () => void
  table: string
  tables: string[]
  placeholder?: string
}) {
  const taRef = useRef<HTMLTextAreaElement>(null)
  const hlRef = useRef<HTMLDivElement>(null)
  const [cols, setCols] = useState<TableColumn[]>([])
  const [caret, setCaret] = useState(0)
  const [active, setActive] = useState(0)
  const [open, setOpen] = useState(false)
  const [focused, setFocused] = useState(false)
  // Caret to restore after a programmatic edit (accepting a suggestion), applied post-render.
  const pendingCaret = useRef<number | null>(null)

  // Fetch the selected table's schema (reusing the app's pattern). Non-fatal on failure — the
  // editor degrades to keyword/operator highlighting with no column awareness.
  useEffect(() => {
    let alive = true
    setCols([])
    if (!table) return
    void (async () => {
      try {
        const r = await fetch(`/engine/table/${encodeURIComponent(table)}/schema`)
        if (!r.ok) throw new Error(`schema → ${r.status}`)
        const s = (await r.json()) as { columns: TableColumn[] }
        if (alive) setCols(s.columns)
      } catch {
        /* non-fatal */
      }
    })()
    return () => {
      alive = false
    }
  }, [table])

  const colSet = useMemo(() => new Set(cols.map((c) => c.name.toLowerCase())), [cols])
  const tableSet = useMemo(() => new Set(tables), [tables])
  const tokens = useMemo(() => tokenize(value, colSet, tableSet), [value, colSet, tableSet])
  const completion = useMemo(
    () => complete(value, caret, cols, colSet, tableSet),
    [value, caret, cols, colSet, tableSet],
  )
  const items = completion.items
  const showMenu = open && focused && items.length > 0

  useEffect(() => {
    if (active >= items.length) setActive(0)
  }, [items, active])

  // Restore the caret after a programmatic value change (suggestion accept).
  useLayoutEffect(() => {
    if (pendingCaret.current != null && taRef.current) {
      const p = pendingCaret.current
      pendingCaret.current = null
      taRef.current.setSelectionRange(p, p)
      setCaret(p)
    }
  }, [value])

  const syncScroll = () => {
    if (hlRef.current && taRef.current) hlRef.current.scrollTop = taRef.current.scrollTop
  }
  const syncCaret = () => {
    const ta = taRef.current
    if (ta) setCaret(ta.selectionStart ?? 0)
  }

  const accept = (s: Suggestion) => {
    const { from, to } = completion
    const after = value.slice(to)
    const needsSpace = s.space && !/^\s/.test(after)
    const insert = s.insert + (needsSpace ? ' ' : '')
    const next = value.slice(0, from) + insert + after
    pendingCaret.current = from + insert.length
    onChange(next)
    setOpen(true) // recompute for the freshly inserted context (e.g. column → operators)
    setActive(0)
  }

  const onKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (showMenu) {
      if (e.key === 'ArrowDown') {
        e.preventDefault()
        setActive((a) => (a + 1) % items.length)
        return
      }
      if (e.key === 'ArrowUp') {
        e.preventDefault()
        setActive((a) => (a - 1 + items.length) % items.length)
        return
      }
      if (e.key === 'Enter' || e.key === 'Tab') {
        e.preventDefault()
        accept(items[active] ?? items[0]!)
        return
      }
      if (e.key === 'Escape') {
        e.preventDefault()
        e.stopPropagation()
        setOpen(false)
        return
      }
    }
    if (e.key === 'Enter') {
      // Dropdown closed → submit the create (matching the original input's behaviour); never a newline.
      e.preventDefault()
      onSubmit()
    }
  }

  return (
    <div className="whereed">
      <div className="whereed-box">
        <div className="whereed-hl" ref={hlRef} aria-hidden="true">
          {tokens.map((t, i) => (
            <span key={i} className={TOKEN_CLASS[t.kind]}>
              {t.text}
            </span>
          ))}
          {/* trailing newline keeps the overlay height in step with a value ending in a newline */}
          {'\n'}
        </div>
        <textarea
          ref={taRef}
          className="whereed-input"
          rows={2}
          spellCheck={false}
          autoComplete="off"
          autoCapitalize="off"
          placeholder={placeholder}
          value={value}
          onChange={(e) => {
            onChange(e.target.value)
            setCaret(e.target.selectionStart ?? 0)
            setOpen(true)
          }}
          onKeyDown={onKeyDown}
          onKeyUp={syncCaret}
          onClick={() => {
            syncCaret()
            setOpen(true)
          }}
          onSelect={syncCaret}
          onScroll={syncScroll}
          onFocus={() => {
            setFocused(true)
            setOpen(true)
          }}
          onBlur={() => setFocused(false)}
        />
      </div>
      {showMenu ? (
        <ul className="whereed-menu" onMouseDown={(e) => e.preventDefault()}>
          {items.map((s, i) => (
            <li
              key={`${s.kind}:${s.label}`}
              className={`whereed-item${i === active ? ' whereed-item-on' : ''}`}
              onMouseEnter={() => setActive(i)}
              onClick={() => {
                accept(s)
                taRef.current?.focus()
              }}
            >
              <span className={`whereed-item-l we-item-${s.kind}`}>{s.label}</span>
              {s.detail ? <span className="whereed-item-d">{s.detail}</span> : null}
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  )
}
