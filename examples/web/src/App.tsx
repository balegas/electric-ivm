import type { Collection } from '@tanstack/db'
import { useLiveQuery } from '@tanstack/react-db'
import { useEffect, useState } from 'react'

import type { ShapeMaterialization } from '@electric-lite/client'

import { client } from './electric'
import { getShapes, type Shapes } from './shapes'

interface Todo {
  id: string
  title: string
  priority: number
  done: boolean
}

// Render a shape's TanStack DB collection reactively via @tanstack/react-db's useLiveQuery.
function useRows(mat: ShapeMaterialization): Todo[] {
  const coll = mat.collection as Collection<Todo, string>
  const { data } = useLiveQuery(
    (q) =>
      q
        .from({ t: coll })
        .select(({ t }) => ({ id: t.id, title: t.title, priority: t.priority, done: t.done })),
    [coll],
  )
  return (data as Todo[]).map((r) => ({ ...r, id: String(r.id) }))
}

export function App(): JSX.Element {
  const [shapes, setShapes] = useState<Shapes | null>(null)
  useEffect(() => {
    getShapes().then(setShapes)
  }, [])
  if (!shapes) return <div className="loading">Connecting to electric-lite…</div>
  return <Board shapes={shapes} />
}

function Board({ shapes }: { shapes: Shapes }): JSX.Element {
  const all = useRows(shapes.all)
  const live = useRows(shapes.live)
  const nextId = all.reduce((m, t) => Math.max(m, Number(t.id)), 0) + 1

  const addTodo = (title: string, priority: number) =>
    client.tables.todos.insert({ id: nextId, title, priority, done: false })
  const toggle = (t: Todo) =>
    client.tables.todos.update({ id: Number(t.id), title: t.title, priority: t.priority, done: !t.done })
  const bump = (t: Todo, d: number) =>
    client.tables.todos.update({ id: Number(t.id), title: t.title, priority: Math.min(5, Math.max(1, t.priority + d)), done: t.done })
  const remove = (t: Todo) => client.tables.todos.delete(Number(t.id))

  const liveSorted = [...live].sort((a, b) => b.priority - a.priority || Number(a.id) - Number(b.id))
  const allSorted = [...all].sort((a, b) => Number(a.id) - Number(b.id))

  return (
    <div className="app">
      <header>
        <h1>electric-lite</h1>
        <p>
          A reactive database: writes flow through durable streams → a dbsp filter circuit per shape →
          materialized live in the browser with <strong>stream-db + TanStack DB</strong>.
        </p>
      </header>

      <div className="columns">
        <section className="card">
          <h2>todos <span className="muted">· all rows (match-all shape)</span></h2>
          <AddForm onAdd={addTodo} />
          <table>
            <thead>
              <tr><th>#</th><th>title</th><th>priority</th><th>done</th><th></th></tr>
            </thead>
            <tbody>
              {allSorted.map((t) => (
                <tr key={t.id} className={t.done ? 'is-done' : ''}>
                  <td className="muted">{t.id}</td>
                  <td>{t.title}</td>
                  <td>
                    <button className="ghost" onClick={() => bump(t, -1)}>–</button>
                    <span className={`pri pri-${t.priority}`}>{t.priority}</span>
                    <button className="ghost" onClick={() => bump(t, +1)}>+</button>
                  </td>
                  <td>
                    <input type="checkbox" checked={t.done} onChange={() => toggle(t)} />
                  </td>
                  <td><button className="ghost danger" onClick={() => remove(t)}>✕</button></td>
                </tr>
              ))}
              {allSorted.length === 0 && (
                <tr><td colSpan={5} className="empty">no todos yet — add one above</td></tr>
              )}
            </tbody>
          </table>
        </section>

        <section className="card live">
          <h2>
            live shape <span className="count">{live.length}</span>
          </h2>
          <p className="predicate">
            <code>done = false AND priority &gt;= 3</code>
          </p>
          <ul className="shape-list">
            {liveSorted.map((t) => (
              <li key={t.id} className="enter">
                <span className={`pri pri-${t.priority}`}>P{t.priority}</span>
                <span className="title">{t.title}</span>
                <span className="muted">#{t.id}</span>
              </li>
            ))}
            {liveSorted.length === 0 && <li className="empty">nothing matches — complete fewer / raise priority</li>}
          </ul>
          <p className="hint">
            Toggle <em>done</em> or change a priority on the left and watch rows enter and leave this
            shape live — that's the dbsp circuit emitting deltas to the shape stream.
          </p>
        </section>
      </div>
    </div>
  )
}

function AddForm({ onAdd }: { onAdd: (title: string, priority: number) => void }): JSX.Element {
  const [title, setTitle] = useState('')
  const [priority, setPriority] = useState(3)
  return (
    <form
      className="add-form"
      onSubmit={(e) => {
        e.preventDefault()
        const t = title.trim()
        if (!t) return
        onAdd(t, priority)
        setTitle('')
        setPriority(3)
      }}
    >
      <input placeholder="new todo…" value={title} onChange={(e) => setTitle(e.target.value)} />
      <label>
        priority
        <select value={priority} onChange={(e) => setPriority(Number(e.target.value))}>
          {[1, 2, 3, 4, 5].map((p) => (
            <option key={p} value={p}>{p}</option>
          ))}
        </select>
      </label>
      <button type="submit">add</button>
    </form>
  )
}
