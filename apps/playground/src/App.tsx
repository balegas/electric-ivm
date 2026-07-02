// The playground shell: data grids (left) → pipeline (center) → live results (right), scenes
// below. One consistent screen; scenes only change which shapes exist and what the explainer
// says. Workspace scoping is silent — the "under the hood" toggle reveals it.

import { useCallback, useEffect, useState } from 'react'

import type { EngineGraph } from '@viz/types'

import { api } from './api.ts'
import { DataPanel } from './DataPanel.tsx'
import { PipelineCanvas, type View } from './PipelineCanvas.tsx'
import { SceneStrip, currentScene } from './SceneStrip.tsx'
import { ShapeBuilder } from './ShapeBuilder.tsx'
import { ShapeCards } from './ShapeCards.tsx'
import { SubsetBoard } from './SubsetBoard.tsx'
import { useWorkspace } from './useWorkspace.ts'
import { hasSeenWelcome, markWelcomeSeen, Welcome } from './Welcome.tsx'

const GRAPH_POLL_MS = 2500
const HOOD_KEY = 'playground-under-hood'

export default function App() {
  const w = useWorkspace()
  const [scene, setScene] = useState(currentScene())
  const [view, setView] = useState<View>('logical')
  const [graph, setGraph] = useState<EngineGraph | null>(null)
  const [mine, setMine] = useState<string[]>([])
  const [builderOpen, setBuilderOpen] = useState(false)
  const [welcomeOpen, setWelcomeOpen] = useState(!hasSeenWelcome())
  const [underHood, setUnderHood] = useState(localStorage.getItem(HOOD_KEY) === '1')

  const wsId = w.state?.workspace.id

  const loadGraph = useCallback(async () => {
    if (!wsId) return
    try {
      const g = await api.graph(wsId)
      setGraph(g.graph as unknown as EngineGraph)
      setMine(g.mine)
    } catch {
      /* workspace resets surface via useWorkspace's own calls */
    }
  }, [wsId])

  useEffect(() => {
    void loadGraph()
    const t = setInterval(() => void loadGraph(), GRAPH_POLL_MS)
    return () => clearInterval(t)
  }, [loadGraph, w.actionTick])

  const enterScene = useCallback(
    (n: number) => {
      setScene(n)
      void w.enterScene(n).then(loadGraph)
    },
    [w.enterScene, loadGraph], // eslint-disable-line react-hooks/exhaustive-deps
  )
  // Provision on boot AND whenever the workspace identity changes (new workspace / reset
  // recovery). Scene 0 is the deliberate empty state; from scene 1 on, scene 1's base shape
  // always exists, then the stored scene's shapes.
  const ready = w.status === 'ready'
  useEffect(() => {
    if (!ready || !wsId) return
    void (async () => {
      if (scene >= 1) {
        await w.enterScene(1)
        if (scene !== 1) await w.enterScene(scene)
      }
      await loadGraph()
    })()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ready, wsId])

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <b>electric-ivm</b> dbsp playground
          <span className="brand-sub">an interactive visualization of a sync engine</span>
        </div>
        <div className="topbar-r">
          {w.error ? <span className="toast">{w.error}</span> : null}
          <label className="hood" title="Reveal the multi-tenancy plumbing: full predicates, shared badges, other visitors' pulses">
            <input type="checkbox" checked={underHood} onChange={(e) => {
              setUnderHood(e.target.checked)
              localStorage.setItem(HOOD_KEY, e.target.checked ? '1' : '0')
            }} />
            under the hood
          </label>
          <button className="tbtn" onClick={() => setBuilderOpen(true)} disabled={!ready}>
            <span className="tbtn-ico">＋</span> shape
          </button>
          <button className="tbtn" onClick={() => setWelcomeOpen(true)} title="What is this?">
            <span className="tbtn-ico">?</span> about
          </button>
          <button
            className="tbtn"
            onClick={() => {
              setWelcomeOpen(true)
              void w.reprovision()
            }}
            title="Start over with fresh data"
          >
            <span className="tbtn-ico">↺</span> start over
          </button>
        </div>
      </header>

      <div className="cols">
        <aside className="col-left">
          {w.state ? (
            <DataPanel projects={w.state.projects} issues={w.state.issues} pending={w.pending > 0} act={(v) => void w.act(v)} />
          ) : (
            <div className="loading">setting things up…</div>
          )}
        </aside>

        <main className="col-center">
          <PipelineCanvas
            workspaceId={wsId}
            graph={graph}
            mine={mine}
            view={view}
            underHood={underHood}
            onViewChange={setView}
          />
        </main>

        <aside className="col-right">
          {scene === 6 ? <SubsetBoard workspaceId={wsId} /> : null}
          <ShapeCards
            workspaceId={wsId}
            shapes={w.state?.shapes ?? []}
            tick={w.actionTick}
            underHood={underHood}
            deleteShape={(id) => void w.deleteShape(id)}
          />
        </aside>
      </div>

      <SceneStrip scene={scene} view={view} onScene={enterScene} />

      {builderOpen ? (
        <ShapeBuilder onCreate={(spec, label) => void w.createShape(spec, label)} onClose={() => setBuilderOpen(false)} />
      ) : null}

      {welcomeOpen ? (
        <Welcome
          onClose={() => {
            markWelcomeSeen()
            setWelcomeOpen(false)
          }}
        />
      ) : null}

      {w.status === 'reset-needed' ? (
        <div className="modal-back">
          <div className="modal">
            <div className="modal-h">The demo was reset</div>
            <p>The server was wiped (it happens — it's a demo). Fresh data is one click away.</p>
            <div className="modal-actions">
              <button className="primary" onClick={() => void w.reprovision()}>
                Start fresh
              </button>
            </div>
          </div>
        </div>
      ) : null}
      {w.status === 'error' ? (
        <div className="modal-back">
          <div className="modal">
            <div className="modal-h">Can't reach the playground</div>
            <p>{w.error}</p>
            <div className="modal-actions">
              <button className="primary" onClick={() => window.location.reload()}>
                Retry
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  )
}
