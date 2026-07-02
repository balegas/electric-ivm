// The playground shell: world (left) → pipeline (center) → subscribers (right), scenes below.
// One consistent screen; scenes only change which shapes exist and what the explainer says.

import { useCallback, useEffect, useState } from 'react'

import type { EngineGraph } from '@viz/types'

import { api } from './api.ts'
import { DeviceCards } from './DeviceCards.tsx'
import { PipelineCanvas, type View } from './PipelineCanvas.tsx'
import { SceneStrip, currentScene } from './SceneStrip.tsx'
import { SubsetBoard } from './SubsetBoard.tsx'
import { useWorkspace } from './useWorkspace.ts'
import { hasSeenWelcome, markWelcomeSeen, Welcome } from './Welcome.tsx'
import { WorldPanel } from './WorldPanel.tsx'

const GRAPH_POLL_MS = 2500

export default function App() {
  const w = useWorkspace()
  const [scene, setScene] = useState(currentScene())
  const [view, setView] = useState<View>('logical')
  const [graph, setGraph] = useState<EngineGraph | null>(null)
  const [mine, setMine] = useState<string[]>([])
  const [welcomeOpen, setWelcomeOpen] = useState(!hasSeenWelcome())

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

  // Entering a scene provisions its shapes (idempotent), then refreshes the graph.
  const enterScene = useCallback(
    (n: number) => {
      setScene(n)
      void w.enterScene(n).then(loadGraph)
    },
    [w.enterScene, loadGraph], // eslint-disable-line react-hooks/exhaustive-deps
  )
  // Provision on boot: scene 1's base shape always exists (so a wiped-and-reminted workspace is
  // never empty), then the stored scene's shapes.
  const ready = w.status === 'ready'
  useEffect(() => {
    if (!ready) return
    void (async () => {
      await w.enterScene(1)
      if (scene !== 1) await w.enterScene(scene)
      await loadGraph()
    })()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ready])

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <b>electric-ivm</b> dbsp playground
          <span className="brand-sub">an interactive visualization of a sync engine over Postgres</span>
        </div>
        <div className="topbar-r">
          {w.error ? <span className="toast">{w.error}</span> : null}
          <span className="ws-chip" title="Your workspace — all your rows and shapes carry this id">
            {wsId ?? '…'}
          </span>
          <button className="tbtn" onClick={() => setWelcomeOpen(true)} title="What is this?">
            <span className="tbtn-ico">?</span> about
          </button>
          <button className="tbtn" onClick={() => void w.reprovision()} title="Start over in a fresh workspace">
            <span className="tbtn-ico">↺</span> new workspace
          </button>
        </div>
      </header>

      <div className="cols">
        <aside className="col-left">
          {w.state ? (
            <WorldPanel
              restaurants={w.state.restaurants}
              orders={w.state.orders}
              showMoveCity={scene >= 4}
              act={(v) => void w.act(v)}
            />
          ) : (
            <div className="loading">provisioning workspace…</div>
          )}
        </aside>

        <main className="col-center">
          <PipelineCanvas workspaceId={wsId} graph={graph} mine={mine} view={view} onViewChange={setView} />
        </main>

        <aside className="col-right">
          {scene === 6 ? <SubsetBoard workspaceId={wsId} /> : null}
          <DeviceCards
            workspaceId={wsId}
            shapes={w.state?.shapes ?? []}
            tick={w.actionTick}
            deleteShape={(id) => void w.deleteShape(id)}
          />
        </aside>
      </div>

      <SceneStrip scene={scene} onScene={enterScene} />

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
            <div className="modal-h">This workspace was reset</div>
            <p>
              The playground server was wiped (it happens — it's a demo). Your data is gone, but a fresh
              workspace is one click away.
            </p>
            <div className="modal-actions">
              <button className="primary" onClick={() => void w.reprovision()}>
                Get a new workspace
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
