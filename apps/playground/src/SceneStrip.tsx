// The bottom strip: the walkthrough. Six scenes, each an explainer + pre-provisioned shapes.
// Entering a scene is idempotent (re-entering creates nothing new); data persists across scenes;
// free play is always available.

import { useState } from 'react'

import { SCENES } from '../shared/scenes.ts'

const SCENE_KEY = 'playground-scene'

export function currentScene(): number {
  return Number(localStorage.getItem(SCENE_KEY) ?? 1)
}

export function SceneStrip({ scene, onScene }: { scene: number; onScene: (n: number) => void }) {
  const [open, setOpen] = useState(true)
  const def = SCENES.find((s) => s.n === scene)
  return (
    <div className={`scenes${open ? '' : ' scenes-closed'}`}>
      <div className="scenes-tabs">
        {SCENES.map((s) => (
          <button
            key={s.n}
            className={`scene-tab${s.n === scene ? ' scene-on' : ''}`}
            onClick={() => {
              localStorage.setItem(SCENE_KEY, String(s.n))
              onScene(s.n)
              setOpen(true)
            }}
          >
            <span className="scene-n">{s.n}</span> {s.title}
          </button>
        ))}
        <button className="scenes-toggle" onClick={() => setOpen((o) => !o)}>
          {open ? '▾' : '▴'}
        </button>
      </div>
      {open && def ? (
        <div className="scene-card">
          <p className="scene-body">{def.body}</p>
          <div className="scene-try">
            {def.try.map((t) => (
              <span key={t} className="try-chip">
                → {t}
              </span>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  )
}
