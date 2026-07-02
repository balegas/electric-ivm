// The bottom strip: the walkthrough. Scenes explain one idea each; a scene's shapes are NOT
// created on entry — the card offers a "Create" button so you deliberately fire the Shape API
// call and watch the pipeline appear. Data persists across scenes; everything stays clickable.

import { useState } from 'react'

import { SCENES } from '../shared/scenes.ts'
import type { PlaygroundShape } from '../shared/types.ts'

const SCENE_KEY = 'playground-scene'

export function currentScene(): number {
  return Number(localStorage.getItem(SCENE_KEY) ?? 0)
}

const CANVAS_PREFIXES = ['On the canvas:', 'On the right:']

export function SceneStrip({
  scene,
  view,
  shapes,
  provisioning,
  onScene,
  onProvision,
}: {
  scene: number
  /** Current canvas view — the dbsp-circuit view swaps in each scene's operator-level explainer. */
  view: 'logical' | 'dbsp'
  /** The workspace's existing shapes — used to tell whether this scene's are already created. */
  shapes: PlaygroundShape[]
  provisioning: boolean
  onScene: (n: number) => void
  onProvision: (n: number) => void
}) {
  const [open, setOpen] = useState(true)
  const def = SCENES.find((s) => s.n === scene)
  const text = view === 'dbsp' && def?.dbsp ? def.dbsp : def?.body

  const paragraphs = (text ?? '').split('\n\n')
  const concept = paragraphs.filter((p) => !CANVAS_PREFIXES.some((x) => p.startsWith(x)))
  const canvasNotes = paragraphs
    .filter((p) => CANVAS_PREFIXES.some((x) => p.startsWith(x)))
    .map((p) => CANVAS_PREFIXES.reduce((acc, x) => acc.replace(x, '').trim(), p))

  const created = def ? shapes.filter((s) => s.scene === def.n).length >= def.shapes.length : false
  const needsShapes = (def?.shapes.length ?? 0) > 0

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
          <div className="scene-cols">
            <div className="scene-concept">
              {view === 'dbsp' && def.dbsp ? <span className="scene-view-tag">dbsp circuit view</span> : null}
              {concept.map((p, i) => (
                <p key={i} className="scene-body">
                  {p}
                </p>
              ))}
            </div>
            {canvasNotes.length > 0 ? (
              <div className="scene-note">
                <div className="scene-note-h">on the canvas</div>
                {canvasNotes.map((p, i) => (
                  <p key={i} className="scene-body">
                    {p}
                  </p>
                ))}
              </div>
            ) : null}
            <div className="scene-actions">
              {needsShapes ? (
                created ? (
                  <div className="scene-created">✓ shape{def.shapes.length > 1 ? 's' : ''} created</div>
                ) : (
                  <>
                    <div className="scene-shape-list">
                      {def.shapes.map((d) => (
                        <div key={d.key} className="scene-shape-item">
                          {d.label}
                        </div>
                      ))}
                    </div>
                    <button className="primary scene-create" disabled={provisioning} onClick={() => onProvision(def.n)}>
                      {provisioning ? 'creating…' : `Create the shape${def.shapes.length > 1 ? 's' : ''} →`}
                    </button>
                  </>
                )
              ) : null}
              <div className="scene-try">
                {def.try.map((t) => (
                  <span key={t} className="try-chip">
                    → {t}
                  </span>
                ))}
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  )
}
