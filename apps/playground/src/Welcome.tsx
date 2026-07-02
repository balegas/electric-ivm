// First-visit welcome: what this app is, and how to read the screen. Shown once (localStorage
// flag) and reopenable from the top bar's "what is this?" button.

const SEEN_KEY = 'playground-welcomed'

export const hasSeenWelcome = (): boolean => localStorage.getItem(SEEN_KEY) === '1'
export const markWelcomeSeen = (): void => localStorage.setItem(SEEN_KEY, '1')

export function Welcome({ onClose }: { onClose: () => void }) {
  return (
    <div className="modal-back">
      <div className="modal welcome">
        <div className="welcome-title">
          Watch your writes become <em>deltas</em>
        </div>
        <p className="welcome-lead">
          <b>electric-ivm</b> keeps SQL queries — <b>shapes</b> — live over Postgres by running them
          as incremental <b>DBSP pipelines</b>: every write becomes a weighted delta (
          <span className="pos">+1</span> insert, <span className="neg">−1</span> delete) that flows
          through routers, filters, joins and folds, touching only the shapes it affects. This
          playground lets you <b>watch that happen</b> — on a real Postgres and a real engine, in
          your own sandbox.
        </p>
        <div className="welcome-grid">
          <div>
            <span className="welcome-ico">🍕</span>
            <b>Left — the world.</b> A food-delivery app. Every button is one SQL write to
            Postgres: place an order, start cooking, deliver…
          </div>
          <div>
            <span className="welcome-ico">🔀</span>
            <b>Middle — the pipeline.</b> The engine's actual maintained dataflow. Your write
            animates through it live; a red ✕ shows where a delta gets dropped. Toggle{' '}
            <i>Logical</i> / <i>dbsp circuit</i> up top.
          </div>
          <div>
            <span className="welcome-ico">📱</span>
            <b>Right — the subscribers.</b> Each card is a live shape (a kitchen screen, a rider's
            phone, a dashboard). Its contents are maintained incrementally — never re-queried.
          </div>
          <div>
            <span className="welcome-ico">🎬</span>
            <b>Bottom — six scenes.</b> A short walkthrough of the ideas, from “a shape is a
            filter” to subqueries and live aggregations. Every scene is free play.
          </div>
        </div>
        <div className="modal-actions welcome-actions">
          <span className="welcome-note">
            You're in a private workspace on a shared server — every predicate carries your
            workspace id, honestly displayed.
          </span>
          <button className="primary" onClick={onClose}>
            Let's go →
          </button>
        </div>
      </div>
    </div>
  )
}
