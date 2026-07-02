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
          How <em>live queries</em> work
        </div>
        <p className="welcome-lead">
          A <b>shape</b> is a database query whose results stay up to date on every screen that
          subscribes to it — that's how Electric-style sync works. This playground is a live view
          inside the engine that makes it happen: change some data in a little food-delivery world
          and watch how the change reaches exactly the screens that care — and nothing else.
        </p>
        <div className="welcome-grid">
          <div>
            <span className="welcome-ico">🍕</span>
            <b>Left — food delivery.</b> Every button changes real data in Postgres: place an
            order, start cooking, deliver…
          </div>
          <div>
            <span className="welcome-ico">🔀</span>
            <b>Middle — inside the engine.</b> Your change travels through the engine's machinery,
            live. A red ✕ shows where a change stops because nobody needs it.
          </div>
          <div>
            <span className="welcome-ico">📱</span>
            <b>Right — the screens.</b> Each card is a subscriber — a kitchen display, a rider's
            phone, a dashboard. They update the instant a change concerns them.
          </div>
          <div>
            <span className="welcome-ico">🎬</span>
            <b>Bottom — start here.</b> Six short scenes explain what you're seeing, one idea at a
            time. Everything stays clickable throughout.
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
