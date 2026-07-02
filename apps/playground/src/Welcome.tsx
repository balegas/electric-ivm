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
          Electric-style sync streams subsets of your Postgres data — <b>shapes</b> — into apps,
          and keeps them up to date as the data changes. This playground drives the Shape API
          directly against a tiny issue tracker. Create a shape. Change some data. Watch the
          change reach every shape that subscribes — and nothing else.
        </p>
        <div className="welcome-grid">
          <div>
            <span className="welcome-ico">📝</span>
            <b>Left — the data.</b> Two plain tables (issues, projects). Every cell you edit is
            one real write to Postgres.
          </div>
          <div>
            <span className="welcome-ico">🔀</span>
            <b>Middle — inside the engine.</b> Your change travels through the engine's machinery,
            live. A red ✕ shows where a change stops because nobody needs it.
          </div>
          <div>
            <span className="welcome-ico">📱</span>
            <b>Right — live results.</b> One card per shape: its query, its API request, and its
            result set — maintained incrementally, never re-queried.
          </div>
          <div>
            <span className="welcome-ico">🎬</span>
            <b>Bottom — start here.</b> Six short scenes explain what you're seeing, one idea at a
            time. Everything stays clickable throughout.
          </div>
        </div>
        <div className="modal-actions welcome-actions">
          <span className="welcome-note">
            Your data is private to you on a shared server. Curious how? Flip "under the hood" in
            the top bar.
          </span>
          <button className="primary" onClick={onClose}>
            Let's go →
          </button>
        </div>
      </div>
    </div>
  )
}
