-- Episode 1 initial state: a single issue-tracker table, small enough that the reader can
-- predict every shape result by looking at it. Applied automatically by Postgres initdb on
-- first boot (docker-entrypoint-initdb.d); `docker compose down -v` resets to this state.

CREATE TABLE issues (
  id        bigint PRIMARY KEY,
  title     text   NOT NULL,
  status    text   NOT NULL DEFAULT 'todo',   -- 'todo' | 'doing' | 'done'
  priority  bigint NOT NULL DEFAULT 0
);

INSERT INTO issues VALUES
  (1, 'fix the flaky test',      'todo',  3),
  (2, 'write the release notes', 'doing', 2),
  (3, 'ship the login page',     'todo',  5),
  (4, 'triage the inbox',        'done',  1),
  (5, 'update the onboarding',   'todo',  1),
  (6, 'refactor the tailer',     'done',  4);
