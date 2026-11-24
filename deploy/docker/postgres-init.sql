-- WXEO campaign database bootstrap (runs once, when the postgres volume is created).
-- The per-run schema (wxeo_run_<run_id>) is created by the artifact SchemaTool, which also grants
-- the read/fault roles their narrow privileges; here we only create the database and roles.
--
-- Roles (mirroring the native stack):
--   wxeo_app   -- the service's application role (schema owner, no UPDATE/DELETE on posting)
--   wxeo_read  -- oracle/quiescence reader (SELECT-only)
--   wxeo_fault -- S4 fault helper: SELECT all + UPDATE on outbox/consumer_progress only
--
-- The pg_hba configuration uses trust for local/loopback connections because the stack is bound to
-- 127.0.0.1 only; the runner records this as the D8 isolation deviation (no cross-host exposure).

CREATE ROLE wxeo_app LOGIN;
CREATE ROLE wxeo_read LOGIN;
CREATE ROLE wxeo_fault LOGIN;

GRANT CONNECT ON DATABASE wxeo TO wxeo_app, wxeo_read, wxeo_fault;
GRANT USAGE ON SCHEMA public TO wxeo_app, wxeo_read, wxeo_fault;
GRANT CREATE ON SCHEMA public TO wxeo_app, wxeo_fault;
