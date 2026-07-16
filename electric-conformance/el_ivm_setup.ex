defmodule Support.ElIvmSetup do
  @moduledoc """
  Boots the `electric-circuits` stack (durable-streams + engine + Electric `/v1/shape` adapter) on an
  ephemeral Postgres and points `Electric.Client` at it — a drop-in replacement for
  `with_unique_db` + `with_complete_stack` + `with_electric_client` so Electric's integration tests run
  against electric-circuits instead of Electric's own sync-service.

  Two-phase: `el_ivm_pg/1` boots the launcher, captures the ephemeral PG URL, and exposes `db_conn` so
  Electric's schema setups (`with_parent_child_tables`, `with_sql_execute`, …) populate it. `el_ivm_client/1`
  then signals "schema ready" (creates a sentinel table), which makes the launcher introspect every table
  (`ADAPTER_PG_TABLES=*`) and start the engine + adapter, and builds `ctx.client`.
  """
  import ExUnit.Callbacks

  @lite_dir System.get_env("ELECTRIC_CIRCUITS_DIR") || Path.expand("../../../../../dbsp-ds", __DIR__)

  def el_ivm_pg(_ctx) do
    cmd = "cd #{@lite_dir} && exec pnpm --filter @electric-circuits/bench exec tsx src/electric-adapter.ts"

    port =
      Port.open({:spawn_executable, "/bin/bash"}, [
        :binary,
        {:args, ["-lc", cmd]},
        {:env,
         [
           {~c"ADAPTER_WAIT_TABLE", ~c"__el_ready"},
           {~c"ADAPTER_PG_TABLES", ~c"*"},
           {~c"ADAPTER_LONGPOLL_MS", ~c"1000"}
         ]},
        {:line, 100_000}
      ])
    # collect exit_status + lines
    Port.monitor(port)

    pg_url = read_line(port, "ADAPTER_PG ", 90_000)
    db_plain = pg_url_to_config(pg_url)
    {:ok, db_conn} = Postgrex.start_link(db_plain)

    on_exit(fn ->
      try do
        Port.close(port)
      rescue
        _ -> :ok
      catch
        _, _ -> :ok
      end
    end)

    %{el_port: port, db_conn: db_conn, pool: db_conn, db_plain: db_plain, pg_url: pg_url}
  end

  def el_ivm_client(ctx) do
    # Signal "schema ready" → launcher introspects all tables and starts the engine + adapter.
    Postgrex.query!(ctx.db_conn, "CREATE TABLE IF NOT EXISTS __el_ready (id INT PRIMARY KEY)", [])
    base_url = read_line(ctx.el_port, "ADAPTER_LISTENING ", 90_000)
    {:ok, client} = Electric.Client.new(base_url: base_url)
    %{client: client, electric_url: base_url}
  end

  defp read_line(port, prefix, timeout) do
    receive do
      {^port, {:data, {:eol, line}}} ->
        if String.starts_with?(line, prefix) do
          line |> String.replace_prefix(prefix, "") |> String.trim()
        else
          read_line(port, prefix, timeout)
        end

      {^port, {:exit_status, status}} ->
        raise "electric-circuits launcher exited (#{status}) while waiting for #{prefix}"

      {:DOWN, _ref, :port, ^port, reason} ->
        raise "electric-circuits launcher down (#{inspect(reason)}) while waiting for #{prefix}"
    after
      timeout -> raise "timed out waiting for '#{prefix}' from electric-circuits launcher"
    end
  end

  defp pg_url_to_config(url) do
    uri = URI.parse(url)
    [user | _] = String.split(uri.userinfo || "postgres", ":")

    [
      hostname: uri.host || "127.0.0.1",
      port: uri.port || 5432,
      username: user,
      password: "postgres",
      database: String.trim_leading(uri.path || "/postgres", "/")
    ]
  end
end
