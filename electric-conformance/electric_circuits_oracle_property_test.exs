defmodule Electric.Integration.ElectricIvmOraclePropertyTest do
  @moduledoc """
  The Electric oracle PROPERTY test, run against electric-circuits's `/v1/shape` adapter. Reuses Electric's
  own `WhereClauseGenerator.shapes_gen/1` + `StandardSchema` (DDL/seed/mutation generators) and
  `OracleHarness.test_against_oracle/4` — only the sync server under test is swapped for electric-circuits.

  Two-phase boot: the launcher starts an ephemeral Postgres and announces its URL; we apply
  StandardSchema's exact DDL+seed; the launcher then introspects + starts the engine + adapter and
  announces its base URL; we point `Electric.Client` at it and run the generated shapes/mutations.

  Tunables (small defaults to keep CI-ish): ORACLE_SHAPE_COUNT, ORACLE_BATCH_COUNT, ORACLE_TXNS_PER_BATCH,
  ORACLE_MUTATIONS_PER_TXN, ORACLE_RUNS.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias Support.OracleHarness
  alias Support.OracleHarness.StandardSchema
  alias Support.OracleHarness.WhereClauseGenerator

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      v -> String.to_integer(v)
    end
  end

  setup do
    lite_dir =
      System.get_env("ELECTRIC_CIRCUITS_DIR") || Path.expand("../../../../../dbsp-ds", __DIR__)

    cmd =
      "cd #{lite_dir} && exec env ADAPTER_WAIT_TABLE=level_3_tags pnpm --filter @electric-circuits/bench exec tsx src/electric-adapter.ts"

    port = Port.open({:spawn, "bash -lc '#{cmd}'"}, [:binary, :exit_status, line: 100_000])

    # Phase 1: wait for the ephemeral PG URL.
    pg_url = read_line(port, "ADAPTER_PG ", 60_000)
    db_plain = pg_url_to_config(pg_url)
    {:ok, db_conn} = Postgrex.start_link(db_plain)

    # Apply Electric's exact standard schema + seed, then let the launcher introspect.
    Enum.each(StandardSchema.schema_sql() ++ StandardSchema.seed_sql(), fn sql ->
      Postgrex.query!(db_conn, sql, [])
    end)

    # Phase 2: wait for the adapter base URL (engine introspected the freshly-created schema).
    base_url = read_line(port, "ADAPTER_LISTENING ", 60_000)
    {:ok, client} = Electric.Client.new(base_url: base_url)

    on_exit(fn ->
      try do
        Port.close(port)
      rescue
        _ -> :ok
      catch
        _, _ -> :ok
      end
    end)

    %{client: client, db_conn: db_conn, db_config: Electric.Utils.obfuscate_password(db_plain)}
  end

  @tag timeout: 600_000
  property "electric-circuits converges with the Postgres oracle across generated shapes + mutations", ctx do
    shape_count = env_int("ORACLE_SHAPE_COUNT", 4)
    batch_count = env_int("ORACLE_BATCH_COUNT", 3)
    txns_per_batch = env_int("ORACLE_TXNS_PER_BATCH", 1)
    mutations_per_txn = env_int("ORACLE_MUTATIONS_PER_TXN", 3)
    runs = env_int("ORACLE_RUNS", 5)
    total_mutations = batch_count * txns_per_batch * mutations_per_txn

    check all shapes <- WhereClauseGenerator.shapes_gen(shape_count),
              mutations <- StandardSchema.mutations_gen(total_mutations),
              max_runs: runs do
      transactions = Enum.chunk_every(mutations, mutations_per_txn)
      batches = Enum.chunk_every(transactions, txns_per_batch)

      assert :ok =
               OracleHarness.test_against_oracle(ctx, shapes, batches,
                 timeout_ms: 15_000,
                 restart_server_every: 0,
                 restart_client_every: 0
               )
    end
  end

  # --- helpers ----------------------------------------------------------------------------------
  defp read_line(port, prefix, timeout) do
    receive do
      {^port, {:data, {:eol, line}}} ->
        if String.starts_with?(line, prefix) do
          line |> String.replace_prefix(prefix, "") |> String.trim()
        else
          read_line(port, prefix, timeout)
        end

      {^port, {:exit_status, status}} ->
        flunk("electric-circuits launcher exited early (status #{status}) while waiting for #{prefix}")
    after
      timeout -> flunk("timed out waiting for '#{prefix}' from electric-circuits launcher")
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
