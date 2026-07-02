defmodule Electric.Integration.ElectricIvmOracleTest do
  @moduledoc """
  Runs Electric's oracle harness (ShapeChecker / OracleHarness — the real comparison-against-Postgres
  logic) against an EXTERNAL sync server: the `electric-ivm` engine's Electric `/v1/shape` adapter.

  We boot the electric-ivm stack (durable-streams + engine + adapter) via its launcher, point
  `Electric.Client` at it, and reuse `Support.OracleHarness.test_against_oracle/4` unchanged. This proves
  electric-ivm speaks Electric's wire protocol well enough for the official client + oracle checks.

  Target the launcher with `ELECTRIC_IVM_DIR` (default ../../../dbsp-ds relative to this repo).
  """
  use ExUnit.Case, async: false

  alias Support.OracleHarness

  @launcher_rel "packages/bench/src/electric-adapter.ts"

  setup do
    lite_dir =
      System.get_env("ELECTRIC_IVM_DIR") ||
        Path.expand("../../../../../dbsp-ds", __DIR__)

    {pg_url, base_url, port} = boot_adapter(lite_dir)
    # db_conn (mutations) needs a plain password; db_config (oracle pool) is deobfuscated by the harness.
    db_plain = pg_url_to_config(pg_url)
    db_config = Electric.Utils.obfuscate_password(db_plain)

    {:ok, db_conn} = Postgrex.start_link(db_plain)
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

    %{client: client, db_config: db_config, db_conn: db_conn}
  end

  @cols %{
    "level_1" => ["id", "active"],
    "level_2" => ["id", "level_1_id", "active"],
    "level_3" => ["id", "level_2_id", "active"],
    "level_4" => ["id", "level_3_id", "value"]
  }

  defp shape(name, table, where),
    do: %{name: name, table: table, where: where, columns: @cols[table], pk: ["id"]}

  defp m(sql), do: %{sql: sql, name: sql}

  @tag timeout: 180_000
  test "electric-ivm passes the oracle harness on the level_1..4 schema", ctx do
    shapes = [
      shape("l1_active", "level_1", "active = true"),
      shape("l3_all", "level_3", "TRUE"),
      shape("l4_like", "level_4", "value LIKE 'a%'"),
      shape("l4_in_list", "level_4", "level_3_id IN ('l3-1', 'l3-2')"),
      shape("l4_subq", "level_4", "level_3_id IN (SELECT id FROM level_3 WHERE active = true)"),
      shape(
        "l4_subq2",
        "level_4",
        "level_3_id IN (SELECT id FROM level_3 WHERE level_2_id IN (SELECT id FROM level_2 WHERE active = true))"
      )
    ]

    batches = [
      # batch 1: a couple of value/active toggles + a new row
      [
        [
          m("UPDATE level_3 SET active = true WHERE id = 'l3-2'"),
          m("UPDATE level_4 SET value = 'alpha-2' WHERE id = 'l4-1'"),
          m("INSERT INTO level_4 VALUES ('l4-9', 'l3-1', 'apex')")
        ]
      ],
      # batch 2: deactivate a level_3 (move-outs), delete a row, deeper move via level_2
      [
        [
          m("UPDATE level_3 SET active = false WHERE id = 'l3-1'"),
          m("DELETE FROM level_4 WHERE id = 'l4-2'"),
          m("UPDATE level_2 SET active = false WHERE id = 'l2-1'")
        ]
      ],
      # batch 3: re-activate everything + insert under l3-2
      [
        [
          m("UPDATE level_3 SET active = true WHERE id = 'l3-1'"),
          m("UPDATE level_2 SET active = true WHERE id = 'l2-1'"),
          m("INSERT INTO level_4 VALUES ('l4-10', 'l3-2', 'azimuth')")
        ]
      ]
    ]

    assert :ok =
             OracleHarness.test_against_oracle(ctx, shapes, batches,
               timeout_ms: 15_000,
               restart_server_every: 0,
               restart_client_every: 0
             )
  end

  # --- boot the electric-ivm adapter via its launcher; read discovery lines off stdout ----------
  defp boot_adapter(lite_dir) do
    launcher = Path.join(lite_dir, @launcher_rel)
    unless File.exists?(launcher), do: flunk("launcher not found at #{launcher}")

    cmd = "cd #{lite_dir} && exec pnpm --filter @electric-ivm/bench exec tsx src/electric-adapter.ts"
    port = Port.open({:spawn, "bash -lc '#{cmd}'"}, [:binary, :exit_status, line: 100_000])

    {pg_url, base_url} = read_discovery(port, %{}, 60_000)
    {pg_url, base_url, port}
  end

  defp read_discovery(_port, %{pg: pg, url: url}, _deadline) when is_binary(pg) and is_binary(url),
    do: {pg, url}

  defp read_discovery(port, acc, timeout) do
    receive do
      {^port, {:data, {:eol, line}}} ->
        acc =
          cond do
            String.starts_with?(line, "ADAPTER_PG ") ->
              Map.put(acc, :pg, line |> String.replace_prefix("ADAPTER_PG ", "") |> String.trim())

            String.starts_with?(line, "ADAPTER_LISTENING ") ->
              Map.put(acc, :url, line |> String.replace_prefix("ADAPTER_LISTENING ", "") |> String.trim())

            true ->
              acc
          end

        read_discovery(port, acc, timeout)

      {^port, {:exit_status, status}} ->
        flunk("electric-ivm launcher exited early (status #{status})")
    after
      timeout -> flunk("timed out waiting for electric-ivm launcher discovery lines")
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
