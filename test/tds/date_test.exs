defmodule DateTest do
  import Tds.TestHelper
  require Logger
  use ExUnit.Case, async: true

  alias Tds.Parameter
  alias Tds.Types

  @tag timeout: 50_000

  setup do
    {:ok, pid} = Tds.start_link(opts())

    {:ok, [pid: pid]}
  end

  @date {2015, 4, 8}
  @table_name "date_test"

  test "date", context do
    query("DROP TABLE #{@table_name}", [])

    :ok =
      query(
        """
          CREATE TABLE #{@table_name} (
            created_at date NULL,
            ver int NOT NULL
            )
        """,
        []
      )

    assert <<0, 0, 40, 0>> == Types.Encoder.encode(%Parameter{value: nil, type: :date})

    encoded = Types.Encoder.encode(%Parameter{value: @date, type: :date})

    assert {%Parameter{value: @date, direction: :output}, <<>>} ==
             Types.Decoder.decode(convert_to_server(encoded))

    assert [[nil]] ==
             "SELECT CAST(NULL AS date)"
             |> query([])

    assert [[{2014, 06, 20}]] ==
             "SELECT CAST('20140620' AS date)"
             |> query([])

    assert [[nil]] ==
             "SELECT @n1"
             |> query([%Parameter{name: "@n1", value: nil, type: :date}])

    assert [[{2015, 4, 8}]] ==
             "SELECT @n1"
             |> query([
               %Parameter{name: "@n1", value: @date, type: :date}
             ])

    assert :ok =
             "INSERT INTO #{@table_name} VALUES (@1, @2)"
             |> query([
               %Parameter{name: "@1", value: nil, type: :date},
               %Parameter{name: "@2", value: 0, type: :integer}
             ])

    assert :ok =
             "INSERT INTO #{@table_name} VALUES (@1, @2)"
             |> query([
               %Parameter{name: "@1", value: nil},
               %Parameter{name: "@2", value: 0, type: :integer}
             ])

    query("DROP TABLE #{@table_name}", [])
  end
end
