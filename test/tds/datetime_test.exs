defmodule DatetimeTest do
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
  @time {15, 16, 23}
  @time_us {15, 16, 23, 123_456}
  @time_fsec {15, 16, 23, 1_234_567}
  @datetime {@date, @time}
  @datetime_us {@date, @time_us}
  @datetime_fsec {@date, @time_fsec}
  @offset -240
  @datetimeoffset {@date, @time, @offset}
  @datetimeoffset_fsec {@date, @time_fsec, @offset}

  test "datetime", context do
    query("DROP TABLE date_test", [])

    :ok =
      query(
        """
          CREATE TABLE date_test (
            created_at datetime NULL,
            ver int NOT NULL
            )
        """,
        []
      )

    assert <<0, 0, 111, 8, 0>> == Types.Encoder.encode(%Parameter{value: nil, type: :datetime})

    encoded = Types.Encoder.encode(%Parameter{value: @datetime, type: :datetime})

    assert {%Parameter{value: {@date, {15, 16, 23, 0}}, direction: :output}, <<>>} ==
             Types.Decoder.decode(convert_to_server(encoded))

    encoded = Types.Encoder.encode(%Parameter{value: @datetime_us, type: :datetime})

    assert {%Parameter{value: {@date, {15, 16, 23, 123}}, direction: :output}, <<>>} ==
             Types.Decoder.decode(convert_to_server(encoded))

    assert [[nil]] ==
             "SELECT CAST(NULL AS datetime)"
             |> query([])

    assert [[{{2014, 06, 20}, {10, 21, 42, 0}}]] ==
             "SELECT CAST('20140620 10:21:42 AM' AS datetime)"
             |> query([])

    assert [[nil]] ==
             "SELECT @n1"
             |> query([%Parameter{name: "@n1", value: nil, type: :datetime}])

    assert [[{{2015, 4, 8}, {15, 16, 23, 0}}]] ==
             "SELECT @n1"
             |> query([
               %Parameter{name: "@n1", value: @datetime, type: :datetime}
             ])

    assert [[{{2015, 4, 8}, {15, 16, 23, 123}}]] ==
             "SELECT @n1"
             |> query([
               %Parameter{name: "@n1", value: @datetime_us, type: :datetime}
             ])

    assert :ok =
             "INSERT INTO date_test VALUES (@1, @2)"
             |> query([
               %Parameter{name: "@1", value: nil, type: :datetime},
               %Parameter{name: "@2", value: 0, type: :integer}
             ])

    query("DROP TABLE date_test", [])
  end

  test "smalldatetime", context do
    assert <<0, 0, 111, 4, 0>> ==
             Types.Encoder.encode(%Parameter{value: nil, type: :smalldatetime})

    encoded = Types.Encoder.encode(%Parameter{value: @datetime, type: :smalldatetime})

    assert {%Parameter{value: {@date, {15, 16, 0, 0}}, direction: :output}, <<>>} ==
             Types.Decoder.decode(convert_to_server(encoded))

    assert [[nil]] ==
             "SELECT CAST(NULL AS smalldatetime)"
             |> query([])

    assert [[{{2014, 06, 20}, {10, 40, 0, 0}}]] ==
             "SELECT CAST('20140620 10:40 AM' AS smalldatetime)"
             |> query([])

    assert [[nil]] ==
             "SELECT @n1"
             |> query([
               %Parameter{name: "@n1", value: nil, type: :smalldatetime}
             ])

    assert [[{{2015, 4, 8}, {15, 16, 0, 0}}]] ==
             "SELECT @n1"
             |> query([
               %Parameter{name: "@n1", value: @datetime, type: :smalldatetime}
             ])

    assert [[{{2015, 4, 8}, {15, 16, 0, 0}}]] ==
             "SELECT @n1"
             |> query([
               %Parameter{
                 name: "@n1",
                 value: @datetime_fsec,
                 type: :smalldatetime
               }
             ])
  end

  test "date", context do
    assert <<0, 0, 40, 0>> == Types.Encoder.encode(%Parameter{value: nil, type: :date})
    enc = Types.Encoder.encode(%Parameter{value: @date, type: :date})

    assert {%Parameter{value: @date, direction: :output}, <<>>} ==
             Types.Decoder.decode(convert_to_server(enc))

    assert [[nil]] == query("SELECT CAST(NULL AS date)", [])
    assert [[{2014, 06, 20}]] == query("SELECT CAST('20140620' AS date)", [])

    assert [[nil]] ==
             query("SELECT @n1", [
               %Parameter{name: "@n1", value: nil, type: :date}
             ])

    assert [[{2015, 4, 8}]] ==
             query("SELECT @n1", [
               %Parameter{name: "@n1", value: @date, type: :date}
             ])
  end

  test "time", context do
    assert <<0, 0, 41, 7, 0>> == Types.Encoder.encode(%Parameter{value: nil, type: :time})

    value = Types.Encoder.encode(%Parameter{value: @time, type: :time})

    assert {%Parameter{value: {15, 16, 23, 0}, direction: :output}, <<>>} ==
             Types.Decoder.decode(convert_to_server(value))

    value = Types.Encoder.encode(%Parameter{value: @time_fsec, type: :time})

    assert {%Parameter{value: {15, 16, 23, 1_234_567}, direction: :output}, <<>>} ==
             Types.Decoder.decode(convert_to_server(value))

    value = Types.Encoder.encode(%Parameter{value: @time_us, type: :time})

    assert {%Parameter{value: {15, 16, 23, 123_456}, direction: :output}, <<>>} ==
             Types.Decoder.decode(convert_to_server(value))

    assert [[nil]] == query("SELECT CAST(NULL AS time)", [])
    assert [[nil]] == query("SELECT CAST(NULL AS time(0))", [])
    assert [[nil]] == query("SELECT CAST(NULL AS time(6))", [])

    assert [[{10, 24, 30, 1_234_567}]] ==
             query("SELECT CAST('10:24:30.1234567' AS time)", [])

    assert [[{10, 24, 30, 0}]] ==
             query("SELECT CAST('10:24:30.1234567' AS time(0))", [])

    assert [[{10, 24, 30, 1_234_567}]] ==
             query("SELECT CAST('10:24:30.1234567' AS time(7))", [])

    assert [[{10, 24, 30, 123_457}]] ==
             query("SELECT CAST('10:24:30.1234567' AS time(6))", [])

    assert [[{10, 24, 30, 1}]] ==
             query("SELECT CAST('10:24:30.1234567' AS time(1))", [])

    assert [[nil]] ==
             query("SELECT @n1", [
               %Parameter{name: "@n1", value: nil, type: :time}
             ])

    assert [[{15, 16, 23, 0}]] ==
             query("SELECT @n1", [
               %Parameter{name: "@n1", value: @time, type: :time}
             ])

    assert [[{15, 16, 23, 123}]] ==
             query("SELECT @n1", [
               %Parameter{name: "@n1", value: {15, 16, 23, 123}, type: :time}
             ])

    assert [[{15, 16, 23, 1_234_567}]] ==
             query("SELECT @n1", [
               %Parameter{name: "@n1", value: @time_fsec, type: :time}
             ])
  end

  test "datetime2", context do
    assert <<0, 0, 42, 7, 0>> == Types.Encoder.encode(%Parameter{value: nil, type: :datetime2})

    dt = Types.Encoder.encode(%Parameter{value: @datetime, type: :datetime2})

    assert {%Parameter{value: {@date, {15, 16, 23, 0}}, direction: :output}, <<>>} ==
             Types.Decoder.decode(convert_to_server(dt))

    dt = Types.Encoder.encode(%Parameter{value: @datetime_fsec, type: :datetime2})

    assert {%Parameter{value: @datetime_fsec, direction: :output}, <<>>} ==
             Types.Decoder.decode(convert_to_server(dt))

    dt = Types.Encoder.encode(%Parameter{value: {@date, {131, 56, 23, 0}}, type: :datetime2})

    assert {%Parameter{value: {@date, {131, 56, 23, 0}}, direction: :output}, <<>>} ==
             Types.Decoder.decode(convert_to_server(dt))

    assert [[nil]] == query("SELECT CAST(NULL AS datetime2)", [])
    assert [[nil]] == query("SELECT CAST(NULL AS datetime2(0))", [])
    assert [[nil]] == query("SELECT CAST(NULL AS datetime2(6))", [])

    assert [[{{2015, 4, 8}, {15, 16, 23, 0}}]] ==
             query("SELECT CAST('20150408 15:16:23' AS datetime2)", [])

    assert [[{{2015, 4, 8}, {15, 16, 23, 4_200_000}}]] ==
             query("SELECT CAST('20150408 15:16:23.42' AS datetime2)", [])

    assert [[{{2015, 4, 8}, {15, 16, 23, 4_200_000}}]] ==
             query("SELECT CAST('20150408 15:16:23.42' AS datetime2(7))", [])

    assert [[{{2015, 4, 8}, {15, 16, 23, 420_000}}]] ==
             query("SELECT CAST('20150408 15:16:23.42' AS datetime2(6))", [])

    assert [[{{2015, 4, 8}, {15, 16, 23, 0}}]] ==
             query("SELECT CAST('20150408 15:16:23.42' AS datetime2(0))", [])

    assert [[{{2015, 4, 8}, {15, 16, 23, 4}}]] ==
             query("SELECT CAST('20150408 15:16:23.42' AS datetime2(1))", [])

    assert [[nil]] ==
             query("SELECT @n1", [
               %Parameter{name: "@n1", value: nil, type: :datetime2}
             ])

    assert [[{{2015, 4, 8}, {15, 16, 23, 0}}]] ==
             query("SELECT @n1", [
               %Parameter{name: "@n1", value: @datetime, type: :datetime2}
             ])

    assert [[{{2015, 4, 8}, {15, 16, 23, 1_234_567}}]] ==
             query("SELECT @n1", [
               %Parameter{name: "@n1", value: @datetime_fsec, type: :datetime2}
             ])
  end

  test "implicit params", context do
    assert [[{{2015, 4, 8}, {15, 16, 23, 0}}]] ==
             query("SELECT @n1", [%Parameter{name: "@n1", value: @datetime}])

    # #datetime_us {_,_,_,}, {_,_,_,_}
    assert [[{{2015, 4, 8}, {15, 16, 23, 123_456}}]] ==
             query("SELECT @n1", [
               %Parameter{name: "@n1", value: @datetime_us}
             ])

    # datetime_fsec {_,_,_,}, {_,_,_}, _
    assert [[{{2015, 4, 8}, {15, 16, 23, 0}, -240}]] ==
             query("SELECT @n1", [
               %Parameter{name: "@n1", value: @datetimeoffset}
             ])

    # datetime_fsec {_,_,_,}, {_,_,_,_}, _
    assert [[{{2015, 4, 8}, {15, 16, 23, 1_234_567}, -240}]] ==
             query("SELECT @n1", [
               %Parameter{name: "@n1", value: @datetimeoffset_fsec}
             ])
  end
end
