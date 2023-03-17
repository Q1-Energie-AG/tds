defmodule Tds.Types.Datetime do
  alias Tds.Encoding.UCS2
  alias Tds.Parameter

  @year_1900_days :calendar.date_to_gregorian_days({1900, 1, 1})

  @tds_data_type_datetimen 0x6F

  def encode(%Parameter{name: name} = p) do
    p_name = UCS2.from_string(name)
    p_flags = p |> Parameter.option_flags()

    {type_data, type_attr} = encode_type(p)

    p_meta_data = <<byte_size(name)>> <> p_name <> p_flags <> type_data

    p_meta_data <> encode_data(p.value, type_attr)
  end

  defp encode_type(%Parameter{type: :smalldatetime}) do
    type = @tds_data_type_datetimen
    data = <<type, 0x04>>
    {data, length: 4}
  end

  defp encode_type(%Parameter{}) do
    type = @tds_data_type_datetimen
    data = <<type, 0x08>>
    {data, length: 8}
  end

  defp encode_data(value, attr) do
    # Logger.debug "dtn #{inspect attr}"
    data =
      case attr[:length] do
        4 ->
          encode_smalldatetime(value)

        _ ->
          encode_datetime(value)
      end

    if data == nil do
      <<0x00>>
    else
      <<byte_size(data)::8>> <> data
    end
  end

  defp encode_smalldatetime(nil), do: nil

  defp encode_smalldatetime({date, {hour, min, _}}),
    do: encode_smalldatetime({date, {hour, min, 0, 0}})

  defp encode_smalldatetime({date, {hour, min, _, _}}) do
    days = :calendar.date_to_gregorian_days(date) - @year_1900_days
    mins = hour * 60 + min
    encode_smalldatetime(days, mins)
  end

  defp encode_smalldatetime(days, mins) do
    <<days::little-unsigned-16, mins::little-unsigned-16>>
  end

  defp encode_datetime(nil), do: nil

  defp encode_datetime(%DateTime{} = dt),
    do: encode_datetime(DateTime.to_naive(dt))

  defp encode_datetime(%NaiveDateTime{} = dt) do
    {date, {h, m, s}} = NaiveDateTime.to_erl(dt)
    {msec, _} = dt.microsecond
    encode_datetime({date, {h, m, s, msec}})
  end

  defp encode_datetime({date, {h, m, s}}),
    do: encode_datetime({date, {h, m, s, 0}})

  defp encode_datetime({date, {h, m, s, us}}) do
    days = :calendar.date_to_gregorian_days(date) - @year_1900_days
    milliseconds = ((h * 60 + m) * 60 + s) * 1_000 + us / 1_000

    secs_300 = round(milliseconds / (10 / 3))

    {days, secs_300} =
      if secs_300 == 25_920_000 do
        {days + 1, 0}
      else
        {days, secs_300}
      end

    <<days::little-signed-32, secs_300::little-unsigned-32>>
  end
end
