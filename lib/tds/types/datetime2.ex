defmodule Tds.Types.Datetime2 do
  alias Tds.Encoding.UCS2
  alias Tds.Parameter
  alias Tds.Types

  @tds_data_type_datetime2n 0x2A
  @max_time_scale 7

  def encode(%Parameter{name: name} = p) do
    p_name = UCS2.from_string(name)
    p_flags = p |> Parameter.option_flags()

    {type_data, type_attr} = encode_type(p)

    p_meta_data = <<byte_size(name)>> <> p_name <> p_flags <> type_data

    p_meta_data <> encode_data(p.value, type_attr)
  end

  defp encode_type(%Parameter{value: %NaiveDateTime{microsecond: {_, s}}}) do
    type = @tds_data_type_datetime2n
    data = <<type, s>>
    {data, scale: s}
  end

  defp encode_type(%Parameter{}) do
    type = @tds_data_type_datetime2n
    data = <<type, 0x07>>
    {data, scale: 7}
  end

  defp encode_data(value, _attr) do
    # Logger.debug "EncodeData #{inspect value}"
    {data, scale} = encode_datetime2(value)

    if data == nil do
      <<0x00>>
    else
      # 0x08 length of binary for scale 7
      storage_size =
        cond do
          scale < 3 -> 0x06
          scale < 5 -> 0x07
          scale < 8 -> 0x08
        end

      <<storage_size>> <> data
    end
  end

  def encode_datetime2(value, scale \\ @max_time_scale)
  def encode_datetime2(nil, _), do: {nil, 0}

  def encode_datetime2({date, time}, scale) do
    {time, scale} = Types.Time.encode_time(time, scale)
    date = Types.Date.encode_date(date)
    {time <> date, scale}
  end

  def encode_datetime2(%NaiveDateTime{} = value, _scale) do
    t = NaiveDateTime.to_time(value)
    {time, scale} = Types.Time.encode_time(t)
    date = Types.Date.encode_date(NaiveDateTime.to_date(value))
    {time <> date, scale}
  end

  def encode_datetime2(value, scale) do
    raise ArgumentError,
          "value #{inspect(value)} with scale #{inspect(scale)} is not supported DateTime2 value"
  end
end
