defmodule Tds.Types.Datetimeoffset do
  alias Tds.Encoding.UCS2
  alias Tds.Parameter

  @tds_data_type_datetimeoffsetn 0x2B
  @max_time_scale 7

  def encode(%Parameter{name: name} = p) do
    p_name = UCS2.from_string(name)
    p_flags = p |> Parameter.option_flags()

    {type_data, type_attr} = encode_type(p)

    p_meta_data = <<byte_size(name)>> <> p_name <> p_flags <> type_data

    p_meta_data <> encode_data(p.value, type_attr)
  end

  defp encode_type(%Parameter{
         value: %DateTime{microsecond: {_, s}}
       }) do
    type = @tds_data_type_datetimeoffsetn
    data = <<type, s>>
    {data, scale: s}
  end

  defp encode_type(%Parameter{}) do
    type = @tds_data_type_datetimeoffsetn
    data = <<type, 0x07>>
    {data, scale: 7}
  end

  defp encode_data(value, _attr) do
    # Logger.debug "encode_data_datetimeoffsetn #{inspect value}"
    data = encode_datetimeoffset(value)

    if data == nil do
      <<0x00>>
    else
      case value do
        %DateTime{microsecond: {_, s}} when s < 3 ->
          <<0x08, data::binary>>

        %DateTime{microsecond: {_, s}} when s < 5 ->
          <<0x09, data::binary>>

        _ ->
          <<0x0A, data::binary>>
      end
    end
  end

  defp encode_datetimeoffset(datetimetz, scale \\ @max_time_scale)
  defp encode_datetimeoffset(nil, _), do: nil

  defp encode_datetimeoffset({date, time, offset_min}, scale) do
    {datetime, _ignore_always_10bytes} = Tds.Types.Datetime2.encode_datetime2({date, time}, scale)

    datetime <> <<offset_min::little-signed-16>>
  end

  defp encode_datetimeoffset(
         %DateTime{utc_offset: offset} = dt,
         scale
       ) do
    {datetime, _} =
      dt
      |> DateTime.add(-offset)
      |> DateTime.to_naive()
      |> Tds.Types.Datetime2.encode_datetime2(scale)

    offset_min = trunc(offset / 60)

    datetime <> <<offset_min::little-signed-16>>
  end
end
