defmodule Tds.Types.Date do
  alias Tds.Encoding.UCS2
  alias Tds.Parameter

  @tds_data_type_daten 0x28

  def encode(%Parameter{name: name} = p) do
    p_name = UCS2.from_string(name)
    p_flags = p |> Parameter.option_flags()

    {type_data, type_attr} = encode_type(p)

    p_meta_data = <<byte_size(name)>> <> p_name <> p_flags <> type_data

    p_meta_data <> encode_data(p.value, type_attr)
  end

  defp encode_type(%Parameter{}) do
    type = @tds_data_type_daten
    data = <<type>>
    {data, []}
  end

  defp encode_data(value, _attr) do
    data = encode_date(value)

    if data == nil do
      <<0x00>>
    else
      <<0x03, data::binary>>
    end
  end

  def encode_date(nil), do: nil

  def encode_date(%Date{} = date), do: date |> Date.to_erl() |> encode_date()

  def encode_date(date) do
    days = :calendar.date_to_gregorian_days(date) - 366
    <<days::little-24>>
  end
end
