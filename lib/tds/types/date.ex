defmodule Tds.Types.Date do
  alias Tds.Encoding.UCS2
  alias Tds.Parameter

  @tds_data_type_daten 0x28

  def encode(%Parameter{name: name} = p) do
    [
      byte_size(name),
      UCS2.from_string(name),
      Parameter.option_flags(p),
      @tds_data_type_daten,
      encode_data(p.value)
    ]
    |> IO.iodata_to_binary()
  end

  defp encode_data(nil), do: 0x00

  defp encode_data(value) do
    data = encode_date(value)
    <<0x03, data::binary>>
  end

  def encode_date(%Date{} = date), do: date |> Date.to_erl() |> encode_date()

  def encode_date(date) do
    days = :calendar.date_to_gregorian_days(date) - 366
    <<days::little-24>>
  end
end
