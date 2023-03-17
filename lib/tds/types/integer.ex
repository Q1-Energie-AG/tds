defmodule Tds.Types.Integer do
  alias Tds.Encoding.UCS2
  alias Tds.Parameter

  @tds_data_type_intn 0x26

  def encode(%Parameter{name: name} = p) do
    p_name = UCS2.from_string(name)
    p_flags = p |> Parameter.option_flags()

    {type_data, type_attr} = encode_type(p)

    p_meta_data = <<byte_size(name)>> <> p_name <> p_flags <> type_data

    p_meta_data <> encode_data(p.value, type_attr)
  end

  def encode_type(%Parameter{value: value}) do
    type = @tds_data_type_intn
    length = size(value)
    {<<type, length>>, [length: length]}
  end

  def encode_data(nil, _attr), do: <<0>>

  def encode_data(value, _attr) do
    size = size(value)
    <<size>> <> <<value::little-signed-size(size)-unit(8)>>
  end

  defp size(int) when int == nil, do: 4
  defp size(int) when int in -254..255, do: 4
  defp size(int) when int in -32_768..32_767, do: 4
  defp size(int) when int in -2_147_483_648..2_147_483_647, do: 4

  defp size(int)
       when int in -9_223_372_036_854_775_808..9_223_372_036_854_775_807,
       do: 8

  defp size(int),
    do:
      raise(
        ArgumentError,
        "Erlang integer value #{int} is too big (more than 64bits) to fit tds" <>
          " integer/bigint. Please consider using Decimal.new/1 to maintain precision."
      )
end
