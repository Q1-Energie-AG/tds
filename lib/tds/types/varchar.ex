defmodule Tds.Types.Varchar do
  alias Tds.Encoding.UCS2
  alias Tds.Parameter
  alias Tds.Types.Encoder.PLP

  @tds_data_type_nvarchar 0xE7
  @tds_plp_null 0xFFFFFFFFFFFFFFFF

  def encode(%Parameter{name: name} = p) do
    p_name = UCS2.from_string(name)
    p_flags = p |> Parameter.option_flags()

    {type_data, type_attr} = encode_type(p)

    p_meta_data = <<byte_size(name)>> <> p_name <> p_flags <> type_data

    p_meta_data <> encode_data(p.value, type_attr)
  end

  def encode_type(%Parameter{value: value}) do
    collation = <<0x00, 0x00, 0x00, 0x00, 0x00>>

    length =
      if value != nil do
        value = value |> UCS2.from_string()
        value_size = byte_size(value)

        if value_size == 0 or value_size > 8000 do
          <<0xFF, 0xFF>>
        else
          <<value_size::little-(2 * 8)>>
        end
      else
        <<0xFF, 0xFF>>
      end

    type = @tds_data_type_nvarchar
    data = <<type>> <> length <> collation
    {data, [collation: collation]}
  end

  def encode_data(nil, _),
    do: <<@tds_plp_null::little-unsigned-64>>

  def encode_data(value, _) do
    value = UCS2.from_string(value)
    value_size = byte_size(value)

    cond do
      value_size <= 0 ->
        <<0x00::unsigned-64, 0x00::unsigned-32>>

      value_size > 8000 ->
        PLP.encode(value)

      true ->
        <<value_size::little-size(2)-unit(8)>> <> value
    end
  end
end
