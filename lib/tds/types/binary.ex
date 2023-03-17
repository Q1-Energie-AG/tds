defmodule Tds.Types.Binary do
  alias Tds.Encoding.UCS2
  alias Tds.Parameter
  alias Tds.Types.Encoder.PLP

  @tds_data_type_bigvarbinary 0xA5
  @tds_plp_null 0xFFFFFFFFFFFFFFFF

  def encode(%Parameter{name: name} = p) do
    p_name = UCS2.from_string(name)
    p_flags = p |> Parameter.option_flags()

    {type_data, type_attr} = encode_type(p)

    p_meta_data = <<byte_size(name)>> <> p_name <> p_flags <> type_data

    p_meta_data <> encode_data(p.value, type_attr)
  end

  def encode_type(%Parameter{value: value}) do
    value =
      if is_integer(value) do
        <<value>>
      else
        value
      end

    length = length_for_binary(value)
    type = @tds_data_type_bigvarbinary
    data = <<type>> <> length
    {data, []}
  end

  def encode_data(value, attr) when is_integer(value),
    do: encode_data(<<value>>, attr)

  def encode_data(nil, _),
    do: <<@tds_plp_null::little-unsigned-64>>

  def encode_data(value, _) do
    case byte_size(value) do
      # varbinary(max) gets encoded in chunks
      value_size when value_size > 8000 -> PLP.encode(value)
      value_size -> <<value_size::little-unsigned-16>> <> value
    end
  end

  defp length_for_binary(nil), do: <<0xFF, 0xFF>>

  defp length_for_binary(value) do
    case byte_size(value) do
      # varbinary(max)
      value_size when value_size > 8000 -> <<0xFF, 0xFF>>
      value_size -> <<value_size::little-unsigned-16>>
    end
  end
end
