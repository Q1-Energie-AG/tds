defmodule Tds.Types.Binary do
  alias Tds.Encoding.UCS2
  alias Tds.Parameter
  alias Tds.Types.Encoder.PLP

  @tds_data_type_bigvarbinary 0xA5
  @tds_plp_null 0xFFFFFFFFFFFFFFFF

  def encode(%Parameter{name: name} = p) do
    p_name = UCS2.from_string(name)
    p_flags = p |> Parameter.option_flags()

    [byte_size(name), p_name, p_flags, do_encode(p.value)]
    |> IO.iodata_to_binary()
  end

  defp do_encode(nil),
    do: [@tds_data_type_bigvarbinary, <<0xFF, 0xFF>>, <<@tds_plp_null::little-unsigned-64>>]

  defp do_encode(int) when is_integer(int), do: do_encode(<<int>>)

  defp do_encode(value) do
    encoded =
      case byte_size(value) do
        size when size > 8000 -> [<<0xFF, 0xFF>>, PLP.encode(value)]
        size -> [<<size::little-unsigned-16>>, <<size::little-unsigned-16>>, value]
      end

    [@tds_data_type_bigvarbinary, encoded]
  end
end
