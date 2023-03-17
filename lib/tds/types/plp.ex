defmodule Tds.Types.Encoder.PLP do
  def encode(data) do
    size = byte_size(data)

    <<size::little-unsigned-64>> <>
      encode_plp_chunk(size, data, <<>>) <> <<0x00::little-unsigned-32>>
  end

  defp encode_plp_chunk(0, _, buf), do: buf

  defp encode_plp_chunk(size, data, buf) do
    <<_t::unsigned-32, chunk_size::unsigned-32>> = <<size::unsigned-64>>
    <<chunk::binary-size(chunk_size), data::binary>> = data
    plp = <<chunk_size::little-unsigned-32>> <> chunk
    encode_plp_chunk(size - chunk_size, data, buf <> plp)
  end
end
