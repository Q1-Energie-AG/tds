defmodule Tds.Types.Decimal do
  alias Tds.Encoding.UCS2
  alias Tds.Parameter

  @tds_data_type_decimaln 0x6A
  @precision 38

  def encode(%Parameter{name: name} = p) do
    p_name = UCS2.from_string(name)
    p_flags = p |> Parameter.option_flags()

    {type_data, type_attr} = encode_type(p)

    [<<byte_size(name)>>, p_name, p_flags, type_data, encode_data(p.value, type_attr)]
    |> IO.iodata_to_binary()
  end

  def encode_type(%Parameter{value: nil}) do
    type = @tds_data_type_decimaln
    {<<type, 0x00, 0x00, 0x00, 0x00>>, []}
  end

  def encode_type(%Parameter{value: value}) do
    Decimal.Context.update(&Map.put(&1, :precision, @precision))

    {precision, scale} =
      value
      |> Decimal.abs()
      |> Decimal.to_string(:normal)
      |> String.split(".")
      |> case do
        [p, s] ->
          len = String.length(s)
          {String.length(p) + len, len}

        [p] ->
          {String.length(p), 0}
      end

    size = size_for_precision(precision)

    {[@tds_data_type_decimaln, size + 1, precision, scale], [precision: precision, scale: scale]}
  end

  # decimal
  def encode_data(%Decimal{} = value, attr) do
    Decimal.Context.update(&Map.put(&1, :precision, 38))
    precision = attr[:precision]

    sign =
      case value.sign do
        1 -> 1
        -1 -> 0
      end

    value_binary = :binary.encode_unsigned(value.coef, :little)

    value_size = byte_size(value_binary)

    size = size_for_precision(precision)

    [size + 1, sign, value_binary, <<0::size(size - value_size)-unit(8)>>]
  end

  def encode_data(nil, _), do: <<>>

  def encode_data(value, attr) do
    encode_data(Decimal.new(value), attr)
  end

  defp size_for_precision(precision) do
    cond do
      precision < 10 -> 4
      precision < 20 -> 8
      precision < 29 -> 12
      precision < 39 -> 16
    end
  end
end
