defmodule Tds.Types.Decimal do
  alias Tds.Encoding.UCS2
  alias Tds.Parameter

  @tds_data_type_decimaln 0x6A

  def encode(%Parameter{name: name} = p) do
    p_name = UCS2.from_string(name)
    p_flags = p |> Parameter.option_flags()

    {type_data, type_attr} = encode_type(p)

    p_meta_data = <<byte_size(name)>> <> p_name <> p_flags <> type_data

    p_meta_data <> encode_data(p.value, type_attr)
  end

  def encode_type(%Parameter{value: nil}) do
    type = @tds_data_type_decimaln
    {<<type, 0x00, 0x00, 0x00, 0x00>>, []}
  end

  def encode_type(%Parameter{value: value}) do
    Decimal.Context.update(&Map.put(&1, :precision, 38))

    value_list =
      value
      |> Decimal.abs()
      |> Decimal.to_string(:normal)
      |> String.split(".")

    {precision, scale} =
      case value_list do
        [p, s] ->
          len = String.length(s)
          {String.length(p) + len, len}

        [p] ->
          {String.length(p), 0}
      end

    value =
      value
      |> Decimal.abs()
      |> Map.fetch!(:coef)
      |> :binary.encode_unsigned(:little)

    value_size = byte_size(value)

    len =
      cond do
        precision <= 9 -> 4
        precision <= 19 -> 8
        precision <= 28 -> 12
        precision <= 38 -> 16
      end

    padding = len - value_size
    value_size = value_size + padding + 1

    type = @tds_data_type_decimaln
    data = <<type, value_size, precision, scale>>
    {data, precision: precision, scale: scale}
  end

  # decimal
  def encode_data(%Decimal{} = value, attr) do
    Decimal.Context.update(&Map.put(&1, :precision, 38))
    precision = attr[:precision]

    d =
      value
      |> Decimal.to_string()
      |> Decimal.new()

    sign =
      case d.sign do
        1 -> 1
        -1 -> 0
      end

    value =
      d
      |> Decimal.abs()
      |> Map.fetch!(:coef)

    value_binary = :binary.encode_unsigned(value, :little)

    value_size = byte_size(value_binary)

    len =
      cond do
        precision <= 9 -> 4
        precision <= 19 -> 8
        precision <= 28 -> 12
        precision <= 38 -> 16
      end

    {byte_len, padding} = {len, len - value_size}
    byte_len = byte_len + 1
    value_binary = value_binary <> <<0::size(padding)-unit(8)>>
    <<byte_len>> <> <<sign>> <> value_binary
  end

  def encode_data(nil, _), do: <<>>
  # <<0, 0, 0, 0>
  # do: <<0x00::little-unsigned-32>>

  def encode_data(value, attr) do
    encode_data(Decimal.new(value), attr)
  end
end
