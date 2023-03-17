defmodule Tds.Types.Float do
  alias Tds.Encoding.UCS2
  alias Tds.Parameter

  @tds_data_type_floatn 0x6D

  def encode(%Parameter{name: name} = p) do
    p_name = UCS2.from_string(name)
    p_flags = p |> Parameter.option_flags()

    {type_data, type_attr} = encode_type(p)

    p_meta_data = <<byte_size(name)>> <> p_name <> p_flags <> type_data

    p_meta_data <> encode_data(p.value, type_attr)
  end

  def encode_type(%Parameter{value: nil}) do
    type = @tds_data_type_floatn
    {<<type, 0x00, 0x00, 0x00, 0x00>>, []}
  end

  def encode_type(%Parameter{value: value} = param)
      when is_float(value) do
    encode_type(%{param | value: Decimal.from_float(value)})
  end

  def encode_type(%Parameter{value: %Decimal{} = value}) do
    d_ctx = Decimal.Context.get()
    d_ctx = %{d_ctx | precision: 38}
    Decimal.Context.set(d_ctx)

    value_list =
      value
      |> Decimal.abs()
      |> Decimal.to_string(:normal)
      |> String.split(".")

    {precision, scale} =
      case value_list do
        [p, s] ->
          {String.length(p) + String.length(s), String.length(s)}

        [p] ->
          {String.length(p), 0}
      end

    dec_abs =
      value
      |> Decimal.abs()

    value =
      dec_abs.coef
      |> :binary.encode_unsigned(:little)

    value_size = byte_size(value)

    # keep max precision
    len = 8

    padding = len - value_size
    value_size = value_size + padding

    type = @tds_data_type_floatn
    data = <<type, value_size>>
    {data, precision: precision, scale: scale}
  end

  def encode_data(nil, _), do: <<0>>

  def encode_data(value, _), do: <<0x08, value::little-float-64>>
end
