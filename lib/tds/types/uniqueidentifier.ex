defmodule Tds.Types.Uniqueidentifier do
  alias Tds.Encoding.UCS2
  alias Tds.Parameter

  @tds_data_type_uniqueidentifier 0x24

  def encode(%Parameter{name: name} = p) do
    p_name = UCS2.from_string(name)
    p_flags = p |> Parameter.option_flags()

    {type_data, type_attr} = encode_type(p)

    p_meta_data = <<byte_size(name)>> <> p_name <> p_flags <> type_data

    p_meta_data <> encode_data(p.value, type_attr)
  end

  defp encode_type(%Parameter{value: value}) do
    length =
      if is_nil(value) do
        0x00
      else
        0x10
      end

    type = @tds_data_type_uniqueidentifier
    data = <<type, length>>
    {data, []}
  end

  defp encode_data(value, _) do
    if value != nil do
      <<0x10>> <> encode_uuid(value)
    else
      <<0x00>>
    end
  end

  defp encode_uuid(<<_::64, ?-, _::32, ?-, _::32, ?-, _::32, ?-, _::96>> = string) do
    raise ArgumentError,
          "trying to load string UUID as Tds.Types.UUID: #{inspect(string)}. " <>
            "Maybe you wanted to declare :uuid as your database field?"
  end

  defp encode_uuid(<<_::128>> = bin), do: bin

  defp encode_uuid(any),
    do: raise(ArgumentError, "Invalid uuid value #{inspect(any)}")
end
