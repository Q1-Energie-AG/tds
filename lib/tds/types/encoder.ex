defmodule Tds.Types.Encoder do
  @moduledoc false
  alias Tds.Parameter
  alias Tds.Types

  def encode(%Parameter{type: nil} = param), do: param |> Parameter.fix_data_type() |> encode()

  def encode(%Parameter{type: type} = param) do
    case type do
      :boolean -> Types.Binary.encode(param)
      :binary -> Types.Binary.encode(param)
      :decimal -> Types.Decimal.encode(param)
      :numeric -> Types.Decimal.encode(param)
      :integer -> Types.Integer.encode(param)
      :float -> Types.Float.encode(param)
      :datetime -> Types.Datetime.encode(param)
      :smalldatetime -> Types.Datetime.encode(param)
      :date -> Types.Date.encode(param)
      :time -> Types.Time.encode(param)
      :datetime2 -> Types.Datetime2.encode(param)
      :datetimeoffset -> Types.Datetimeoffset.encode(param)
      :uuid -> Types.Uniqueidentifier.encode(param)
      _ -> Types.Varchar.encode(param)
    end
  end

  @doc """
  Creates the Parameter Descriptor for the selected type
  """
  def encode_param_descriptor(%Parameter{name: name, value: value, type: type} = param)
      when type != nil do
    desc =
      case type do
        :uuid ->
          "uniqueidentifier"

        :datetime ->
          "datetime"

        :datetime2 ->
          case value do
            %NaiveDateTime{microsecond: {_, scale}} ->
              "datetime2(#{scale})"

            _ ->
              "datetime2"
          end

        :datetimeoffset ->
          case value do
            %DateTime{microsecond: {_, s}} ->
              "datetimeoffset(#{s})"

            _ ->
              "datetimeoffset"
          end

        :date ->
          "date"

        :time ->
          case value do
            %Time{microsecond: {_, scale}} ->
              "time(#{scale})"

            _ ->
              "time"
          end

        :smalldatetime ->
          "smalldatetime"

        :binary ->
          encode_binary_descriptor(value)

        :string ->
          cond do
            is_nil(value) -> "nvarchar(1)"
            String.length(value) <= 0 -> "nvarchar(1)"
            String.length(value) <= 2_000 -> "nvarchar(2000)"
            true -> "nvarchar(max)"
          end

        :varchar ->
          cond do
            is_nil(value) -> "varchar(1)"
            String.length(value) <= 0 -> "varchar(1)"
            String.length(value) <= 2_000 -> "varchar(2000)"
            true -> "varchar(max)"
          end

        :integer ->
          case value do
            0 ->
              "int"

            val when val >= 1 ->
              "bigint"

            _ ->
              precision =
                value
                |> Integer.to_string()
                |> String.length()

              "decimal(#{precision - 1}, 0)"
          end

        :bigint ->
          "bigint"

        :decimal ->
          encode_decimal_descriptor(param)

        :numeric ->
          encode_decimal_descriptor(param)

        :float ->
          encode_float_descriptor(param)

        :boolean ->
          "bit"

        _ ->
          # this should fix issues when column is varchar but parameter
          # is threated as nvarchar(..) since nothing defines parameter
          # as varchar.
          latin1 = :unicode.characters_to_list(value || "", :latin1)
          utf8 = :unicode.characters_to_list(value || "", :utf8)

          db_type =
            if latin1 == utf8,
              do: "varchar",
              else: "nvarchar"

          # this is same .net driver uses in order to avoid too many
          # cached execution plans, it must be always same length otherwise it will
          # use too much memory in sql server to cache each plan per param size
          cond do
            is_nil(value) -> "#{db_type}(1)"
            String.length(value) <= 0 -> "#{db_type}(1)"
            String.length(value) <= 2_000 -> "#{db_type}(2000)"
            true -> "#{db_type}(max)"
          end
      end

    "#{name} #{desc}"
  end

  # nil
  def encode_param_descriptor(param),
    do: param |> Parameter.fix_data_type() |> encode_param_descriptor()

  defp encode_decimal_descriptor(%Parameter{value: nil}),
    do: encode_binary_descriptor(nil)

  defp encode_decimal_descriptor(%Parameter{value: value} = param)
       when is_float(value) do
    encode_decimal_descriptor(%{param | value: Decimal.from_float(value)})
  end

  defp encode_decimal_descriptor(%Parameter{value: value} = param)
       when is_binary(value) or is_integer(value) do
    encode_decimal_descriptor(%{param | value: Decimal.new(value)})
  end

  defp encode_decimal_descriptor(%Parameter{value: %Decimal{} = dec}) do
    Decimal.Context.update(&Map.put(&1, :precision, 38))

    value_list =
      dec
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

    "decimal(#{precision}, #{scale})"
  end

  # Decimal.new/0 is undefined -- modifying params to hopefully fix
  defp encode_decimal_descriptor(%Parameter{type: :decimal, value: value} = param) do
    encode_decimal_descriptor(%{param | value: Decimal.new(value)})
  end

  defp encode_float_descriptor(%Parameter{value: nil}), do: "decimal(1,0)"

  defp encode_float_descriptor(%Parameter{value: value} = param)
       when is_float(value) do
    param
    |> Map.put(:value, Decimal.from_float(value))
    |> encode_float_descriptor
  end

  defp encode_float_descriptor(%Parameter{value: %Decimal{}}), do: "float(53)"

  defp encode_binary_descriptor(value) when is_integer(value),
    do: encode_binary_descriptor(<<value>>)

  defp encode_binary_descriptor(value) when is_nil(value), do: "varbinary(1)"

  defp encode_binary_descriptor(value) when byte_size(value) <= 0,
    do: "varbinary(1)"

  defp encode_binary_descriptor(value) when byte_size(value) > 0,
    do: "varbinary(max)"
end
