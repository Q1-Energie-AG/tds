defmodule Tds.Types.Encoder do
  @moduledoc false

  alias Tds.Encoding.UCS2
  alias Tds.Parameter

  @year_1900_days :calendar.date_to_gregorian_days({1900, 1, 1})
  @max_time_scale 7

  # Zero Length Data Types
  @tds_data_type_null 0x1F

  # Fixed Length Data Types
  # See: https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/859eb3d2-80d3-40f6-a637-414552c9c552
  @tds_data_type_tinyint 0x30
  @tds_data_type_bit 0x32
  @tds_data_type_smallint 0x34
  @tds_data_type_int 0x38
  @tds_data_type_smalldatetime 0x3A
  @tds_data_type_real 0x3B
  @tds_data_type_money 0x3C
  @tds_data_type_datetime 0x3D
  @tds_data_type_float 0x3E
  @tds_data_type_smallmoney 0x7A
  @tds_data_type_bigint 0x7F

  # Variable-Length Data Types
  # See: https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/ce3183a6-9d89-47e8-a02f-de5a1a1303de
  @tds_data_type_uniqueidentifier 0x24
  @tds_data_type_intn 0x26
  # legacy
  @tds_data_type_decimal 0x37
  # legacy
  @tds_data_type_numeric 0x3F
  @tds_data_type_bitn 0x68
  @tds_data_type_decimaln 0x6A
  @tds_data_type_numericn 0x6C
  @tds_data_type_floatn 0x6D
  @tds_data_type_moneyn 0x6E
  @tds_data_type_datetimen 0x6F
  @tds_data_type_daten 0x28
  @tds_data_type_timen 0x29
  @tds_data_type_datetime2n 0x2A
  @tds_data_type_datetimeoffsetn 0x2B
  @tds_data_type_char 0x2F
  @tds_data_type_varchar 0x27
  @tds_data_type_binary 0x2D
  @tds_data_type_varbinary 0x25
  @tds_data_type_bigvarbinary 0xA5
  @tds_data_type_bigvarchar 0xA7
  @tds_data_type_bigbinary 0xAD
  @tds_data_type_bigchar 0xAF
  @tds_data_type_nvarchar 0xE7
  @tds_data_type_nchar 0xEF
  @tds_data_type_xml 0xF1
  @tds_data_type_udt 0xF0
  @tds_data_type_text 0x23
  @tds_data_type_image 0x22
  @tds_data_type_ntext 0x63
  @tds_data_type_variant 0x62

  # @tds_plp_marker 0xffff
  @tds_plp_null 0xFFFFFFFFFFFFFFFF
  # @tds_plp_unknown 0xfffffffffffffffe

  #
  #  Data Type Decoders
  #

  def to_atom(token) do
    case token do
      @tds_data_type_null -> :null
      @tds_data_type_tinyint -> :tinyint
      @tds_data_type_bit -> :bit
      @tds_data_type_smallint -> :smallint
      @tds_data_type_int -> :int
      @tds_data_type_smalldatetime -> :smalldatetime
      @tds_data_type_real -> :real
      @tds_data_type_money -> :money
      @tds_data_type_datetime -> :datetime
      @tds_data_type_float -> :float
      @tds_data_type_smallmoney -> :smallmoney
      @tds_data_type_bigint -> :bigint
      @tds_data_type_uniqueidentifier -> :uniqueidentifier
      @tds_data_type_intn -> :intn
      @tds_data_type_decimal -> :decimal
      @tds_data_type_numeric -> :numeric
      @tds_data_type_bitn -> :bitn
      @tds_data_type_decimaln -> :decimaln
      @tds_data_type_numericn -> :numericn
      @tds_data_type_floatn -> :floatn
      @tds_data_type_moneyn -> :moneyn
      @tds_data_type_datetimen -> :datetimen
      @tds_data_type_daten -> :daten
      @tds_data_type_timen -> :timen
      @tds_data_type_datetime2n -> :datetime2n
      @tds_data_type_datetimeoffsetn -> :datetimeoffsetn
      @tds_data_type_char -> :char
      @tds_data_type_varchar -> :varchar
      @tds_data_type_binary -> :binary
      @tds_data_type_varbinary -> :varbinary
      @tds_data_type_bigvarbinary -> :bigvarbinary
      @tds_data_type_bigvarchar -> :bigvarchar
      @tds_data_type_bigbinary -> :bigbinary
      @tds_data_type_bigchar -> :bigchar
      @tds_data_type_nvarchar -> :nvarchar
      @tds_data_type_nchar -> :nchar
      @tds_data_type_xml -> :xml
      @tds_data_type_udt -> :udt
      @tds_data_type_text -> :text
      @tds_data_type_image -> :image
      @tds_data_type_ntext -> :ntext
      @tds_data_type_variant -> :variant
    end
  end

  @doc """
  Data Type Encoders
  Encodes the COLMETADATA for the data type
  """
  def encode_data_type(%Parameter{type: type} = param) when type != nil do
    case type do
      :boolean -> encode_binary_type(param)
      :binary -> encode_binary_type(param)
      :string -> encode_string_type(param)
      :integer -> encode_integer_type(param)
      :decimal -> encode_decimal_type(param)
      :numeric -> encode_decimal_type(param)
      :float -> encode_float_type(param)
      :smalldatetime -> encode_smalldatetime_type(param)
      :datetime -> encode_datetime_type(param)
      :datetime2 -> encode_datetime2_type(param)
      :datetimeoffset -> encode_datetimeoffset_type(param)
      :date -> encode_date_type(param)
      :time -> encode_time_type(param)
      :uuid -> encode_uuid_type(param)
      _ -> encode_string_type(param)
    end
  end

  def encode_data_type(param),
    do: param |> Parameter.fix_data_type() |> encode_data_type()

  # binary
  def encode_data(@tds_data_type_bigvarbinary, value, attr)
      when is_integer(value),
      do: encode_data(@tds_data_type_bigvarbinary, <<value>>, attr)

  def encode_data(@tds_data_type_bigvarbinary, nil, _),
    do: <<@tds_plp_null::little-unsigned-64>>

  def encode_data(@tds_data_type_bigvarbinary, value, _) do
    case byte_size(value) do
      # varbinary(max) gets encoded in chunks
      value_size when value_size > 8000 -> encode_plp(value)
      value_size -> <<value_size::little-unsigned-16>> <> value
    end
  end

  # string
  def encode_data(@tds_data_type_nvarchar, nil, _),
    do: <<@tds_plp_null::little-unsigned-64>>

  def encode_data(@tds_data_type_nvarchar, value, _) do
    value = UCS2.from_string(value)
    value_size = byte_size(value)

    cond do
      value_size <= 0 ->
        <<0x00::unsigned-64, 0x00::unsigned-32>>

      value_size > 8000 ->
        encode_plp(value)

      true ->
        <<value_size::little-size(2)-unit(8)>> <> value
    end
  end

  # integers
  def encode_data(_, value, _) when is_integer(value) do
    size = int_type_size(value)
    <<size>> <> <<value::little-signed-size(size)-unit(8)>>
  end

  def encode_data(@tds_data_type_intn, value, _) when value == nil do
    <<0>>
  end

  def encode_data(@tds_data_type_tinyint, value, _) when value == nil do
    <<0>>
  end

  # float
  def encode_data(@tds_data_type_floatn, nil, _) do
    <<0>>
  end

  def encode_data(@tds_data_type_floatn, value, _) do
    <<0x08, value::little-float-64>>
  end

  # decimal
  def encode_data(@tds_data_type_decimaln, %Decimal{} = value, attr) do
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

  def encode_data(@tds_data_type_decimaln, nil, _),
    # <<0, 0, 0, 0>
    do: <<0x00::little-unsigned-32>>

  def encode_data(@tds_data_type_decimaln = data_type, value, attr) do
    encode_data(data_type, Decimal.new(value), attr)
  end

  # uuid
  def encode_data(@tds_data_type_uniqueidentifier, value, _) do
    if value != nil do
      <<0x10>> <> encode_uuid(value)
    else
      <<0x00>>
    end
  end

  # datetime
  def encode_data(@tds_data_type_daten, value, _attr) do
    data = encode_date(value)

    if data == nil do
      <<0x00>>
    else
      <<0x03, data::binary>>
    end
  end

  def encode_data(@tds_data_type_timen, value, _attr) do
    # Logger.debug"encode_data_timen"
    {data, scale} = encode_time(value)
    # Logger.debug "#{inspect data}"
    if data == nil do
      <<0x00>>
    else
      len =
        cond do
          scale < 3 -> 0x03
          scale < 5 -> 0x04
          scale < 8 -> 0x05
        end

      <<len, data::binary>>
    end
  end

  def encode_data(@tds_data_type_datetimen, value, attr) do
    # Logger.debug "dtn #{inspect attr}"
    data =
      case attr[:length] do
        4 ->
          encode_smalldatetime(value)

        _ ->
          encode_datetime(value)
      end

    if data == nil do
      <<0x00>>
    else
      <<byte_size(data)::8>> <> data
    end
  end

  def encode_data(@tds_data_type_datetime2n, value, _attr) do
    # Logger.debug "EncodeData #{inspect value}"
    {data, scale} = encode_datetime2(value)

    if data == nil do
      <<0x00>>
    else
      # 0x08 length of binary for scale 7
      storage_size =
        cond do
          scale < 3 -> 0x06
          scale < 5 -> 0x07
          scale < 8 -> 0x08
        end

      <<storage_size>> <> data
    end
  end

  def encode_data(@tds_data_type_datetimeoffsetn, value, _attr) do
    # Logger.debug "encode_data_datetimeoffsetn #{inspect value}"
    data = encode_datetimeoffset(value)

    if data == nil do
      <<0x00>>
    else
      case value do
        %DateTime{microsecond: {_, s}} when s < 3 ->
          <<0x08, data::binary>>

        %DateTime{microsecond: {_, s}} when s < 5 ->
          <<0x09, data::binary>>

        _ ->
          <<0x0A, data::binary>>
      end
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

  defp encode_uuid(<<_::64, ?-, _::32, ?-, _::32, ?-, _::32, ?-, _::96>> = string) do
    raise ArgumentError,
          "trying to load string UUID as Tds.Types.UUID: #{inspect(string)}. " <>
            "Maybe you wanted to declare :uuid as your database field?"
  end

  defp encode_uuid(<<_::128>> = bin), do: bin

  defp encode_uuid(any),
    do: raise(ArgumentError, "Invalid uuid value #{inspect(any)}")

  defp encode_binary_type(%Parameter{value: ""} = param) do
    encode_string_type(param)
  end

  defp encode_binary_type(%Parameter{value: value} = param)
       when is_integer(value) do
    %{param | value: <<value>>} |> encode_binary_type
  end

  defp encode_binary_type(%Parameter{value: value}) do
    length = length_for_binary(value)
    type = @tds_data_type_bigvarbinary
    data = <<type>> <> length
    {type, data, []}
  end

  defp length_for_binary(nil), do: <<0xFF, 0xFF>>

  defp length_for_binary(value) do
    case byte_size(value) do
      # varbinary(max)
      value_size when value_size > 8000 -> <<0xFF, 0xFF>>
      value_size -> <<value_size::little-unsigned-16>>
    end
  end

  # defp encode_bit_type(%Parameter{}) do
  #   type = @tds_data_type_bigvarbinary
  #   data = <<type, 0x01>>
  #   {type, data, []}
  # end

  defp encode_uuid_type(%Parameter{value: value}) do
    length =
      if is_nil(value) do
        0x00
      else
        0x10
      end

    type = @tds_data_type_uniqueidentifier
    data = <<type, length>>
    {type, data, []}
  end

  defp encode_string_type(%Parameter{value: value}) do
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
    {type, data, [collation: collation]}
  end

  defp encode_integer_type(%Parameter{value: value}) do
    attributes = []
    type = @tds_data_type_intn

    length = int_type_size(value)
    attributes = Keyword.put(attributes, :length, length)

    {type, <<type, length>>, attributes}
  end

  defp encode_decimal_type(%Parameter{value: nil} = param) do
    encode_binary_type(param)
  end

  defp encode_decimal_type(%Parameter{value: value}) do
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
    {type, data, precision: precision, scale: scale}
  end

  defp encode_float_type(%Parameter{value: nil} = param) do
    encode_decimal_type(param)
  end

  defp encode_float_type(%Parameter{value: value} = param)
       when is_float(value) do
    encode_float_type(%{param | value: Decimal.from_float(value)})
  end

  defp encode_float_type(%Parameter{value: %Decimal{} = value}) do
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
    {type, data, precision: precision, scale: scale}
  end

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

  # def encode_binary_descriptor(value) when byte_size(value) > 8_000,
  #   do: "varbinary(max)"

  # def encode_binary_descriptor(value), do: "varbinary(#{byte_size(value)})"

  defp encode_plp(data) do
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

  defp int_type_size(int) when int == nil, do: 4
  defp int_type_size(int) when int in -254..255, do: 4
  defp int_type_size(int) when int in -32_768..32_767, do: 4
  defp int_type_size(int) when int in -2_147_483_648..2_147_483_647, do: 4

  defp int_type_size(int)
       when int in -9_223_372_036_854_775_808..9_223_372_036_854_775_807,
       do: 8

  defp int_type_size(int),
    do:
      raise(
        ArgumentError,
        "Erlang integer value #{int} is too big (more than 64bits) to fit tds" <>
          " integer/bigint. Please consider using Decimal.new/1 to maintain precision."
      )

  # Date

  defp encode_date(nil), do: nil

  defp encode_date(%Date{} = date), do: date |> Date.to_erl() |> encode_date()

  defp encode_date(date) do
    days = :calendar.date_to_gregorian_days(date) - 366
    <<days::little-24>>
  end

  defp encode_smalldatetime(nil), do: nil

  defp encode_smalldatetime({date, {hour, min, _}}),
    do: encode_smalldatetime({date, {hour, min, 0, 0}})

  defp encode_smalldatetime({date, {hour, min, _, _}}) do
    days = :calendar.date_to_gregorian_days(date) - @year_1900_days
    mins = hour * 60 + min
    encode_smalldatetime(days, mins)
  end

  defp encode_smalldatetime(days, mins) do
    <<days::little-unsigned-16, mins::little-unsigned-16>>
  end

  defp encode_datetime(nil), do: nil

  defp encode_datetime(%DateTime{} = dt),
    do: encode_datetime(DateTime.to_naive(dt))

  defp encode_datetime(%NaiveDateTime{} = dt) do
    {date, {h, m, s}} = NaiveDateTime.to_erl(dt)
    {msec, _} = dt.microsecond
    encode_datetime({date, {h, m, s, msec}})
  end

  defp encode_datetime({date, {h, m, s}}),
    do: encode_datetime({date, {h, m, s, 0}})

  defp encode_datetime({date, {h, m, s, us}}) do
    days = :calendar.date_to_gregorian_days(date) - @year_1900_days
    milliseconds = ((h * 60 + m) * 60 + s) * 1_000 + us / 1_000

    secs_300 = round(milliseconds / (10 / 3))

    {days, secs_300} =
      if secs_300 == 25_920_000 do
        {days + 1, 0}
      else
        {days, secs_300}
      end

    <<days::little-signed-32, secs_300::little-unsigned-32>>
  end

  # Time

  # time(n) is represented as one unsigned integer that represents the number of
  # 10-n second increments since 12 AM within a day. The length, in bytes, of
  # that integer depends on the scale n as follows:
  # 3 bytes if 0 <= n < = 2.
  # 4 bytes if 3 <= n < = 4.
  # 5 bytes if 5 <= n < = 7.
  defp encode_time(nil), do: {nil, 0}

  defp encode_time({h, m, s}), do: encode_time({h, m, s, 0})

  defp encode_time(%Time{} = t) do
    {h, m, s} = Time.to_erl(t)
    {_, scale} = t.microsecond
    # fix ms
    fsec = microsecond_to_fsec(t.microsecond)

    encode_time({h, m, s, fsec}, scale)
  end

  defp encode_time(time), do: encode_time(time, @max_time_scale)

  defp encode_time({h, m, s}, scale), do: encode_time({h, m, s, 0}, scale)

  defp encode_time({hour, min, sec, fsec}, scale) do
    # 10^scale fs in 1 sec
    fs_per_sec = trunc(:math.pow(10, scale))

    fsec = hour * 3600 * fs_per_sec + min * 60 * fs_per_sec + sec * fs_per_sec + fsec

    bin =
      cond do
        scale < 3 ->
          <<fsec::little-unsigned-24>>

        scale < 5 ->
          <<fsec::little-unsigned-32>>

        :else ->
          <<fsec::little-unsigned-40>>
      end

    {bin, scale}
  end

  defp microsecond_to_fsec({us, 6}),
    do: us

  defp microsecond_to_fsec({us, scale}),
    do: trunc(us / :math.pow(10, 6 - scale))

  defp encode_datetime2(value, scale \\ @max_time_scale)
  defp encode_datetime2(nil, _), do: {nil, 0}

  defp encode_datetime2({date, time}, scale) do
    {time, scale} = encode_time(time, scale)
    date = encode_date(date)
    {time <> date, scale}
  end

  defp encode_datetime2(%NaiveDateTime{} = value, _scale) do
    t = NaiveDateTime.to_time(value)
    {time, scale} = encode_time(t)
    date = encode_date(NaiveDateTime.to_date(value))
    {time <> date, scale}
  end

  defp encode_datetime2(value, scale) do
    raise ArgumentError,
          "value #{inspect(value)} with scale #{inspect(scale)} is not supported DateTime2 value"
  end

  defp encode_datetimeoffset(datetimetz, scale \\ @max_time_scale)
  defp encode_datetimeoffset(nil, _), do: nil

  defp encode_datetimeoffset({date, time, offset_min}, scale) do
    {datetime, _ignore_always_10bytes} = encode_datetime2({date, time}, scale)
    datetime <> <<offset_min::little-signed-16>>
  end

  defp encode_datetimeoffset(
         %DateTime{utc_offset: offset} = dt,
         scale
       ) do
    {datetime, _} =
      dt
      |> DateTime.add(-offset)
      |> DateTime.to_naive()
      |> encode_datetime2(scale)

    offset_min = trunc(offset / 60)

    datetime <> <<offset_min::little-signed-16>>
  end

  defp encode_datetime_type(%Parameter{}) do
    # Logger.debug "encode_datetime_type"
    type = @tds_data_type_datetimen
    data = <<type, 0x08>>
    {type, data, length: 8}
  end

  defp encode_smalldatetime_type(%Parameter{}) do
    # Logger.debug "encode_smalldatetime_type"
    type = @tds_data_type_datetimen
    data = <<type, 0x04>>
    {type, data, length: 4}
  end

  defp encode_date_type(%Parameter{}) do
    type = @tds_data_type_daten
    data = <<type>>
    {type, data, []}
  end

  defp encode_time_type(%Parameter{value: value}) do
    # Logger.debug "encode_time_type"
    type = @tds_data_type_timen

    case value do
      nil ->
        {type, <<type, 0x07>>, scale: 1}

      {_, _, _} ->
        {type, <<type, 0x07>>, scale: 1}

      {_, _, _, fsec} ->
        scale = Integer.digits(fsec) |> length()
        {type, <<type, 0x07>>, scale: scale}

      %Time{microsecond: {_, scale}} ->
        {type, <<type, scale>>, scale: scale}

      other ->
        raise ArgumentError, "Value #{inspect(other)} is not valid time"
    end
  end

  defp encode_datetime2_type(%Parameter{
         value: %NaiveDateTime{microsecond: {_, s}}
       }) do
    type = @tds_data_type_datetime2n
    data = <<type, s>>
    {type, data, scale: s}
  end

  defp encode_datetime2_type(%Parameter{}) do
    # Logger.debug "encode_datetime2_type"
    type = @tds_data_type_datetime2n
    data = <<type, 0x07>>
    {type, data, scale: 7}
  end

  defp encode_datetimeoffset_type(%Parameter{
         value: %DateTime{microsecond: {_, s}}
       }) do
    type = @tds_data_type_datetimeoffsetn
    data = <<type, s>>
    {type, data, scale: s}
  end

  defp encode_datetimeoffset_type(%Parameter{}) do
    type = @tds_data_type_datetimeoffsetn
    data = <<type, 0x07>>
    {type, data, scale: 7}
  end
end
