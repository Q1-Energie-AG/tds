defmodule Tds.Types.Decoder do
  @moduledoc """
  Decoder for TDS data types
  """
  import Tds.BinaryUtils
  import Tds.Utils

  alias Tds.Encoding.UCS2

  @year_1900_days :calendar.date_to_gregorian_days({1900, 1, 1})
  @secs_in_min 60
  @secs_in_hour 60 * @secs_in_min

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

  # Fixed Data Types with their length
  @fixed_data_types %{
    @tds_data_type_null => 0,
    @tds_data_type_tinyint => 1,
    @tds_data_type_bit => 1,
    @tds_data_type_smallint => 2,
    @tds_data_type_int => 4,
    @tds_data_type_smalldatetime => 4,
    @tds_data_type_real => 4,
    @tds_data_type_money => 8,
    @tds_data_type_datetime => 8,
    @tds_data_type_float => 8,
    @tds_data_type_smallmoney => 4,
    @tds_data_type_bigint => 8
  }

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

  @variable_data_types [
    @tds_data_type_uniqueidentifier,
    @tds_data_type_intn,
    @tds_data_type_decimal,
    @tds_data_type_numeric,
    @tds_data_type_bitn,
    @tds_data_type_decimaln,
    @tds_data_type_numericn,
    @tds_data_type_floatn,
    @tds_data_type_moneyn,
    @tds_data_type_datetimen,
    @tds_data_type_daten,
    @tds_data_type_timen,
    @tds_data_type_datetime2n,
    @tds_data_type_datetimeoffsetn,
    @tds_data_type_char,
    @tds_data_type_varchar,
    @tds_data_type_binary,
    @tds_data_type_varbinary,
    @tds_data_type_bigvarbinary,
    @tds_data_type_bigvarchar,
    @tds_data_type_bigbinary,
    @tds_data_type_bigchar,
    @tds_data_type_nvarchar,
    @tds_data_type_nchar,
    @tds_data_type_xml,
    @tds_data_type_udt,
    @tds_data_type_text,
    @tds_data_type_image,
    @tds_data_type_ntext,
    @tds_data_type_variant
  ]

  # @tds_plp_marker 0xffff
  @tds_plp_null 0xFFFFFFFFFFFFFFFF
  # @tds_plp_unknown 0xfffffffffffffffe

  def decode(data) do
    <<
      _ord::little-unsigned-16,
      length::size(8),
      name::binary-size(length)-unit(16),
      _status::size(8),
      _usertype::size(32),
      _flags::size(16),
      data::binary
    >> = data

    name = UCS2.to_string(name)

    {type_info, tail} = decode_info(data)
    {value, tail} = decode_data(type_info, tail)
    {%Tds.Parameter{name: name, value: value, direction: :output}, tail}
  end

  def decode_column(data) do
    {type_info, tail} = decode_info(data)
    {name, tail} = decode_column_name(tail)

    {Map.put(type_info, :name, name), tail}
  end

  def decode_data(
        %{data_type: :fixed, data_type_code: data_type_code, length: length},
        <<tail::binary>>
      ) do
    <<value_binary::binary-size(length)-unit(8), tail::binary>> = tail

    value =
      case data_type_code do
        @tds_data_type_null ->
          nil

        @tds_data_type_bit ->
          value_binary != <<0x00>>

        @tds_data_type_smalldatetime ->
          decode_smalldatetime(value_binary)

        @tds_data_type_smallmoney ->
          decode_smallmoney(value_binary)

        @tds_data_type_real ->
          <<val::little-float-32>> = value_binary
          Float.round(val, 4)

        @tds_data_type_datetime ->
          decode_datetime(value_binary)

        @tds_data_type_float ->
          <<val::little-float-64>> = value_binary
          Float.round(val, 8)

        @tds_data_type_money ->
          decode_money(value_binary)

        _ ->
          <<val::little-signed-size(length)-unit(8)>> = value_binary
          val
      end

    {value, tail}
  end

  # ByteLength Types
  def decode_data(%{data_reader: :bytelen}, <<0x00, tail::binary>>),
    do: {nil, tail}

  def decode_data(
        %{
          data_type_code: data_type_code,
          data_reader: :bytelen,
          length: length
        } = data_info,
        <<size::unsigned-8, data::binary-size(size), tail::binary>>
      ) do
    value =
      cond do
        data_type_code == @tds_data_type_daten ->
          decode_date(data)

        data_type_code == @tds_data_type_timen ->
          decode_time(data_info[:scale], data)

        data_type_code == @tds_data_type_datetime2n ->
          decode_datetime2(data_info[:scale], data)

        data_type_code == @tds_data_type_datetimeoffsetn ->
          decode_datetimeoffset(data_info[:scale], data)

        data_type_code == @tds_data_type_uniqueidentifier ->
          decode_uuid(:binary.copy(data))

        data_type_code == @tds_data_type_intn ->
          case length do
            1 ->
              <<val::unsigned-8, _tail::binary>> = data
              val

            2 ->
              <<val::little-signed-16, _tail::binary>> = data
              val

            4 ->
              <<val::little-signed-32, _tail::binary>> = data
              val

            8 ->
              <<val::little-signed-64, _tail::binary>> = data
              val
          end

        data_type_code in [
          @tds_data_type_decimal,
          @tds_data_type_numeric,
          @tds_data_type_decimaln,
          @tds_data_type_numericn
        ] ->
          decode_decimal(data_info[:precision], data_info[:scale], data)

        data_type_code == @tds_data_type_bitn ->
          data != <<0x00>>

        data_type_code == @tds_data_type_floatn ->
          data = data <> tail
          len = length * 8
          <<val::little-float-size(len), _::binary>> = data
          val

        data_type_code == @tds_data_type_moneyn ->
          case length do
            4 -> decode_smallmoney(data)
            8 -> decode_money(data)
          end

        data_type_code == @tds_data_type_datetimen ->
          case length do
            4 -> decode_smalldatetime(data)
            8 -> decode_datetime(data)
          end

        data_type_code in [
          @tds_data_type_char,
          @tds_data_type_varchar
        ] ->
          decode_char(data_info, data)

        data_type_code in [
          @tds_data_type_binary,
          @tds_data_type_varbinary
        ] ->
          :binary.copy(data)
      end

    {value, tail}
  end

  # ShortLength Types
  def decode_data(%{data_reader: :shortlen}, <<0xFF, 0xFF, tail::binary>>),
    do: {nil, tail}

  def decode_data(
        %{data_type_code: data_type_code, data_reader: :shortlen} = data_info,
        <<size::little-unsigned-16, data::binary-size(size), tail::binary>>
      ) do
    value =
      cond do
        data_type_code in [
          @tds_data_type_bigvarchar,
          @tds_data_type_bigchar
        ] ->
          decode_char(data_info, data)

        data_type_code in [
          @tds_data_type_bigvarbinary,
          @tds_data_type_bigbinary
        ] ->
          :binary.copy(data)

        data_type_code in [
          @tds_data_type_nvarchar,
          @tds_data_type_nchar
        ] ->
          decode_nchar(data_info, data)

        data_type_code == @tds_data_type_udt ->
          decode_udt(data_info, :binary.copy(data))
      end

    {value, tail}
  end

  def decode_data(%{data_reader: :longlen}, <<0x00, tail::binary>>),
    do: {nil, tail}

  def decode_data(
        %{data_type_code: data_type_code, data_reader: :longlen} = data_info,
        <<
          text_ptr_size::unsigned-8,
          _text_ptr::size(text_ptr_size)-unit(8),
          _timestamp::unsigned-64,
          size::little-signed-32,
          data::binary-size(size)-unit(8),
          tail::binary
        >>
      ) do
    value =
      case data_type_code do
        @tds_data_type_text -> decode_char(data_info, data)
        @tds_data_type_ntext -> decode_nchar(data_info, data)
        @tds_data_type_image -> :binary.copy(data)
        _ -> nil
      end

    {value, tail}
  end

  # TODO Variant Types

  def decode_data(%{data_reader: :plp}, <<
        @tds_plp_null::little-unsigned-64,
        tail::binary
      >>),
      do: {nil, tail}

  def decode_data(
        %{data_type_code: data_type_code, data_reader: :plp} = data_info,
        <<_size::little-unsigned-64, tail::binary>>
      ) do
    {data, tail} = decode_plp_chunk(tail, <<>>)

    value =
      cond do
        data_type_code == @tds_data_type_xml ->
          decode_xml(data_info, data)

        data_type_code in [
          @tds_data_type_bigvarchar,
          @tds_data_type_bigchar,
          @tds_data_type_text
        ] ->
          decode_char(data_info, data)

        data_type_code in [
          @tds_data_type_bigvarbinary,
          @tds_data_type_bigbinary,
          @tds_data_type_image
        ] ->
          data

        data_type_code in [
          @tds_data_type_nvarchar,
          @tds_data_type_nchar,
          @tds_data_type_ntext
        ] ->
          decode_nchar(data_info, data)

        data_type_code == @tds_data_type_udt ->
          decode_udt(data_info, data)
      end

    {value, tail}
  end

  defp to_atom(token) do
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

  defp decode_column_name(<<length::int8(), name::binary-size(length)-unit(16), tail::binary>>),
    do: {UCS2.to_string(name), tail}

  defp decode_info(<<data_type_code::unsigned-8, tail::binary>>)
       when is_map_key(@fixed_data_types, data_type_code) do
    {%{
       data_type: :fixed,
       data_type_code: data_type_code,
       length: @fixed_data_types[data_type_code],
       data_type_name: to_atom(data_type_code)
     }, tail}
  end

  defp decode_info(<<data_type_code::unsigned-8, tail::binary>>)
       when data_type_code in @variable_data_types do
    def_type_info = %{
      data_type: :variable,
      data_type_code: data_type_code,
      sql_type: to_atom(data_type_code)
    }

    cond do
      data_type_code == @tds_data_type_daten ->
        length = 3

        type_info =
          def_type_info
          |> Map.put(:length, length)
          |> Map.put(:data_reader, :bytelen)

        {type_info, tail}

      data_type_code in [
        @tds_data_type_timen,
        @tds_data_type_datetime2n,
        @tds_data_type_datetimeoffsetn
      ] ->
        <<scale::unsigned-8, rest::binary>> = tail

        length =
          cond do
            scale in [0, 1, 2] -> 3
            scale in [3, 4] -> 4
            scale in [5, 6, 7] -> 5
            true -> nil
          end

        length =
          case data_type_code do
            @tds_data_type_datetime2n -> length + 3
            @tds_data_type_datetimeoffsetn -> length + 5
            _ -> length
          end

        type_info =
          def_type_info
          |> Map.put(:scale, scale)
          |> Map.put(:length, length)
          |> Map.put(:data_reader, :bytelen)

        {type_info, rest}

      data_type_code in [
        @tds_data_type_numericn,
        @tds_data_type_decimaln
      ] ->
        <<
          length::little-unsigned-8,
          precision::unsigned-8,
          scale::unsigned-8,
          rest::binary
        >> = tail

        type_info =
          def_type_info
          |> Map.put(:precision, precision)
          |> Map.put(:scale, scale)
          |> Map.put(:length, length)
          |> Map.put(:data_reader, :bytelen)

        {type_info, rest}

      data_type_code in [
        @tds_data_type_uniqueidentifier,
        @tds_data_type_intn,
        @tds_data_type_decimal,
        @tds_data_type_numeric,
        @tds_data_type_bitn,
        @tds_data_type_floatn,
        @tds_data_type_moneyn,
        @tds_data_type_datetimen,
        @tds_data_type_binary,
        @tds_data_type_varbinary
      ] ->
        <<length::little-unsigned-8, rest::binary>> = tail

        type_info =
          def_type_info
          |> Map.put(:length, length)
          |> Map.put(:data_reader, :bytelen)

        {type_info, rest}

      data_type_code in [
        @tds_data_type_char,
        @tds_data_type_varchar
      ] ->
        <<length::little-unsigned-8, collation::binary-5, rest::binary>> = tail
        {:ok, collation} = decode_collation(collation)

        type_info =
          def_type_info
          |> Map.put(:length, length)
          |> Map.put(:data_reader, :bytelen)
          |> Map.put(:collation, collation)

        {type_info, rest}

      data_type_code == @tds_data_type_xml ->
        {_schema_info, rest} = decode_schema_info(tail)

        type_info =
          def_type_info
          |> Map.put(:data_reader, :plp)

        {type_info, rest}

      data_type_code in [
        @tds_data_type_bigvarchar,
        @tds_data_type_bigchar,
        @tds_data_type_nvarchar,
        @tds_data_type_nchar
      ] ->
        <<length::little-unsigned-16, collation::binary-5, rest::binary>> = tail
        {:ok, collation} = decode_collation(collation)

        type_info =
          def_type_info
          |> Map.put(:collation, collation)
          |> Map.put(
            :data_reader,
            if(length == 0xFFFF, do: :plp, else: :shortlen)
          )
          |> Map.put(:length, length)

        {type_info, rest}

      data_type_code in [
        @tds_data_type_bigvarbinary,
        @tds_data_type_bigbinary,
        @tds_data_type_udt
      ] ->
        <<length::little-unsigned-16, rest::binary>> = tail

        type_info =
          def_type_info
          |> Map.put(
            :data_reader,
            if(length == 0xFFFF, do: :plp, else: :shortlen)
          )
          |> Map.put(:length, length)

        {type_info, rest}

      data_type_code in [@tds_data_type_text, @tds_data_type_ntext] ->
        <<
          length::little-unsigned-32,
          collation::binary-5,
          numparts::signed-8,
          rest::binary
        >> = tail

        {:ok, collation} = decode_collation(collation)

        type_info =
          def_type_info
          |> Map.put(:collation, collation)
          |> Map.put(:data_reader, :longlen)
          |> Map.put(:length, length)

        rest =
          Enum.reduce(
            1..numparts,
            rest,
            fn _,
               <<tsize::little-unsigned-16, _table_name::binary-size(tsize)-unit(16),
                 next_rest::binary>> ->
              next_rest
            end
          )

        {type_info, rest}

      data_type_code == @tds_data_type_image ->
        # TODO NumBarts Reader
        <<length::signed-32, numparts::signed-8, rest::binary>> = tail

        rest =
          Enum.reduce(
            1..numparts,
            rest,
            fn _, <<s::unsigned-16, _str::size(s)-unit(16), next::binary>> ->
              next
            end
          )

        type_info =
          def_type_info
          |> Map.put(:length, length)
          |> Map.put(:data_reader, :bytelen)

        {type_info, rest}

      data_type_code == @tds_data_type_variant ->
        <<length::signed-32, rest::binary>> = tail

        type_info =
          def_type_info
          |> Map.put(:length, length)
          |> Map.put(:data_reader, :variant)

        {type_info, rest}
    end
  end

  @spec decode_collation(binpart :: <<_::40>>) ::
          {:ok, Tds.Protocol.Collation.t()}
          | {:error, :more}
          | {:error, any}
  defdelegate decode_collation(binpart),
    to: Tds.Protocol.Collation,
    as: :decode

  #
  #  Data Decoders
  #

  defp decode_plp_chunk(<<0::little-unsigned-32, tail::binary>>, buf), do: {buf, tail}

  defp decode_plp_chunk(
         <<
           chunksize::little-unsigned-32,
           chunk::binary-size(chunksize)-unit(8),
           tail::binary
         >>,
         buf
       ) do
    decode_plp_chunk(tail, buf <> :binary.copy(chunk))
  end

  defp decode_smallmoney(<<money::little-signed-32>>) do
    Float.round(money * 0.0001, 4)
  end

  defp decode_money(<<
         money_m::little-unsigned-32,
         money_l::little-unsigned-32
       >>) do
    <<money::signed-64>> = <<money_m::32, money_l::32>>
    Float.round(money * 0.0001, 4)
  end

  defp decode_schema_info(<<0x00, tail::binary>>) do
    {nil, tail}
  end

  defp decode_schema_info(<<0x01, tail::binary>>) do
    <<
      dblen::little-unsigned-8,
      db::binary-size(dblen)-unit(16),
      prefixlen::little-unsigned-8,
      prefix::binary-size(prefixlen)-unit(16),
      schemalen::little-unsigned-16,
      schema::binary-size(schemalen)-unit(16),
      rest::binary
    >> = tail

    schema_info = %{
      db: UCS2.to_string(db),
      prefix: UCS2.to_string(prefix),
      schema: UCS2.to_string(schema)
    }

    {schema_info, rest}
  end

  defp decode_uuid(<<_::128>> = bin), do: bin

  # Decimal
  defp decode_decimal(precision, scale, <<sign::int8(), value::binary>>) do
    size = byte_size(value)
    <<value::little-size(size)-unit(8)>> = value

    Decimal.Context.update(&Map.put(&1, :precision, precision))

    case sign do
      0 -> Decimal.new(-1, value, -scale)
      _ -> Decimal.new(1, value, -scale)
    end
  end

  defp decode_char(data_info, <<data::binary>>) do
    Tds.Utils.decode_chars(data, data_info.collation.codepage)
  end

  defp decode_nchar(_data_info, <<data::binary>>), do: UCS2.to_string(data)

  defp decode_xml(_data_info, <<data::binary>>), do: UCS2.to_string(data)

  # UDT, if used, should be decoded by app that uses it,
  # tho we could've registered UDT types on connection
  # Example could be ecto, where custom type is created
  # special case are built in udt types such as HierarchyId
  defp decode_udt(%{}, <<data::binary>>), do: data

  defp decode_date(<<days::little-24>>) do
    date = :calendar.gregorian_days_to_date(days + 366)

    if use_elixir_calendar_types?() do
      Date.from_erl!(date, Calendar.ISO)
    else
      date
    end
  end

  # SmallDateTime
  defp decode_smalldatetime(<<
         days::little-unsigned-16,
         mins::little-unsigned-16
       >>) do
    date = :calendar.gregorian_days_to_date(@year_1900_days + days)
    hour = trunc(mins / 60)
    min = trunc(mins - hour * 60)

    if use_elixir_calendar_types?() do
      NaiveDateTime.from_erl!({date, {hour, min, 0}})
    else
      {date, {hour, min, 0, 0}}
    end
  end

  # DateTime
  defp decode_datetime(<<
         days::little-signed-32,
         secs300::little-unsigned-32
       >>) do
    # Logger.debug "#{inspect {days, secs300}}"
    date = :calendar.gregorian_days_to_date(@year_1900_days + days)

    milliseconds = round(secs300 * 10 / 3)
    usec = rem(milliseconds, 1_000)

    seconds = div(milliseconds, 1_000)

    {_, {h, m, s}} = :calendar.seconds_to_daystime(seconds)

    if use_elixir_calendar_types?() do
      NaiveDateTime.from_erl!(
        {date, {h, m, s}},
        {usec * 1_000, 3},
        Calendar.ISO
      )
    else
      {date, {h, m, s, usec}}
    end
  end

  defp decode_time(scale, <<fsec::binary>>) do
    # this is kind of rendudant, since "size" can be, and is, read from token
    parsed_fsec =
      cond do
        scale in [0, 1, 2] ->
          <<parsed_fsec::little-unsigned-24>> = fsec
          parsed_fsec

        scale in [3, 4] ->
          <<parsed_fsec::little-unsigned-32>> = fsec
          parsed_fsec

        scale in [5, 6, 7] ->
          <<parsed_fsec::little-unsigned-40>> = fsec
          parsed_fsec
      end

    fs_per_sec = trunc(:math.pow(10, scale))

    hour = trunc(parsed_fsec / fs_per_sec / @secs_in_hour)
    parsed_fsec = parsed_fsec - hour * @secs_in_hour * fs_per_sec

    min = trunc(parsed_fsec / fs_per_sec / @secs_in_min)
    parsed_fsec = parsed_fsec - min * @secs_in_min * fs_per_sec

    sec = trunc(parsed_fsec / fs_per_sec)

    parsed_fsec = trunc(parsed_fsec - sec * fs_per_sec)

    if use_elixir_calendar_types?() do
      {usec, scale} =
        if scale > 6 do
          {trunc(parsed_fsec / 10), 6}
        else
          {trunc(parsed_fsec * :math.pow(10, 6 - scale)), scale}
        end

      Time.from_erl!({hour, min, sec}, {usec, scale})
    else
      {hour, min, sec, parsed_fsec}
    end
  end

  # DateTime2
  defp decode_datetime2(scale, <<data::binary>>) do
    {time, date} =
      cond do
        scale in [0, 1, 2] ->
          <<time::binary-3, date::binary-3>> = data
          {time, date}

        scale in [3, 4] ->
          <<time::binary-4, date::binary-3>> = data
          {time, date}

        scale in [5, 6, 7] ->
          <<time::binary-5, date::binary-3>> = data
          {time, date}

        true ->
          raise "DateTime Scale Unknown"
      end

    date = decode_date(date)
    time = decode_time(scale, time)

    with true <- use_elixir_calendar_types?(),
         {:ok, datetime2} <- NaiveDateTime.new(date, time) do
      datetime2
    else
      false -> {date, time}
      {:error, error} -> raise DBConnection.EncodeError, error
    end
  end

  # DateTimeOffset
  defp decode_datetimeoffset(scale, <<data::binary>>) do
    {datetime, offset_min} =
      cond do
        scale in [0, 1, 2] ->
          <<datetime::binary-6, offset_min::little-signed-16>> = data
          {datetime, offset_min}

        scale in [3, 4] ->
          <<datetime::binary-7, offset_min::little-signed-16>> = data
          {datetime, offset_min}

        scale in [5, 6, 7] ->
          <<datetime::binary-8, offset_min::little-signed-16>> = data
          {datetime, offset_min}

        true ->
          raise DBConnection.EncodeError, "DateTimeOffset Scale invalid"
      end

    case decode_datetime2(scale, datetime) do
      {date, time} ->
        {date, time, offset_min}

      %NaiveDateTime{} = dt ->
        offset = offset_min * 60

        str =
          dt
          |> NaiveDateTime.add(offset)
          |> NaiveDateTime.to_iso8601()

        sign = if offset_min >= 0, do: "+", else: "-"

        h = trunc(offset_min / 60)

        m =
          Integer.to_string(offset_min - h * 60)
          |> String.pad_leading(2, "0")

        h =
          abs(h)
          |> Integer.to_string()
          |> String.pad_leading(2, "0")

        {:ok, datetime, ^offset} = DateTime.from_iso8601("#{str}#{sign}#{h}:#{m}")

        datetime
    end
  end
end
