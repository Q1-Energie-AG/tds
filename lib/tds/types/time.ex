defmodule Tds.Types.Time do
  alias Tds.Encoding.UCS2
  alias Tds.Parameter

  @tds_data_type_timen 0x29
  @max_time_scale 7

  def encode(%Parameter{name: name} = p) do
    p_name = UCS2.from_string(name)
    p_flags = p |> Parameter.option_flags()

    {type_data, type_attr} = encode_type(p)

    p_meta_data = <<byte_size(name)>> <> p_name <> p_flags <> type_data

    p_meta_data <> encode_data(p.value, type_attr)
  end

  defp encode_type(%Parameter{value: value}) do
    # Logger.debug "encode_time_type"
    type = @tds_data_type_timen

    case value do
      nil ->
        {<<type, 0x07>>, scale: 1}

      {_, _, _} ->
        {<<type, 0x07>>, scale: 1}

      {_, _, _, fsec} ->
        scale = Integer.digits(fsec) |> length()
        {<<type, 0x07>>, scale: scale}

      %Time{microsecond: {_, scale}} ->
        {<<type, scale>>, scale: scale}

      other ->
        raise ArgumentError, "Value #{inspect(other)} is not valid time"
    end
  end

  defp encode_data(value, _attr) do
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

  # time(n) is represented as one unsigned integer that represents the number of
  # 10-n second increments since 12 AM within a day. The length, in bytes, of
  # that integer depends on the scale n as follows:
  # 3 bytes if 0 <= n < = 2.
  # 4 bytes if 3 <= n < = 4.
  # 5 bytes if 5 <= n < = 7.
  def encode_time(nil), do: {nil, 0}

  def encode_time({h, m, s}), do: encode_time({h, m, s, 0})

  def encode_time(%Time{} = t) do
    {h, m, s} = Time.to_erl(t)
    {_, scale} = t.microsecond
    # fix ms
    fsec = microsecond_to_fsec(t.microsecond)

    encode_time({h, m, s, fsec}, scale)
  end

  def encode_time(time), do: encode_time(time, @max_time_scale)

  def encode_time({h, m, s}, scale), do: encode_time({h, m, s, 0}, scale)

  def encode_time({hour, min, sec, fsec}, scale) do
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
end
