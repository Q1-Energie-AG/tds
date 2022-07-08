defmodule Tds.Utils do
  alias Tds.Encoding.Latin1

  @moduledoc false
  def encode_chars(string, to_codepage) do
    Application.get_env(:tds, :text_encoder, Latin1)
    |> apply(:encode, [string, to_codepage])
  end

  def decode_chars(binary, from_codepage) when is_binary(binary) do
    Application.get_env(:tds, :text_encoder, Latin1)
    |> apply(:decode, [binary, from_codepage])
  end

  @doc false
  def use_elixir_calendar_types(value),
    do: Process.put(:use_elixir_calendar_types, value)

  @doc false
  def use_elixir_calendar_types?,
    do: Process.get(:use_elixir_calendar_types, false)

  @doc false
  def default_opts(opts) do
    opts
    |> Keyword.put_new(:idle_timeout, 5000)
    |> Keyword.put_new(:execution_mode, :prepare_execute)
  end
end
