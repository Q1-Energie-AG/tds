defmodule Tds.Protocol.Instance do
  @moduledoc false

  @spec get(Keyword.t(), Tds.Protocol.t()) :: {:ok, Tds.Protocol.t()} | {:error, any()}
  def get(opts, s) do
    host =
      opts
      |> Keyword.fetch!(:hostname)
      |> to_charlist()

    case :gen_udp.open(0, [:binary, {:active, false}, {:reuseaddr, true}]) do
      {:ok, sock} ->
        :gen_udp.send(sock, host, 1434, <<3>>)
        {:ok, msg} = :gen_udp.recv(sock, 0)
        parse_udp(msg, %{s | opts: opts, usock: sock})

      {:error, error} ->
        {:error, %Tds.Error{message: "udp connect: #{error}"}}
    end
  end

  defp parse_udp(
         {_, 1434, <<_head::binary-3, data::binary>>},
         %{opts: opts, usock: sock} = s
       ) do
    :gen_udp.close(sock)

    server =
      data
      |> String.split(";;")
      |> Enum.slice(0..-2)
      |> Enum.reduce([], fn str, acc ->
        server =
          str
          |> String.split(";")
          |> Enum.chunk_every(2)
          |> Enum.reduce([], fn [k, v], acc ->
            k =
              k
              |> String.downcase()
              |> String.to_atom()

            Keyword.put_new(acc, k, v)
          end)

        [server | acc]
      end)
      |> Enum.find(fn s ->
        String.downcase(s[:instancename]) == String.downcase(opts[:instance])
      end)

    case server do
      nil ->
        {:error, %Tds.Error{message: "Instance #{opts[:instance]} not found"}}

      serv ->
        {port, _} = Integer.parse(serv[:tcp])
        {:ok, %{s | opts: opts, itcp: port, usock: nil}}
    end
  end
end
