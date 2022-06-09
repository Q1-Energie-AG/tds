defmodule Tds.Protocol do
  @moduledoc """
  Implements DBConnection behaviour for TDS protocol.
  """
  alias Tds.{Parameter, Query}
  alias Tds.Protocol.{Instance, SessionOptions}
  import Tds.{BinaryUtils, Messages, Utils}
  require Logger

  use DBConnection

  @timeout 5_000
  @required_socket_opts [packet: :raw, mode: :binary, active: false]

  @type sock :: {:gen_tcp | :ssl, :gen_tcp.socket() | :ssl.sslsocket()}
  @type env :: %{
          trans: <<_::8>>,
          savepoint: non_neg_integer,
          collation: Tds.Protocol.Collation.t(),
          packetsize: integer
        }
  @type transaction :: nil | :started | :successful | :failed
  @type state ::
          :ready
          | :prelogin
          | :login
          | :prepare
          | :executing
  @type packet_data :: binary

  @type t :: %__MODULE__{
          sock: nil | sock,
          usock: nil | pid,
          instance_port: nil | term,
          opts: nil | Keyword.t(),
          state: state,
          result: nil | list(),
          query: nil | String.t(),
          transaction: transaction,
          env: env
        }

  defstruct sock: nil,
            usock: nil,
            instance_port: nil,
            opts: nil,
            # Tells if connection is ready or executing command
            state: :ready,
            result: nil,
            query: nil,
            transaction: nil,
            env: %{
              trans: <<0x00>>,
              savepoint: 0,
              collation: %Tds.Protocol.Collation{},
              packetsize: 4096
            }

  @impl DBConnection
  @spec checkout(state :: t) ::
          {:ok, new_state :: any} | {:disconnect, Exception.t(), new_state :: t}
  def checkout(%{transaction: :started} = s) do
    {:disconnect, %Tds.Error{message: "Invalid transactions status `:started`"}, s}
  end

  def checkout(%{sock: {mod, _sock}} = s) do
    case setopts(s.sock, active: false) do
      :ok ->
        {:ok, s}

      {:error, reason} ->
        msg = "Failed to #{inspect(mod)}.setops(active: false) due `#{reason}`"
        {:disconnect, %Tds.Error{message: msg}, s}
    end
  end

  @impl DBConnection
  @spec connect(opts :: Keyword.t()) :: {:ok, state :: t()} | {:error, Exception.t()}
  def connect(opts) do
    opts =
      opts
      |> Keyword.put_new(:username, System.get_env("MSSQLUSER") || System.get_env("USER"))
      |> Keyword.put_new(:password, System.get_env("MSSQLPASSWORD"))
      |> Keyword.put_new(:instance, System.get_env("MSSQLINSTANCE"))
      |> Keyword.put_new(:hostname, System.get_env("MSSQLHOST") || "localhost")
      |> Enum.reject(fn {_k, v} -> is_nil(v) end)

    s = %__MODULE__{}

    case opts[:instance] do
      nil ->
        connect(opts, s)

      _instance ->
        case Instance.get(opts, s) do
          {:ok, s} -> connect(opts, s)
          err -> {:error, err}
        end
    end
  end

  @impl DBConnection
  @spec disconnect(err :: Exception.t() | String.t(), state :: t()) :: :ok
  def disconnect(_err, %{sock: {mod, sock}} = s) do
    # If socket is active we flush any socket messages so the next
    # socket does not get the messages.
    _ = flush(s)
    mod.close(sock)
  end

  @impl DBConnection
  @spec handle_begin(Keyword.t(), t) ::
          {:ok, Tds.Result.t(), new_state :: t}
          | {DBConnection.status(), new_state :: t}
          | {:disconnect, Exception.t(), new_state :: t}
  def handle_begin(opts, %{env: env, transaction: t} = s) do
    isolation_level = Keyword.get(opts, :isolation_level, :read_committed)

    case Keyword.get(opts, :mode, :transaction) do
      :transaction when is_nil(t) ->
        payload = [isolation_level: isolation_level]
        send_transaction("TM_BEGIN_XACT", payload, %{s | transaction: :started})

      :savepoint when t in [:started, :failed] ->
        env = Map.update!(env, :savepoint, &(&1 + 1))

        s = %{s | transaction: :started, env: env}
        send_transaction("TM_SAVE_XACT", [name: env.savepoint], s)

      mode when mode in [:transaction, :savepoint] ->
        handle_status(opts, s)
    end
  end

  @impl DBConnection
  @spec handle_close(Tds.Query.t(), nil | keyword | map, t()) ::
          {:ok, Tds.Result.t(), new_state :: t()}
          | {:error | :disconnect, Exception.t(), new_state :: t()}
  def handle_close(query, opts, s), do: send_close(query, opts[:parameters], s)

  @impl DBConnection
  @spec handle_commit(Keyword.t(), t) ::
          {:ok, Tds.Result.t(), new_state :: t}
          | {DBConnection.status(), new_state :: t}
          | {:disconnect, Exception.t(), new_state :: t}
  def handle_commit(opts, %{transaction: t, env: env} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when t == :started ->
        send_transaction("TM_COMMIT_XACT", [], %{s | transaction: nil})

      :savepoint when t == :started ->
        send_transaction("TM_SAVE_XACT", [name: env.savepoint], s)

      mode when mode in [:transaction, :savepoint] ->
        handle_status(opts, s)
    end
  end

  @impl DBConnection
  @spec handle_deallocate(query :: Query.t(), cursor :: any, opts :: Keyword.t(), state :: t()) ::
          {:ok, Tds.Result.t(), new_state :: t()}
          | {:error | :disconnect, Exception.t(), new_state :: t()}
  def handle_deallocate(_query, _cursor, _opts, state) do
    {:error, Tds.Error.exception("Cursor operations are not supported in TDS"), state}
  end

  @impl DBConnection
  @spec handle_declare(Query.t(), params :: any, opts :: Keyword.t(), state :: t) ::
          {:ok, Query.t(), cursor :: any, new_state :: t}
          | {:error | :disconnect, Exception.t(), new_state :: t}
  def handle_declare(_query, _params, _opts, state) do
    {:error, Tds.Error.exception("Cursor operations are not supported in TDS"), state}
  end

  @impl DBConnection
  @spec handle_execute(Tds.Query.t(), DBConnection.params(), Keyword.t(), t) ::
          {:ok, Tds.Query.t(), Tds.Result.t(), new_state :: t}
          | {:error | :disconnect, Exception.t(), new_state :: t}
  def handle_execute(
        %Query{handle: handle, statement: statement} = query,
        params,
        opts,
        %{sock: _sock} = s
      ) do
    params = opts[:parameters] || params
    Process.put(:resultset, Keyword.get(opts, :resultset, false))

    try do
      if params != [] do
        send_param_query(query, params, s)
      else
        send_query(statement, s)
      end
      |> case do
        {:ok, result, state} ->
          {:ok, query, result, state}

        other ->
          other
      end
    rescue
      exception ->
        {:error, exception, s}
    after
      Process.delete(:resultset)

      unless is_nil(handle) do
        handle_close(query, opts, %{s | state: :executing})
      end
    end
  end

  @impl DBConnection
  @spec ping(t) :: {:ok, t} | {:disconnect, Exception.t(), t}
  def ping(state) do
    case send_query(~s(SELECT 'pong' as [msg]), state) do
      {:ok, _, s} ->
        {:ok, s}

      {:disconnect, :closed, s} ->
        {:disconnect, %Tds.Error{message: "Connection closed."}, s}

      {:error, err, s} ->
        err =
          if Exception.exception?(err) do
            err
          else
            Tds.Error.exception(inspect(err))
          end

        {:disconnect, err, s}

      other ->
        {:disconnect, Tds.Error.exception(inspect(other)), state}
    end
  end

  @spec checkin(state :: t) ::
          {:ok, new_state :: t} | {:disconnect, Exception.t(), new_state :: t}
  def checkin(%{transaction: :started} = s) do
    err = %Tds.Error{message: "Unexpected transaction status `:started`"}
    {:disconnect, err, s}
  end

  def checkin(%{sock: {mod, _sock}} = s) do
    sock_mod = inspect(mod)

    case setopts(s.sock, active: :once) do
      :ok ->
        {:ok, s}

      {:error, reason} ->
        msg = "Failed to #{sock_mod}.setops(active: false) due `#{reason}`"
        {:disconnect, %Tds.Error{message: msg}, s}
    end
  end

  @impl DBConnection
  @spec handle_prepare(Tds.Query.t(), Keyword.t(), t) ::
          {:ok, Tds.Query.t(), new_state :: t()}
          | {:error | :disconnect, Exception.t(), new_state :: t}
  def handle_prepare(%{statement: statement} = query, opts, s) do
    case Keyword.get(opts, :execution_mode, :prepare_execute) do
      :prepare_execute ->
        params =
          opts[:parameters]
          |> Parameter.prepared_params()

        send_prepare(statement, params, %{s | state: :prepare})

      :executesql ->
        {:ok, query, %{s | state: :executing}}

      execution_mode ->
        message =
          "Unknown execution mode #{inspect(execution_mode)}, please check your config." <>
            "Supported modes are :prepare_execute and :executesql"

        {:error, %Tds.Error{message: message}, s}
    end
  end

  @spec handle_rollback(Keyword.t(), t) ::
          {:ok, Tds.Result.t(), new_state :: t}
          | {:idle, new_state :: t}
          | {:disconnect, Exception.t(), new_state :: t}
  @impl DBConnection
  def handle_rollback(opts, %{env: env, transaction: transaction} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when transaction in [:started, :failed] ->
        env = %{env | savepoint: 0}
        s = %{s | transaction: nil, env: env}
        payload = [name: 0, isolation_level: :read_committed]
        send_transaction("TM_ROLLBACK_XACT", payload, s)

      :savepoint when transaction in [:started, :failed] ->
        payload = [name: env.savepoint]

        send_transaction("TM_ROLLBACK_XACT", payload, %{
          s
          | transaction: :started
        })

      mode when mode in [:transaction, :savepoint] ->
        handle_status(opts, s)
    end
  end

  @spec handle_status(Keyword.t(), t) ::
          {:idle | :transaction | :error, t}
          | {:disconnect, Exception.t(), t}
  @impl DBConnection
  def handle_status(_, %{transaction: transaction} = state) do
    case transaction do
      nil -> {:idle, state}
      :successful -> {:idle, state}
      :started -> {:transaction, state}
      :failed -> {:error, state}
    end
  end

  @spec handle_fetch(
          Query.t(),
          cursor :: any(),
          opts :: Keyword.t(),
          state :: t()
        ) ::
          {:cont | :halt, Tds.Result.t(), new_state :: t()}
          | {:error | :disconnect, Exception.t(), new_state :: t()}
  @impl DBConnection
  def handle_fetch(_query, _cursor, _opts, state) do
    {:error, Tds.Error.exception("Cursor is not supported by TDS"), state}
  end

  # CONNECTION

  defp connect(opts, s) do
    host = opts |> Keyword.fetch!(:hostname) |> to_charlist()

    port = s.instance_port || opts[:port] || System.get_env("MSSQLPORT") || 1433
    {port, _} = if is_binary(port), do: Integer.parse(port), else: {port, nil}

    timeout = opts[:timeout] || @timeout

    sock_opts = Keyword.merge(opts[:socket_options] || [], @required_socket_opts)

    s = %{s | opts: opts}

    # Initalize TCP connection with the SQL Server
    with {:ok, sock} <- :gen_tcp.connect(host, port, sock_opts, timeout),
         {:ok, buffers} <- :inet.getopts(sock, [:sndbuf, :recbuf, :buffer]),
         :ok <- :inet.setopts(sock, buffer: max_buf_size(buffers)) do
      # Send Prelogin message to SQL Server
      case send_prelogin(%{s | sock: {:gen_tcp, sock}}) do
        {:error, error, _state} ->
          :gen_tcp.close(sock)
          {:error, error}

        other ->
          other
      end
    else
      {:error, error} ->
        {:error, %Tds.Error{message: "tcp connect: #{error}"}}
    end
  end

  defp ssl_connect(%{sock: {:gen_tcp, sock}, opts: opts} = s) do
    {:ok, _} = Application.ensure_all_started(:ssl)

    case Tds.Tls.connect(sock, opts[:ssl_opts] || []) do
      {:ok, ssl_sock} ->
        state = %{s | sock: {:ssl, ssl_sock}}
        {:ok, state}

      {:error, reason} ->
        error =
          Tds.Error.exception(
            "Unable to establish secure connection to server due #{inspect(reason)}"
          )

        :gen_tcp.close(sock)
        {:error, error, s}
    end
  end

  def handle_info({:udp_error, _, :econnreset}, s) do
    msg =
      "Tds encountered an error while connecting to the Sql Server " <>
        "Browser: econnreset"

    {:stop, Tds.Error.exception(msg), s}
  end

  def handle_info(
        {:tcp, _, _data},
        %{sock: {mod, sock}, opts: opts, state: :prelogin} = s
      ) do
    setopts(s.sock, active: false)

    login(%{s | opts: opts, sock: {mod, sock}})
  end

  def handle_info({tag, _}, s) when tag in [:tcp_closed, :ssl_closed] do
    {:stop, Tds.Error.exception("tcp closed"), s}
  end

  def handle_info({tag, _, reason}, s) when tag in [:tcp_error, :ssl_error] do
    {:stop, Tds.Error.exception("tcp error: #{reason}"), s}
  end

  def handle_info(msg, s) do
    Logger.error(fn ->
      "Unhandled message! \n\n" <>
        "  Tds.Protocol.hand_info/2 \n\n" <>
        "    Arg #1 \n" <>
        inspect(msg) <>
        "    Arg #2 \n" <>
        inspect(s)
    end)

    {:ok, s}
  end

  defp decode(packet_data, %{state: state} = s) do
    {msg, s} = parse(state, packet_data, s)

    case message(state, msg, s) do
      {:ok, s} ->
        # message processed, reset header and msg buffer, then process
        # tail
        {:ok, s}

      {:ok, _result, s} ->
        # send_query returns a result
        {:ok, s}

      {:error, _, _} = err ->
        err
    end
  end

  defp flush(%{sock: sock} = s) do
    receive do
      {:tcp, ^sock, data} ->
        _ = decode(data, s)
        {:ok, s}

      {:tcp_closed, ^sock} ->
        {:disconnect, %Tds.Error{message: "tcp closed"}, s}

      {:tcp_error, ^sock, reason} ->
        {:disconnect, %Tds.Error{message: "tcp error: #{reason}"}, s}
    after
      0 ->
        # There might not be any socket messages.
        {:ok, s}
    end
  end

  # PROTOCOL

  def send_prelogin(%{opts: opts} = s) do
    msg = msg_prelogin(params: opts)

    case msg_send(msg, %{s | state: :prelogin}) do
      {:ok, s} -> login(s)
      any -> any
    end
  end

  def login(%{opts: opts} = s) do
    msg = msg_login(params: opts)

    case login_send(msg, %{s | state: :login}) do
      {:ok, s} ->
        {:ok, %{s | state: :ready}}

      err ->
        err
    end
  end

  defp send_query(statement, s) do
    msg = msg_sql(query: statement)

    case msg_send(msg, %{s | state: :executing}) do
      {:ok, %{result: result} = s} ->
        {:ok, result, s}

      {:error, err, %{transaction: :started} = s} ->
        {:error, err, %{s | transaction: :failed}}

      err ->
        err
    end
  end

  def send_prepare(statement, params, s) do
    params = [
      %Tds.Parameter{
        name: "@handle",
        type: :integer,
        direction: :output,
        value: nil
      },
      %Tds.Parameter{name: "@params", type: :string, value: params},
      %Tds.Parameter{name: "@stmt", type: :string, value: statement}
    ]

    msg = msg_rpc(proc: :sp_prepare, query: statement, params: params)

    case msg_send(msg, %{s | state: :prepare}) do
      {:ok, %{query: query} = s} ->
        {:ok, %{query | statement: statement}, %{s | state: :executing}}

      {:error, err, %{transaction: :started} = s} ->
        {:error, err, %{s | transaction: :failed}}

      err ->
        err
    end
  end

  def send_transaction(command, payload, s) do
    msg =
      msg_transmgr(
        command: command,
        name: Keyword.get(payload, :name),
        isolation_level: Keyword.get(payload, :isolation_level)
      )

    case msg_send(msg, %{s | state: :transaction_manager}) do
      {:ok, %{result: result} = s} ->
        {:ok, result, s}

      {:error, err} ->
        {:disconnect, err, s}

      {:error, err, s} ->
        {:disconnect, err, s}
    end
  end

  @spec send_param_query(Tds.Query.t(), list(), t) ::
          {:error, any()}
          | {:ok, %{optional(:result) => none()}}
          | {:disconnect, any(), %{env: any(), sock: {any(), any()}}}
          | {:error, Tds.Error.t(), %{pak_header: <<>>, tail: <<>>}}
          | {:ok, any(), %{result: any(), state: :ready}}
  defp send_param_query(%Query{handle: handle, statement: statement}, params, s) do
    msg =
      if is_nil(handle) do
        statement_msg(statement, params)
      else
        handle_msg(handle, params)
      end

    case msg_send(msg, s) do
      {:ok, %{result: result} = s} ->
        {:ok, result, %{s | state: :ready}}

      {:error, err, %{transaction: :started} = s} ->
        {:error, err, %{s | transaction: :failed}}

      err ->
        err
    end
  end

  def send_close(%Query{handle: handle} = _query, _params, s) do
    params = [
      %Tds.Parameter{
        name: "@handle",
        type: :integer,
        direction: :input,
        value: handle
      }
    ]

    msg = msg_rpc(proc: :sp_unprepare, params: params)

    case msg_send(msg, s) do
      {:ok, %{result: result} = s} ->
        {:ok, result, %{s | state: :ready}}

      {:error, err, %{transaction: :started} = s} ->
        {:error, err, %{s | transaction: :failed}}

      err ->
        err
    end
  end

  def message(:prelogin, msg_preloginack(response: response), _) do
    case response do
      {:login, s} -> {:ok, s}
      {:encrypt, s} -> ssl_connect(s)
      other -> other
    end
  end

  def message(
        :login,
        msg_loginack(redirect: %{hostname: host, port: port}),
        %{opts: opts}
      ) do
    opts
    |> Keyword.put(:hostname, host)
    |> Keyword.put(:port, port)
    |> connect()
  end

  def message(:login, msg_loginack(), %{opts: opts} = s) do
    opts
    |> SessionOptions.new()
    |> IO.iodata_to_binary()
    |> send_query(%{s | opts: clean_opts(opts)})
  end

  def message(:executing, msg_result(set: set), s) do
    result =
      if Process.get(:resultset, false) do
        set
      else
        List.first(set) || %Tds.Result{rows: nil}
      end

    {:ok, mark_ready(%{s | result: result})}
  end

  def message(:transaction_manager, msg_trans(), s) do
    result = %Tds.Result{columns: [], rows: [], num_rows: 0}

    {:ok, %{s | state: :ready, result: result}}
  end

  def message(:prepare, msg_prepared(params: params), %{} = s) do
    handle =
      params
      |> Enum.find(%{}, &(&1.name == "@handle" and &1.direction == :output))
      |> Map.get(:value)

    result = %Tds.Result{columns: [], rows: [], num_rows: 0}
    query = %Tds.Query{handle: handle}

    {:ok, mark_ready(%{s | result: result, query: query})}
  end

  ## Error
  def message(_, msg_error(error: e), %{} = s) do
    {:error, %Tds.Error{mssql: e}, mark_ready(s)}
  end

  ## ATTN Ack
  def message(:attn, _, %{} = s) do
    result = %Tds.Result{columns: [], rows: [], num_rows: 0}

    {:ok, %{s | statement: "", state: :ready, result: result}}
  end

  defp statement_msg(statement, params) do
    p = [
      %Parameter{
        name: "@statement",
        type: :string,
        direction: :input,
        value: statement
      },
      %Parameter{
        name: "@params",
        type: :string,
        direction: :input,
        value: Parameter.prepared_params(params)
      }
      | Parameter.prepare_params(params)
    ]

    msg_rpc(proc: :sp_executesql, params: p)
  end

  defp handle_msg(handle, params) do
    p = [
      %Parameter{
        name: "@handle",
        type: :integer,
        direction: :input,
        value: handle
      }
      | Parameter.prepare_params(params)
    ]

    msg_rpc(proc: :sp_execute, params: p)
  end

  defp mark_ready(%{state: _} = s) do
    %{s | state: :ready}
  end

  # Send Command To Sql Server
  defp login_send(msg, %{sock: {mod, sock}, env: env, opts: opts} = s) do
    packets = encode_msg(msg, env)
    s = %{s | opts: clean_opts(opts)}

    Enum.each(packets, fn packet ->
      mod.send(sock, packet)
    end)

    case msg_recv(s) do
      {:disconnect, ex, s} ->
        {:disconnect, ex, s}

      buffer ->
        buffer
        |> IO.iodata_to_binary()
        |> decode(%{s | state: :login})
    end
  end

  defp msg_send(
         msg,
         %{sock: {mod, port}, env: env, opts: opts} = s
       ) do
    setopts(s.sock, active: false)

    opts
    |> Keyword.get(:use_elixir_calendar_types, false)
    |> use_elixir_calendar_types()

    send_result =
      msg
      |> encode_msg(env)
      |> Enum.reduce_while(:ok, fn chunk, _ ->
        case mod.send(port, chunk) do
          {:error, reason} -> {:halt, {:error, reason}}
          :ok -> {:cont, :ok}
        end
      end)

    with :ok <- send_result,
         buffer when is_list(buffer) <- msg_recv(s) do
      buffer
      |> IO.iodata_to_binary()
      |> decode(s)
    else
      {:disconnect, _ex, _s} = res -> {0, res}
      other -> other
    end
  end

  defp msg_recv(%{sock: {mod, pid}} = s) do
    case mod.recv(pid, 0) do
      {:ok, pkg} ->
        pkg
        |> next_tds_pkg([])
        |> msg_recv(s)

      {:error, error} ->
        {:disconnect,
         %Tds.Error{
           message: "Connection failed to receive packet due #{inspect(error)}"
         }, s}
    end
  catch
    {:error, error} -> {:disconnect, error, s}
  end

  defp msg_recv({:done, buffer, _}, _s) do
    Enum.reverse(buffer)
  end

  defp msg_recv({:more, buffer, more, last?}, %{sock: {mod, pid}} = s) do
    take = if last?, do: more, else: 0

    case mod.recv(pid, take) do
      {:ok, pkg} ->
        next_tds_pkg(pkg, buffer, more, last?)
        |> msg_recv(s)

      {:error, error} ->
        throw({:error, error})
    end
  end

  defp msg_recv({:more, buffer, unknown_pkg}, %{sock: {mod, pid}} = s) do
    case mod.recv(pid, 0) do
      {:ok, pkg} ->
        unknown_pkg
        |> Kernel.<>(pkg)
        |> next_tds_pkg(buffer)
        |> msg_recv(s)

      {:error, error} ->
        throw({:error, error})
    end
  end

  defp next_tds_pkg(pkg, buffer) do
    case pkg do
      <<0x04, 0x01, size::int16(), _::int32(), chunk::binary>> ->
        more = size - 8
        next_tds_pkg(chunk, buffer, more, true)

      <<0x04, 0x00, size::int16(), _::int32(), chunk::binary>> ->
        more = size - 8
        next_tds_pkg(chunk, buffer, more, false)

      unknown_pkg ->
        {:more, buffer, unknown_pkg}
    end
  end

  defp next_tds_pkg(pkg, buffer, more, true) do
    case pkg do
      <<chunk::binary(more, 8), tail::binary>> ->
        {:done, [chunk | buffer], tail}

      <<chunk::binary>> ->
        more = more - byte_size(chunk)
        {:more, [chunk | buffer], more, true}
    end
  end

  defp next_tds_pkg(pkg, buffer, more, false) do
    case pkg do
      <<chunk::binary(more, 8), tail::binary>> ->
        next_tds_pkg(tail, [chunk | buffer])

      <<chunk::binary>> ->
        more = more - byte_size(chunk)
        {:more, [chunk | buffer], more, false}
    end
  end

  defp clean_opts(opts) do
    Keyword.replace(opts, :password, :REDACTED)
  end

  defp setopts({mod, sock}, options) do
    case mod do
      :gen_tcp -> :inet.setopts(sock, options)
      :ssl -> :ssl.setopts(sock, options)
    end
  end

  defp max_buf_size(buffers) when is_list(buffers) do
    buffers
    |> Keyword.values()
    |> Enum.max()
  end
end
