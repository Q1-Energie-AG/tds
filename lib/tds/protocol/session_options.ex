defmodule Tds.Protocol.SessionOptions do
  @trans_levels [
    :read_uncommitted,
    :read_committed,
    :repeatable_read,
    :snapshot,
    :serializable
  ]

  @default_opts [
    "SET ANSI_NULLS ON; ",
    "SET QUOTED_IDENTIFIER ON; ",
    "SET CURSOR_CLOSE_ON_COMMIT OFF; ",
    "SET ANSI_NULL_DFLT_ON ON; ",
    "SET ANSI_PADDING ON; ",
    "SET ANSI_WARNINGS ON; ",
    "SET CONCAT_NULL_YIELDS_NULL ON; ",
    "SET TEXTSIZE 2147483647; "
  ]

  @spec new(Keyword.t()) :: list() | no_return
  def new(opts) do
    @default_opts
    |> append_opts(opts, :set_language)
    |> append_opts(opts, :set_datefirst)
    |> append_opts(opts, :set_dateformat)
    |> append_opts(opts, :set_deadlock_priority)
    |> append_opts(opts, :set_lock_timeout)
    |> append_opts(opts, :set_remote_proc_transactions)
    |> append_opts(opts, :set_implicit_transactions)
    |> append_opts(opts, :set_transaction_isolation_level)
    |> append_opts(opts, :set_allow_snapshot_isolation)
  end

  defp append_opts(conn, opts, :set_language) do
    case Keyword.get(opts, :set_language) do
      nil -> conn
      val -> conn ++ ["SET LANGUAGE #{val}; "]
    end
  end

  defp append_opts(conn, opts, :set_datefirst) do
    case Keyword.get(opts, :set_datefirst) do
      nil ->
        conn

      val when val in 1..7 ->
        conn ++ ["SET DATEFIRST #{val}; "]

      val ->
        raise(
          ArgumentError,
          "set_datefirst: #{inspect(val)} is out of bounds, valid range is 1..7"
        )
    end
  end

  defp append_opts(conn, opts, :set_dateformat) do
    case Keyword.get(opts, :set_dateformat) do
      nil ->
        conn

      val when val in [:mdy, :dmy, :ymd, :ydm, :myd, :dym] ->
        conn ++ ["SET DATEFORMAT #{val}; "]

      val ->
        raise(
          ArgumentError,
          "set_dateformat: #{inspect(val)} is an invalid value, " <>
            "valid values are [:mdy, :dmy, :ymd, :ydm, :myd, :dym]"
        )
    end
  end

  defp append_opts(conn, opts, :set_deadlock_priority) do
    case Keyword.get(opts, :set_deadlock_priority) do
      nil ->
        conn

      val when val in [:low, :high, :normal] ->
        conn ++ ["SET DEADLOCK_PRIORITY #{val}; "]

      val when val in -10..10 ->
        conn ++ ["SET DEADLOCK_PRIORITY #{val}; "]

      val ->
        raise(
          ArgumentError,
          "set_deadlock_priority: #{inspect(val)} is an invalid value, " <>
            "valid values are #{inspect([:low, :high, :normal] ++ [-10..10])}"
        )
    end
  end

  defp append_opts(conn, opts, :set_lock_timeout) do
    case Keyword.get(opts, :set_lock_timeout) do
      nil ->
        conn

      val when val > 0 ->
        conn ++ ["SET LOCK_TIMEOUT #{val}; "]

      val ->
        raise(
          ArgumentError,
          "set_lock_timeout: #{inspect(val)} is an invalid value, " <>
            "must be an positive integer."
        )
    end
  end

  defp append_opts(conn, opts, :set_remote_proc_transactions) do
    case Keyword.get(opts, :set_remote_proc_transactions) do
      nil ->
        conn

      val when val in [:on, :off] ->
        val = val |> Atom.to_string() |> String.upcase()
        conn ++ ["SET REMOTE_PROC_TRANSACTIONS #{val}; "]

      val ->
        raise(
          ArgumentError,
          "set_remote_proc_transactions: #{inspect(val)} is an invalid value, " <>
            "should be either :on, :off, nil"
        )
    end
  end

  defp append_opts(conn, opts, :set_implicit_transactions) do
    case Keyword.get(opts, :set_implicit_transactions) do
      nil ->
        conn ++ ["SET IMPLICIT_TRANSACTIONS OFF; "]

      val when val in [:on, :off] ->
        val = val |> Atom.to_string() |> String.upcase()
        conn ++ ["SET IMPLICIT_TRANSACTIONS #{val}; "]

      val ->
        raise(
          ArgumentError,
          "set_implicit_transactions: #{inspect(val)} is an invalid value, " <>
            "should be either :on, :off, nil"
        )
    end
  end

  defp append_opts(conn, opts, :set_transaction_isolation_level) do
    case Keyword.get(opts, :set_transaction_isolation_level) do
      nil ->
        conn

      val when val in @trans_levels ->
        t =
          val
          |> Atom.to_string()
          |> String.replace("_", " ")
          |> String.upcase()

        conn ++ ["SET TRANSACTION ISOLATION LEVEL #{t}; "]

      val ->
        raise(
          ArgumentError,
          "set_transaction_isolation_level: #{inspect(val)} is an invalid value, " <>
            "should be one of #{inspect(@trans_levels)} or nil"
        )
    end
  end

  defp append_opts(conn, opts, :set_allow_snapshot_isolation) do
    database = Keyword.get(opts, :database)

    case Keyword.get(opts, :set_allow_snapshot_isolation) do
      nil ->
        conn

      val when val in [:on, :off] ->
        val = val |> Atom.to_string() |> String.upcase()

        conn ++
          ["ALTER DATABASE [#{database}] SET ALLOW_SNAPSHOT_ISOLATION #{val}; "]

      val ->
        raise(
          ArgumentError,
          "set_allow_snapshot_isolation: #{inspect(val)} is an invalid value, " <>
            "should be either :on, :off, nil"
        )
    end
  end
end
