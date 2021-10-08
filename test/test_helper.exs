Application.ensure_all_started(:tzdata)

defmodule Tds.TestHelper do
  alias Tds.Connection

  require Logger

  defmacro query(statement, params \\ [], opts \\ []) do
    quote do
      case Tds.query(
             var!(context)[:pid],
             unquote(statement),
             unquote(params),
             unquote(opts)
           ) do
        {:ok, %Tds.Result{rows: nil}} -> :ok
        {:ok, %Tds.Result{rows: []}} -> :ok
        {:ok, %Tds.Result{rows: rows}} -> rows
        {:error, %Tds.Error{} = err} -> err
      end
    end
  end

  defmacro prepare(stat, opts \\ []) do
    quote do
      case Tds.prepare(var!(context)[:pid], unquote(stat), unquote(opts)) do
        {:ok, %Tds.Query{} = query} -> query
        {:error, err} -> err
      end
    end
  end

  defmacro execute(query, params, opts \\ []) do
    quote do
      case Tds.execute(var!(context)[:pid], unquote(query), unquote(params), unquote(opts)) do
        {:ok, %Tds.Query{}, %Tds.Result{rows: nil}} -> :ok
        {:ok, %Tds.Query{}, %Tds.Result{rows: rows}} -> rows
        {:error, err} -> err
      end
    end
  end

  defmacro query_multi(statement, params \\ [], opts \\ []) do
    quote do
      case Tds.query_multi(
             var!(context)[:pid],
             unquote(statement),
             unquote(params),
             unquote(opts)
           ) do
        {:ok, resultset} -> resultset
        {:error, %Tds.Error{} = err} -> err
      end
    end
  end

  defmacro proc(proc, params, opts \\ []) do
    quote do
      case Connection.proc(
             var!(context)[:pid],
             unquote(proc),
             unquote(params),
             unquote(opts)
           ) do
        {:ok, %Tds.Result{rows: nil}} -> :ok
        {:ok, %Tds.Result{rows: []}} -> :ok
        {:ok, %Tds.Result{rows: rows}} -> rows
        {:error, %Tds.Error{} = err} -> err
      end
    end
  end

  defmacro transaction(fun, opts \\ []) do
    quote do
      Tds.transaction(
        var!(context)[:pid],
        unquote(fun),
        unquote(opts)
      )
    end
  end

  defmacro drop_table(table) do
    quote bind_quoted: [table: table] do
      statement =
        "if exists(select * from sys.tables where [name] = '#{table}')" <>
          " drop table #{table}"

      Tds.query!(var!(context)[:pid], statement, [])
    end
  end

  defmacro close(query, opts \\ []) do
    quote do
      case Tds.close(var!(context)[:pid], unquote(query), unquote(opts)) do
        :ok -> :ok
        {:error, err} -> err
      end
    end
  end

  def sqlcmd(params, sql, args \\ []) do
    args = [
      "-U",
      params[:username],
      "-P",
      params[:password],
      "-S",
      params[:hostname],
      "-Q",
      ~s(#{sql}) | args
    ]

    System.cmd("sqlcmd", args)
  end
end

opts = Application.get_env(:tds, :opts)
database = opts[:database]

{"", 0} =
  Tds.TestHelper.sqlcmd(opts, """
  IF EXISTS(SELECT * FROM sys.databases where name = '#{database}')
  BEGIN
    DROP DATABASE [#{database}];
  END;
  CREATE DATABASE [#{database}];
  """)

{"Changed database context to 'test'." <> _, 0} =
  Tds.TestHelper.sqlcmd(opts, """
  USE [test];

  CREATE TABLE altering ([a] int)

  CREATE TABLE [composite1] ([a] int, [b] text);
  CREATE TABLE [composite2] ([a] int, [b] int, [c] int);
  CREATE TABLE [uniques] ([id] int NOT NULL, CONSTRAINT UIX_uniques_id UNIQUE([id]))
  """)

{"Changed database context to 'test'." <> _, 0} =
  Tds.TestHelper.sqlcmd(opts, """
  USE test
  GO
  CREATE SCHEMA test;
  """)

# :dbg.start()
# :dbg.tracer()
# :dbg.p(:all,:c)
# :dbg.tpl(Tds, :query, :x)

ExUnit.start()
ExUnit.configure(exclude: [:manual])
