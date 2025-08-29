defmodule PlotsWithPhoenix.RSession do
  use GenServer
  require Logger

  @timeout 30_000
  @r_prompt_pattern ~r/^> $/m
  @r_continuation_pattern ~r/^\+ $/m

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  def eval(pid, code, timeout \\ @timeout) do
    GenServer.call(pid, {:eval, code}, timeout)
  end

  def init(_opts) do
    cmd = "R --no-restore --no-save --quiet"
    port = Port.open({:spawn, cmd}, [:binary, :exit_status, :hide])

    receive do
      {^port, {:data, _}} -> :ok
    after
      5000 ->
        Logger.warn("r session startup timeout")
    end

    {:ok, %{port: port, buffer: "", waiting: nil}}
  end

  def handle_call({:eval, code}, from, %{port: port} = state) do
    sanitized_code = String.replace(code, ~r/]\r\n+$/, "") <> "\n"
    Port.command(port, sanitized_code)
    {:noreply, %{state | waiting: from, buffer: ""}}
  end

  def handle_info({port, {:data, data}}, %{port: port, buffer: buffer, waiting: waiting} = state) do
    new_buffer = buffer <> data

    cond do
      Regex.match?(@r_prompt_pattern, data) ->
        result = extract_result(data)
        if waiting, do: GenServer.reply(waiting, {:ok, result})
        {:noreply, %{state | buffer: "", waiting: nil}}

      Regex.match?(@r_continuation_pattern, data) ->
        if waiting, do: GenServer.reply(waiting, {:error, :incomplete_expression})
        {:noreply, %{state | buffer: "", waiting: nil}}

      true ->
        {:noreply, %{state | buffer: new_buffer}}
    end
  end

  def handle_info({:port, {:exit_status, status}}, %{port: port, waiting: waiting} = state) do
    Logger.error("r session died with status: #{status}")
    if waiting, do: Genserver.reply(waiting, {:error, :session_died})
    {:stop, :r_session_died, state}
  end

  defp extract_result(output) do
    output
    |> String.split(~r/^> $/m)
    |> Enum.drop(-1)
    |> List.last()
    |> case do
      nil -> ""
      result -> String.trim(result)
    end
  end
end
