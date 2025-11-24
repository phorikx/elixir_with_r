defmodule PlotsWithPhoenix.RSession do
  use GenServer
  require Logger

  @timeout 30_000
  # R prompt is "> " (with space) at the end of output
  @r_prompt_pattern ~r/> $/
  # R continuation prompt is "+ " (with space) at the end
  @r_continuation_pattern ~r/\+ $/

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  def eval(pid, code, timeout \\ @timeout) do
    GenServer.call(pid, {:eval, code}, timeout)
  end

  def init(_opts) do
    cmd = "R --no-restore --no-save --quiet"
    port = Port.open({:spawn, cmd}, [:binary, :exit_status, :hide])

    # Wait for initial R startup and consume all startup messages
    _startup_buffer = wait_for_r_ready(port, "", 5000)

    # Pre-load arrow library to avoid timeout on first use
    Port.command(port, "library(arrow)\n")

    # Wait for arrow library to load and R to be ready again
    _arrow_buffer = wait_for_r_ready(port, "", 10000)

    {:ok, %{port: port, buffer: "", waiting: nil}}
  end

  defp wait_for_r_ready(port, buffer, timeout) do
    receive do
      {^port, {:data, data}} ->
        new_buffer = buffer <> data
        if Regex.match?(@r_prompt_pattern, new_buffer) do
          new_buffer
        else
          wait_for_r_ready(port, new_buffer, timeout)
        end
    after
      timeout ->
        Logger.warning("R ready timeout, buffer: #{inspect(buffer)}")
        buffer
    end
  end

  def handle_call({:eval, code}, from, %{port: port} = state) do
    sanitized_code = String.replace(code, ~r/]\r\n+$/, "") <> "\n"
    Port.command(port, sanitized_code)
    {:noreply, %{state | waiting: from, buffer: ""}}
  end

  def handle_info({port, {:data, data}}, %{port: port, buffer: buffer, waiting: waiting} = state) do
    new_buffer = buffer <> data

    cond do
      Regex.match?(@r_prompt_pattern, new_buffer) ->
        result = extract_result(new_buffer)
        if waiting, do: GenServer.reply(waiting, {:ok, result})
        {:noreply, %{state | buffer: "", waiting: nil}}

      Regex.match?(@r_continuation_pattern, new_buffer) ->
        if waiting, do: GenServer.reply(waiting, {:error, :incomplete_expression})
        {:noreply, %{state | buffer: "", waiting: nil}}

      true ->
        {:noreply, %{state | buffer: new_buffer}}
    end
  end

  def handle_info({:port, {:exit_status, status}}, %{port: _port, waiting: waiting} = state) do
    Logger.error("r session died with status: #{status}")
    if waiting, do: GenServer.reply(waiting, {:error, :session_died})
    {:stop, :r_session_died, state}
  end

  defp extract_result(output) do
    # The output ends with "> " (prompt with space)
    # Remove the final prompt and extract just the result
    output
    |> String.replace_suffix("> ", "")  # Remove trailing prompt
    |> String.split("\n")               # Split into lines
    |> Enum.reverse()                   # Start from the end
    |> Enum.take_while(fn line ->
      # Take lines that are actual output (not echoed commands)
      line != "" and !String.starts_with?(line, ["tryCatch", "library(", "> ", "+ "])
    end)
    |> Enum.reverse()                   # Restore original order
    |> Enum.join("\n")
    |> String.replace(~r/^\[1\] /, "")  # Remove [1] prefix if present
    |> String.trim()
  end
end
