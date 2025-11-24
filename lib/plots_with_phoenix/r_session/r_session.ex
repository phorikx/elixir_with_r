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
    cmd = "R --no-restore --no-save --quiet 2>/dev/null"
    port = Port.open({:spawn, cmd}, [:binary, :exit_status, :hide])

    # Wait for initial R startup and consume all startup messages
    _startup_buffer = wait_for_r_ready(port, "", 5000)

    # Pre-load arrow library with suppressed messages
    Port.command(port, "suppressPackageStartupMessages(library(arrow))\n")

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
    # Only log non-zero exit status (0 is normal termination)
    if status != 0 do
      Logger.error("r session died with status: #{status}")
    end
    if waiting, do: GenServer.reply(waiting, {:error, :session_died})
    {:stop, :r_session_died, state}
  end

  def terminate(_reason, %{port: port}) do
    # Send quit command to R before closing port to avoid SIGPIPE errors
    try do
      Port.command(port, "q(save='no')\n")
      Port.close(port)
    catch
      _, _ -> :ok
    end
    :ok
  end

  defp extract_result(output) do
    # The output ends with "> " (prompt with space)
    # R echoes the command back before showing output, so we need to skip it
    output
    # Remove trailing prompt
    |> String.replace_suffix("> ", "")
    # Split into lines and filter out empty ones
    |> String.split("\n")
    |> Enum.reject(&(&1 == ""))
    # Drop the first line (the echoed command)
    |> Enum.drop(1)
    |> Enum.join("\n")
    |> String.trim()
  end
end
