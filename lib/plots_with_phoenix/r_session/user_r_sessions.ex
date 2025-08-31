defmodule PlotsWithPhoenix.UserRSessions do
  use GenServer

  @cleanup_interval :timer.hours(1)
  @session_timeout :timer.hours(2)

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def eval_for_user(user_id, code) do
    GenServer.call(__MODULE__, {:eval, user_id, code}, 35_000)
  end

  def init(_) do
    Process.send_after(self(), :cleanup, @cleanup_interval)
    {:ok, %{sessions: %{}, last_used: %{}}}
  end

  def handle_call({:eval, user_id, code}, _from, state) do
    {pid, new_state} = ensure_session(user_id, state)
    result = PlotsWithPhoenix.RSession.eval(pid, code)

    updated_state = put_in(new_state.last_used[user_id], System.system_time(:millisecond))
    {:reply, result, updated_state}
  end

  def handle_info(:cleanup, state) do
    now = System.system_time(:millisecond)
    timeout_ms = @session_timeout

    expired_users =
      state.last_used
      |> Enum.filter(fn {_ser_id, last_used} -> now - last_used > timeout_ms end)
      |> Enum.map(fn {user_id, _} -> user_id end)

    Enum.each(expired_users, fn user_id ->
      if pid = state.sessions[user_id] do
        GenServer.stop(pid, :normal)
      end
    end)

    new_sessions = Map.drop(state.sessions, expired_users)
    new_last_used = Map.drop(state.last_used, expired_users)

    Process.send_after(self(), :cleanup, @cleanup_interval)
    {:noreply, %{state | sessions: new_sessions, last_used: new_last_used}}
  end

  defp ensure_session(user_id, state) do
    case state.sessions[user_id] do
      nil ->
        {:ok, pid} = PlotsWithPhoenix.RSession.start_link()
        new_sessions = Map.put(state.sessions, user_id, pid)
        {pid, %{state | sessions: new_sessions}}

      pid when is_pid(pid) ->
        if Process.alive?(pid) do
          {pid, state}
        else
          {:ok, new_pid} = PlotsWithPhoenix.RSession.start_link()
          new_sessions = Map.put(state.sessions, user_id, new_pid)
          {new_pid, %{state | sessions: new_sessions}}
        end
    end
  end
end
