defmodule PlotsWithPhoenixWeb.RConsoleLive do
  use PlotsWithPhoenixWeb, :live_view

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(:code, "")
      |> assign(:results, [])
      |> assign(:loading, false)
      |> assign(:current_task, nil)

    {:ok, socket}
  end

  @impl true
  def handle_event("update_code", %{"code" => code}, socket) do
    {:noreply, assign(socket, :code, code)}
  end

  @impl true
  def handle_event("execute", %{"code" => code}, socket) do
    if String.trim(code) == "" do
      {:noreply, socket}
    else
      task =
        Task.async(fn ->
          PlotsWithPhoenix.RSessionPool.eval(code)
        end)

      socket =
        socket
        |> assign(:loading, true)
        |> assign(:current_task, task.ref)

      {:noreply, socket}
    end
  end

  @impl true
  def handle_info({ref, result}, %{assigns: %{current_task: ref}} = socket) do
    Process.demonitor(ref, [:flush])

    new_result = %{
      code: socket.assigns.code,
      result: result,
      timestamp: DateTime.utc_now()
    }

    socket =
      socket
      |> assign(:results, [new_result | socket.assigns.results])
      |> assign(:loading, false)
      |> assign(:code, "")
      |> assign(:current_task, nil)

    {:noreply, socket}
  end

  @impl true
  def handle_info(
        {:DOWN, ref, :process, _pid, _reason},
        %{assigns: %{current_task: ref}} = socket
      ) do
    socket =
      socket
      |> assign(:loading, false)
      |> assign(:current_task, nil)
      |> put_flash(:error, "R Execution failed")

    {:noreply, socket}
  end

  @impl true
  def handle_info(_, socket), do: {:noreply, socket}
end
