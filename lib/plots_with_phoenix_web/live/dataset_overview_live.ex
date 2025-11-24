defmodule PlotsWithPhoenixWeb.DatasetOverviewLive do
  use PlotsWithPhoenixWeb, :live_view

  @months ~w(january february march april may june july august september october november december)

  @impl true
  def mount(_params, session, socket) do
    user_session_id = session["user_session_id"] || generate_session_id()

    socket =
      socket
      |> assign(:user_session_id, user_session_id)
      |> assign(:datasets, initialize_datasets())
      |> assign(:loading_count, 0)

    # start async loading for all datasets
    send(self(), :load_all_datasets)

    {:ok, socket}
  end

  @impl true
  def handle_info(:load_all_datasets, socket) do
    datasets_to_load =
      socket.assigns.datasets
      |> Enum.filter(fn {_month, dataset} -> dataset.status == :pending end)

    socket = assign(socket, :loading_count, length(datasets_to_load))

    # start async task for each dataset
    Enum.each(datasets_to_load, fn {month, dataset} ->
      if dataset.file_exists do
        task =
          Task.async(fn ->
            get_dataset_info_from_r(dataset.file_path, socket.assigns.user_session_id)
          end)

        send(self(), {:dataset_task_started, month, task.ref})
      end
    end)

    {:noreply, socket}
  end

  @impl true
  def handle_info({:dataset_task_started, month, task_ref}, socket) do
    datasets = socket.assigns.datasets
    updated_datasets = Map.put(datasets, month, %{datasets[month] | task_ref: task_ref, status: :loading})
    {:noreply, assign(socket, :datasets, updated_datasets)}
  end

  @impl true
  def handle_info({ref, result}, socket) do
    Process.demonitor(ref, [:flush])

    # find which dataset this result belongs to
    {month, _dataset} =
      Enum.find(socket.assigns.datasets, fn {_month, dataset} ->
        dataset.task_ref == ref
      end)

    datasets = socket.assigns.datasets

    updated_datasets =
      case result do
        {:ok, %{rows: rows, columns: columns}} ->
          Map.put(datasets, month, %{
            datasets[month]
            | status: :loaded,
              rows: rows,
              columns: columns,
              task_ref: nil
          })

        {:error, reason} ->
          Map.put(datasets, month, %{
            datasets[month]
            | status: :error,
              error: reason,
              task_ref: nil
          })
      end

    new_loading_count = socket.assigns.loading_count - 1

    {:noreply,
     socket
     |> assign(:datasets, updated_datasets)
     |> assign(:loading_count, new_loading_count)}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, socket) do
    # find failed task and mark as error
    case Enum.find(socket.assigns.datasets, fn {_month, dataset} ->
           dataset.task_ref == ref
         end) do
      nil ->
        # Task ref not found, might have already been handled
        {:noreply, socket}

      {month, _dataset} ->
        datasets = socket.assigns.datasets

        error_msg =
          case reason do
            :normal -> "Task completed normally but no result received"
            {:timeout, _} -> "Timeout loading dataset (R session may be busy)"
            _ -> "Task failed: #{inspect(reason)}"
          end

        updated_datasets =
          Map.put(datasets, month, %{
            datasets[month]
            | status: :error,
              error: error_msg,
              task_ref: nil
          })

        {:noreply,
         socket
         |> assign(:datasets, updated_datasets)
         |> assign(:loading_count, max(0, socket.assigns.loading_count - 1))}
    end
  end

  # Helper functions for template
  defp ordered_datasets(datasets) do
    @months
    |> Enum.map(fn month -> {month, datasets[month]} end)
  end

  defp loaded_dataset_count(datasets) do
    datasets
    |> Map.values()
    |> Enum.count(&(&1.status == :loaded))
  end

  defp total_rows(datasets) do
    datasets
    |> Map.values()
    |> Enum.filter(&(&1.status == :loaded))
    |> Enum.sum_by(& &1.rows)
  end

  defp initialize_datasets do
    @months
    |> Map.new(fn month ->
      file_path = Path.join([File.cwd!(), "fixtures", "benefits_#{month}.parquet"])
      file_exists = File.exists?(file_path)

      dataset = %{
        month: month,
        file_path: file_path,
        file_exists: file_exists,
        status: if(file_exists, do: :pending, else: :missing),
        rows: nil,
        columns: nil,
        error: nil,
        task_ref: nil
      }

      {month, dataset}
    end)
  end

  defp get_dataset_info_from_r(file_path, user_session_id) do
    # escape the file path for r
    escaped_path = String.replace(file_path, "\\", "/")

    # Use semicolons to make this a single-line expression for R
    # arrow library is pre-loaded in RSession init, so no need to load it here
    r_code = "tryCatch({ data <- read_parquet(\"#{escaped_path}\"); rows <- nrow(data); columns <- ncol(data); cat(paste(\"ROWS:\", rows, \"COLUMNS:\", columns)) }, error = function(e) { cat(paste(\"ERROR:\", e$message)) })"

    case PlotsWithPhoenix.UserRSessions.eval_for_user(user_session_id, r_code) do
      {:ok, output} ->
        parse_r_output(output)
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_r_output(output) when is_binary(output) do
    cond do
      String.contains?(output, "ERROR:") ->
        error_msg = String.replace(output, "ERROR:", "") |> String.trim()
        {:error, error_msg}

      String.contains?(output, "ROWS:") ->
        # parse "ROWS: 1000 COLUMNS: 5"
        parts = String.split(output)
        rows_idx = Enum.find_index(parts, &(&1 == "ROWS:"))
        cols_idx = Enum.find_index(parts, &(&1 == "COLUMNS:"))

        if rows_idx && cols_idx do
          rows = Enum.at(parts, rows_idx + 1) |> String.to_integer()
          columns = Enum.at(parts, cols_idx + 1) |> String.to_integer()
          {:ok, %{rows: rows, columns: columns}}
        else
          {:error, "Could not parse R output: #{output}"}
        end

      true ->
        {:error, "Unexpected R output: #{output}"}
    end
  end

  defp generate_session_id do
    :crypto.strong_rand_bytes(16) |> Base.encode64()
  end

  defp format_number(number) when is_integer(number) do
    number
    |> Integer.to_string()
    |> String.reverse()
    |> String.replace(~r/(\d{3})(?=\d)/, "\\1,")
    |> String.reverse()
  end

  defp format_number(_), do: "0"
end
