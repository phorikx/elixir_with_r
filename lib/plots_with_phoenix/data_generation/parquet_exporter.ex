defmodule PlotsWithPhoenix.ParquetExporter do
  require Logger

  def export_to_parquet(data_stream, column_order, output_path) do
    tmp_csv = Path.join(System.tmp_dir!(), "dataset_#{:rand.uniform(10000)}.csv")

    try do
      write_csv_stream(data_stream, column_order, tmp_csv)
      convert_to_parquet(tmp_csv, output_path)
    after
      File.rm(tmp_csv)
    end
  end

  defp write_csv_stream(data_stream, column_order, csv_path) do
    file = File.open!(csv_path, [:write])

    header = Enum.join(column_order, ",") <> "\n"
    IO.write(file, header)

    Enum.each(data_stream, fn chunk ->
      chunk_csv =
        Enum.map(chunk, fn row ->
          column_order
          |> Enum.map(fn col -> format_csv_value(row[col]) end)
          |> Enum.join(",")
        end)
        |> Enum.join("\n")

      IO.write(file, chunk_csv <> "\n")
    end)

    File.close(file)
  end

  defp format_csv_value(value) when is_binary(value) do
    "\"#{String.replace(value, "\"", "\"\"")}\""
  end

  defp format_csv_value(value), do: to_string(value)

  defp convert_to_parquet(csv_path, output_path) do
    r_code = """
          library(arrow)
          data <- read.csv("#{csv_path}")
          write_parquet(data, "#{output_path}")
          "export complete"
    """

    case PlotsWithPhoenix.UserRSessions.eval_for_user("system", r_code) do
      {:ok, result} ->
        Logger.warning("Got result from call to R code: " <> result)

        if String.contains?(result, "export complete") do
          {:ok, output_path}
        else
          {:error, {:r_error, result}}
        end

      error ->
        Logger.warning("Call to R code failed: " <> error)
        error
    end
  end
end
