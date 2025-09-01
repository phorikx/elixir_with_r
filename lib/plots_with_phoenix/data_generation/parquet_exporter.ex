defmodule PlotsWithPhoenix.ParquetExporter do
  require Logger

  def export_to_parquet(data_stream, column_order, output_path) do
    data_list = Enum.to_list(data_stream) |> List.flatten()
    df = Explorer.DataFrame.new(data_list)
    Explorer.DataFrame.to_parquet(df, output_path)
  end
end
