defmodule PlotsWithPhoenix.ParquetExporterTest do
  use ExUnit.Case, async: true
  alias PlotsWithPhoenix.{DataGenerator, DatasetTemplate, ParquetExporter}

  @tmp_dir System.tmp_dir!()

  setup do
    test_file = Path.join(@tmp_dir, "test_#{System.unique_integer([:positive])}.parquet")
    on_exit(fn -> File.rm(test_file) end)
    {:ok, test_file: test_file}
  end

  describe "export_to_parquet/3 with :stream mode" do
    test "exports stream data to parquet file", %{test_file: test_file} do
      template =
        DatasetTemplate.new("test")
        |> DatasetTemplate.add_variable(:id, {:sequence, 1})
        |> DatasetTemplate.add_variable(:value, {:constant, 42})

      {:ok, stream} = DataGenerator.generate_dataset(template, 100)

      assert :ok = ParquetExporter.export_to_parquet(stream, test_file, :stream)
      assert File.exists?(test_file)
    end

    test "exported file can be read back", %{test_file: test_file} do
      template =
        DatasetTemplate.new("test")
        |> DatasetTemplate.add_variable(:id, {:sequence, 1})
        |> DatasetTemplate.add_variable(:name, {:categorical, ["Alice", "Bob"]})

      {:ok, stream} = DataGenerator.generate_dataset(template, 50)
      ParquetExporter.export_to_parquet(stream, test_file, :stream)

      {:ok, df} = Explorer.DataFrame.from_parquet(test_file)
      assert Explorer.DataFrame.n_rows(df) == 50
      assert Explorer.DataFrame.n_columns(df) == 2
    end

    test "handles large datasets", %{test_file: test_file} do
      template =
        DatasetTemplate.new("test")
        |> DatasetTemplate.add_variable(:id, {:sequence, 1})
        |> DatasetTemplate.add_variable(:value, {:uniform, 0, 100})

      {:ok, stream} = DataGenerator.generate_dataset(template, 25_000)

      assert :ok = ParquetExporter.export_to_parquet(stream, test_file, :stream)

      {:ok, df} = Explorer.DataFrame.from_parquet(test_file)
      assert Explorer.DataFrame.n_rows(df) == 25_000
    end
  end

  describe "export_to_parquet/3 with :list mode" do
    test "exports list data to parquet file", %{test_file: test_file} do
      data = [
        %{id: 1, name: "Alice", age: 30},
        %{id: 2, name: "Bob", age: 25},
        %{id: 3, name: "Charlie", age: 35}
      ]

      assert :ok = ParquetExporter.export_to_parquet(data, test_file, :list)
      assert File.exists?(test_file)
    end

    test "exported list file can be read back", %{test_file: test_file} do
      data = [
        %{x: 1.5, y: 2.5},
        %{x: 3.5, y: 4.5}
      ]

      ParquetExporter.export_to_parquet(data, test_file, :list)

      {:ok, df} = Explorer.DataFrame.from_parquet(test_file)
      assert Explorer.DataFrame.n_rows(df) == 2
      assert Explorer.DataFrame.n_columns(df) == 2
    end

    test "handles empty list", %{test_file: test_file} do
      data = []

      ParquetExporter.export_to_parquet(data, test_file, :list)

      {:ok, df} = Explorer.DataFrame.from_parquet(test_file)
      assert Explorer.DataFrame.n_rows(df) == 0
    end

    test "preserves data types", %{test_file: test_file} do
      data = [
        %{int_col: 42, float_col: 3.14, string_col: "test"}
      ]

      ParquetExporter.export_to_parquet(data, test_file, :list)

      {:ok, df} = Explorer.DataFrame.from_parquet(test_file)
      assert Explorer.DataFrame.n_rows(df) == 1
      assert Explorer.DataFrame.n_columns(df) == 3
    end
  end
end
