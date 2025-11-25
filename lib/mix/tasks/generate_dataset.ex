defmodule Mix.Tasks.GenerateDataset do
  @moduledoc """
  Generates synthetic datasets from YAML template files.

  ## Examples

  mix generate_dataset --template priv/templates/unemployment_benefits.yaml --rows 1000000 --output /tmp/export.parquet --reporting_period 20240101
  mix generate_dataset --template priv/templates/unemployment_benefits.yaml --rows 5000 --reporting_period 202401
  mix generate_dataset --iterate --template priv/templates/unemployment_benefits.yaml --input /tmp/dataset.parquet --output /tmp/dataset_next.parquet
  """

  use Mix.Task

  alias PlotsWithPhoenix.{
    DataGenerator,
    ParquetExporter,
    DatasetIterator,
    TemplateParser
  }

  @shortdoc "Generates synthetic dataset"
  @switches [
    template: :string,
    rows: :integer,
    output: :string,
    input: :string,
    reporting_period: :string,
    help: :boolean,
    iterate: :boolean
  ]
  @aliases [
    t: :template,
    r: :rows,
    o: :output,
    r: :input,
    p: :reporting_period,
    h: :help,
    i: :iterate
  ]

  def run(args) do
    Mix.Task.run("app.start")

    {opts, _} = OptionParser.parse!(args, switches: @switches, aliases: @aliases)

    cond do
      opts[:help] ->
        print_help()

      opts[:iterate] ->
        iterate_dataset(opts)

      true ->
        generate_dataset(opts)
    end
  end

  defp iterate_dataset(opts) do
    template_path = opts[:template] || raise_missing_template_error()
    output_path = opts[:output] || "/tmp/dataset_iterated.parquet"
    input_path = opts[:input] || raise "Missing required option: --input <path>"

    unless File.exists?(template_path) do
      IO.puts("❌ Template file not found: #{template_path}")
      System.halt(1)
    end

    unless File.exists?(input_path) do
      IO.puts("❌ Input file not found: #{input_path}")
      System.halt(1)
    end

    {:ok, previous_data} = Explorer.DataFrame.from_parquet(input_path)
    previous_data_correct_format = Explorer.DataFrame.to_rows(previous_data)

    # Extract substitutions from existing data (e.g., reporting_period)
    substitutions = extract_substitutions_from_data(previous_data)

    # Parse templates from YAML with substitutions
    {:ok, dataset_template, iteration_template} =
      TemplateParser.parse_file(template_path, substitutions)

    unless iteration_template do
      IO.puts("❌ Template file must contain an 'iteration' section for iteration mode")
      System.halt(1)
    end

    start_time = System.monotonic_time()

    {:ok, data_set} =
      DatasetIterator.iterate_dataset(
        previous_data_correct_format,
        dataset_template,
        iteration_template
      )

    :ok = ParquetExporter.export_to_parquet(data_set, output_path, :list)

    end_time = System.monotonic_time()
    duration_ms = System.convert_time_unit(end_time - start_time, :native, :millisecond)
    file_size = File.stat!(output_path).size |> format_bytes()

    IO.puts("""
    ✅ Dataset iterated successfully!

      Template: #{template_path}
      Input: #{input_path}
      Output: #{output_path}
      Size: #{file_size}
      Duration: #{duration_ms}ms
    """)
  end

  defp print_help do
    IO.puts(@moduledoc)
  end

  defp generate_dataset(opts) do
    template_path = opts[:template] || raise_missing_template_error()
    n_rows = opts[:rows] || 10_000
    output_path = opts[:output] || "/tmp/dataset_#{n_rows}.parquet"

    unless File.exists?(template_path) do
      IO.puts("❌ Template file not found: #{template_path}")
      System.halt(1)
    end

    # Build substitutions for placeholders
    substitutions = build_substitutions(opts)

    IO.puts("Generating dataset with #{n_rows} rows from template: #{template_path}")

    # Parse template from YAML
    {:ok, dataset_template, _iteration_template} =
      TemplateParser.parse_file(template_path, substitutions)

    unless dataset_template do
      IO.puts("❌ Template file must contain a 'dataset' section for generation mode")
      System.halt(1)
    end

    start_time = System.monotonic_time()
    {:ok, data_stream} = DataGenerator.generate_dataset(dataset_template, n_rows)
    :ok = ParquetExporter.export_to_parquet(data_stream, output_path, :stream)

    end_time = System.monotonic_time()
    duration_ms = System.convert_time_unit(end_time - start_time, :native, :millisecond)
    file_size = File.stat!(output_path).size |> format_bytes()

    IO.puts("""
    ✅ Dataset generated successfully!

      Template: #{template_path}
      Output: #{output_path}
      Size: #{file_size}
      Rows: #{n_rows}
      Duration: #{duration_ms}ms
    """)
  end

  defp raise_missing_template_error do
    IO.puts("""
    ❌ Missing required option: --template <path>

    Example templates can be found in: priv/templates/

    Usage:
      mix generate_dataset --template priv/templates/unemployment_benefits.yaml --rows 1000 --reporting_period 20240101
    """)

    System.halt(1)
  end

  defp build_substitutions(opts) do
    substitutions = %{}

    substitutions =
      if reporting_period = opts[:reporting_period] do
        date = parse_reporting_period(reporting_period)
        Map.put(substitutions, "reporting_period", date)
      else
        Map.put(substitutions, "reporting_period", Date.utc_today())
      end

    substitutions
  end

  defp parse_reporting_period(period) when is_binary(period) do
    if String.length(period) != 8 do
      raise("reporting period should be length 9")
    end

    yyyy = String.slice(period, 0..3)
    mm = String.slice(period, 4..5)
    dd = String.slice(period, 6..7)
    [yyyy, mm, dd] = for i <- [yyyy, mm, dd], do: String.to_integer(i)
    NaiveDateTime.new!(yyyy, mm, dd, 0, 0, 0) |> NaiveDateTime.to_date()
  end

  defp parse_reporting_period(period) when is_nil(period), do: Date.utc_today()

  defp parse_reporting_period(test) do
    IO.puts(test)
    IO.puts("received invalid date as reporting period.")
    System.halt(1)
  end

  defp extract_substitutions_from_data(dataframe) do
    substitutions = %{}

    # Try to extract reporting_period if it exists
    substitutions =
      try do
        reporting_period =
          dataframe
          |> Explorer.DataFrame.pull("reporting_period")
          |> Explorer.Series.to_enum()
          |> Enum.at(0)

        Map.put(substitutions, "reporting_period", reporting_period)
      rescue
        _ -> substitutions
      end

    substitutions
  end

  defp format_bytes(bytes) do
    cond do
      bytes >= 1_000_000 -> "#{Float.round(bytes / 1_000_000, 1)} MB"
      bytes >= 1_000 -> "#{Float.round(bytes / 1_000, 1)} KB"
      true -> "#{bytes} B"
    end
  end
end
