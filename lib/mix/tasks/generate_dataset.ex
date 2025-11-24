defmodule Mix.Tasks.GenerateDataset do
  @moduledoc """
  Generates synthetic datasets for development and testing. 

  ## Examples 

  mix generate_dataset --rows 1000000 --output /tmp/export.parquet
  mix generate_dataset --template customers --rows 5000 --reporting_period 202401
  """

  use Mix.Task

  alias PlotsWithPhoenix.{
    DatasetTemplate,
    DataGenerator,
    ParquetExporter,
    DatasetIterator,
    IterationTemplate
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
    template_name = opts[:template] || "transactions"
    output_path = opts[:output] || "/tmp/dataset_#{template_name}.parquet"
    input_path = opts[:input] || "/tmp/dataset_#{template_name}.parquet"

    {:ok, previous_data} = Explorer.DataFrame.from_parquet(input_path)
    previous_data_correct_format = Explorer.DataFrame.to_rows(previous_data)

    reporting_period_dt =
      previous_data
      |> Explorer.DataFrame.pull("reporting_period")
      |> Explorer.Series.to_enum()
      |> Enum.at(0)

    next_reporting_period = transform_reporting_period(reporting_period_dt)

    reporting_period =
      "#{next_reporting_period.year}#{next_reporting_period.month |> left_pad()}#{reporting_period_dt.day |> left_pad()}"

    template = build_template(template_name, reporting_period)

    iteration_template =
      build_iteration_template(template_name)

    start_time = System.monotonic_time()

    {:ok, data_set} =
      DatasetIterator.iterate_dataset(previous_data_correct_format, template, iteration_template)

    :ok = ParquetExporter.export_to_parquet(data_set, output_path, :list)

    end_time = System.monotonic_time()
    duration_ms = System.convert_time_unit(end_time - start_time, :native, :millisecond)
    file_size = File.stat!(output_path).size |> format_bytes()

    IO.puts("""
    ✅ Dataset iterated on successfully!

      File: #{output_path}
      Size: #{file_size}
      Duration: #{duration_ms}ms 
      Template: #{template_name}
    """)
  end

  defp print_help do
    IO.puts(@moduledoc)
  end

  defp generate_dataset(opts) do
    template_name = opts[:template] || "transactions"
    n_rows = opts[:rows] || 10_000
    output_path = opts[:output] || "/tmp/dataset_#{template_name}_#{n_rows}.parquet"
    reporting_period = opts[:reporting_period]

    IO.puts("Generating #{template_name} dataset with #{n_rows} rows")
    template = build_template(template_name, reporting_period)

    start_time = System.monotonic_time()
    {:ok, data_stream} = DataGenerator.generate_dataset(template, n_rows)
    :ok = ParquetExporter.export_to_parquet(data_stream, output_path, :stream)

    end_time = System.monotonic_time()
    duration_ms = System.convert_time_unit(end_time - start_time, :native, :millisecond)
    file_size = File.stat!(output_path).size |> format_bytes()

    IO.puts("""
    ✅ Dataset generated successfully!

      File: #{output_path}
      Size: #{file_size}
      Rows: #{n_rows}
      Duration: #{duration_ms}ms 
      Template: #{template_name}
    """)
  end

  defp build_iteration_template("unemployment_benefits") do
    IterationTemplate.new("unemployment_benefits", 0.7)
    |> IterationTemplate.set_drop_rate(0.05)
    |> IterationTemplate.set_new_record_percentage(0.10)
    |> IterationTemplate.add_column_resampler(
      "benefit_type",
      0.70,
      {:categorical,
       {["temporary", "permanent", "due to sickness", "settlement"], [0.5, 0.1, 0.3, 0.1]}}
    )
    |> IterationTemplate.add_column_resampler("amount", 0.3, {:normal, 1000, 200})
    |> IterationTemplate.add_column_resampler("additional_income", 0.9, {:normal, 300, 50})
    |> IterationTemplate.add_column_transformer("age", fn age, _row -> age + 1 / 12 end)
    |> IterationTemplate.add_column_transformer("reporting_period", &transform_reporting_period/2)
  end

  defp transform_reporting_period(period, _), do: transform_reporting_period(period)

  defp transform_reporting_period(period) do
    NaiveDateTime.new!(period, ~T[00:00:00])
    |> NaiveDateTime.shift(month: 1)
    |> NaiveDateTime.to_date()
  end

  #   defp build_template("transactions", reporting_period) do
  #     base_date = parse_reporting_period(reporting_period)
  # 
  #     DatasetTemplate.new("transactions")
  #     |> DatasetTemplate.add_variable("transaction_id", {:sequence, 1})
  #     |> DatasetTemplate.add_variable("customer_id", {:uniform, 1, 10_000})
  #     |> DatasetTemplate.add_variable("amount", {:normal, 85.50, 10})
  #     |> DatasetTemplate.add_variable("transaction_date", {:constant, base_date})
  #     |> DatasetTemplate.add_variable(
  #       "category",
  #       {:categorical,
  #        {["groceries", "gas", "restaurant", "retail", "online"], [0.3, 0.2, 0.2, 0.1, 0.2]}}
  #     )
  #     |> DatasetTemplate.add_variable(
  #       "is_expensive",
  #       {:dependent, "amount", &is_expensive/2},
  #       ["amount"]
  #     )
  #   end

  defp build_template("unemployment_benefits", reporting_period) do
    base_date = parse_reporting_period(reporting_period)

    DatasetTemplate.new("unemployment_benefits")
    |> DatasetTemplate.add_variable("person_id", {:sequence, 1})
    |> DatasetTemplate.add_variable("reporting_period", {:constant, base_date})
    |> DatasetTemplate.add_variable("age", {:normal, 85.50, 10})
    |> DatasetTemplate.add_variable("gender", {:categorical, ["M", "F"]})
    |> DatasetTemplate.add_variable(
      "benefit_type",
      {:categorical,
       {["temporary", "permanent", "due to sickness", "settlement"], [0.5, 0.1, 0.3, 0.1]}}
    )
    |> DatasetTemplate.add_variable("amount", {:normal, 1000, 200})
    |> DatasetTemplate.add_variable("additional_income", {:normal, 300, 50})
  end

  defp build_template(unknown, _) do
    IO.puts("X Unkown template: #{unknown}")
    IO.puts("Available templates: unemployment_benefits")
    System.halt(1)
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

  defp format_bytes(bytes) do
    cond do
      bytes >= 1_000_000 -> "#{Float.round(bytes / 1_000_000, 1)} MB"
      bytes >= 1_000 -> "#{Float.round(bytes / 1_000, 1)} KB"
      true -> "#{bytes} B"
    end
  end

  defp left_pad(something) do
    string = to_string(something)

    if String.length(string) < 2 do
      "0" <> string
    else
      string
    end
  end
end
