defmodule Mix.Tasks.GenerateDataset do
  @moduledoc """
  Generates synthetic datasets for development and testing. 

  ## Examples 

  mix generate_dataset --rows 1000000 --output /tmp/export.parquet
  mix generate_dataset --template customers --rows 5000 --reporting_period 202401
  """

  use Mix.Task
  alias PlotsWithPhoenix.{DatasetTemplate, DataGenerator, ParquetExporter}

  @shortdoc "Generates synthetic dataset"
  @switches [
    template: :string,
    rows: :integer,
    output: :string,
    reporting_period: :string,
    help: :boolean
  ]
  @aliases [
    t: :template,
    r: :rows,
    o: :output,
    p: :reporting_period,
    h: :help
  ]

  def run(args) do
    Mix.Task.run("app.start")

    {opts, _} = OptionParser.parse!(args, switches: @switches, aliases: @aliases)

    if opts[:help] do
      print_help()
    else
      generate_dataset(opts)
    end
  end

  defp print_help do
    IO.puts(@moduledoc)
  end

  defp generate_dataset(opts \\ []) do
    template_name = opts[:template] || "transactions"
    n_rows = opts[:rows] || 10_000
    output_path = opts[:output] || "/tmp/dataset_#{template_name}_#{n_rows}.parquet"
    reporting_period = opts[:reporting_period]

    IO.puts("Generating #{template_name} dataset with #{n_rows} rows")
    template = build_template(template_name, reporting_period)

    start_time = System.monotonic_time()
    {:ok, data_stream, column_order} = DataGenerator.generate_dataset(template, n_rows)
    :ok = ParquetExporter.export_to_parquet(data_stream, column_order, output_path)

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

  defp build_template("transactions", reporting_period) do
    base_date = parse_reporting_period(reporting_period)

    DatasetTemplate.new("transactions")
    |> DatasetTemplate.add_variable("transaction_id", {:sequence, 1})
    |> DatasetTemplate.add_variable("customer_id", {:uniform, 1, 10_000})
    |> DatasetTemplate.add_variable("amount", {:normal, 85.50, 10})
    |> DatasetTemplate.add_variable("transaction_date", {:constant, base_date})
    |> DatasetTemplate.add_variable(
      "category",
      {:categorical,
       {["groceries", "gas", "restaurant", "retail", "online"], [0.3, 0.2, 0.2, 0.1, 0.2]}}
    )
    |> DatasetTemplate.add_variable(
      "is_expensive",
      {:dependent, "amount", &is_expensive/2},
      ["amount"]
    )
  end

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
    IO.puts("Available templates: transactions, unemployment_benefits")
    System.halt(1)
  end

  defp is_expensive(_row_idx, row_data) do
    if row_data["amount"] > 100 do
      true
    else
      false
    end
  end

  defp parse_reporting_period(period) when is_nil(period), do: Date.utc_today()

  defp parse_reporting_period(<<yyyy::binary-4, mm::binary-2, dd::binary-2>>) do
    [yyyy, mm, dd] = for i <- [yyyy, mm, dd], do: String.to_integer(i)
    NaiveDateTime.new!(yyyy, mm, dd, 0, 0, 0) |> NaiveDateTime.to_date()
  end

  defp parse_reporting_period(_) do
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
end
