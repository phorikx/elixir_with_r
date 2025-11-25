defmodule PlotsWithPhoenix.TemplateParser do
  @moduledoc """
  Parses YAML template files into DatasetTemplate and IterationTemplate structs.

  ## YAML Structure

  ```yaml
  name: template_name

  dataset:
    variables:
      variable_name:
        type: sequence|normal|uniform|categorical|constant|dependent|custom
        # Type-specific fields...
        depends_on: [other_variable]  # Optional, for dependent types

  iteration:
    identical_rate: 0.7
    drop_rate: 0.05
    new_record_percentage: 0.10
    column_resamplers:
      - column: column_name
        probability: 0.5
        generator: {...}
    column_transformers:
      - column: column_name
        type: add|multiply|shift_date|custom
        # Type-specific fields...
  ```

  ## Generator Types

  - `sequence`: Generates sequential integers
    - `start`: Starting value (integer)

  - `normal`: Normal distribution
    - `mean`: Mean value (float)
    - `sd`: Standard deviation (float)

  - `uniform`: Uniform distribution
    - `min`: Minimum value (float)
    - `max`: Maximum value (float)

  - `categorical`: Categorical values with optional weights
    - `options`: List of values
    - `weights`: Optional list of weights (must sum to any positive number)

  - `constant`: Constant value
    - `value`: The constant value (any type, supports {{placeholder}})

  - `dependent`: Computed from other variables
    - `depends_on_var`: Name of variable to depend on
    - `elixir`: Elixir code string, e.g., "fn val, idx -> val * 2 end"

  - `custom`: Custom function
    - `elixir`: Elixir code string, e.g., "fn idx, row -> idx * 2 end"

  ## Transformer Types

  - `add`: Add a value
    - `value`: Number to add

  - `multiply`: Multiply by a value
    - `value`: Number to multiply by

  - `shift_date`: Shift a date
    - `months`: Number of months to shift (can be negative)
    - `days`: Number of days to shift (can be negative)

  - `custom`: Custom transformer function
    - `elixir`: Elixir code string, e.g., "fn value, row -> value + 1 end"
  """

  alias PlotsWithPhoenix.{DatasetTemplate, IterationTemplate}

  @doc """
  Parses a YAML template file and returns {dataset_template, iteration_template}.

  Placeholders in the form {{name}} can be provided via the substitutions map.
  """
  def parse_file(file_path, substitutions \\ %{}) do
    case YamlElixir.read_from_file(file_path) do
      {:ok, yaml} ->
        parse_yaml(yaml, substitutions)

      {:error, reason} ->
        {:error, "Failed to parse YAML: #{inspect(reason)}"}
    end
  end

  @doc """
  Parses YAML string content.
  """
  def parse_string(yaml_string, substitutions \\ %{}) do
    case YamlElixir.read_from_string(yaml_string) do
      {:ok, yaml} ->
        parse_yaml(yaml, substitutions)

      {:error, reason} ->
        {:error, "Failed to parse YAML: #{inspect(reason)}"}
    end
  end

  defp parse_yaml(yaml, substitutions) do
    name = yaml["name"] || "unnamed"

    dataset_template =
      if dataset = yaml["dataset"] do
        parse_dataset_template(name, dataset, substitutions)
      else
        nil
      end

    iteration_template =
      if iteration = yaml["iteration"] do
        parse_iteration_template(name, iteration)
      else
        nil
      end

    {:ok, dataset_template, iteration_template}
  end

  defp parse_dataset_template(name, dataset, substitutions) do
    variables = dataset["variables"] || %{}

    template = DatasetTemplate.new(name)

    # Sort variables to handle dependencies properly
    variables
    |> Enum.to_list()
    |> Enum.reduce(template, fn {var_name, var_spec}, acc ->
      var_name_atom = String.to_atom(var_name)
      generator = parse_generator(var_spec, substitutions)
      depends_on = parse_depends_on(var_spec)

      DatasetTemplate.add_variable(acc, var_name_atom, generator, depends_on)
    end)
  end

  defp parse_generator(spec, substitutions) do
    type = spec["type"]

    case type do
      "sequence" ->
        start = spec["start"] || 1
        {:sequence, start}

      "normal" ->
        mean = spec["mean"] || 0.0
        sd = spec["sd"] || 1.0
        {:normal, mean, sd}

      "uniform" ->
        min = spec["min"] || 0.0
        max = spec["max"] || 1.0
        {:uniform, min, max}

      "categorical" ->
        options = spec["options"] || []
        weights = spec["weights"]

        if weights do
          {:categorical, {options, weights}}
        else
          {:categorical, options}
        end

      "constant" ->
        value = substitute_placeholders(spec["value"], substitutions)
        {:constant, value}

      "dependent" ->
        depends_on_var = String.to_atom(spec["depends_on_var"])
        elixir_code = spec["elixir"]
        {fun, _} = Code.eval_string(elixir_code)
        {:dependent, depends_on_var, fun}

      "custom" ->
        elixir_code = spec["elixir"]
        {fun, _} = Code.eval_string(elixir_code)
        {:custom, fun}

      _ ->
        raise "Unknown generator type: #{type}"
    end
  end

  defp parse_depends_on(spec) do
    case spec["depends_on"] do
      nil ->
        []

      list when is_list(list) ->
        Enum.map(list, &String.to_atom/1)

      single ->
        [String.to_atom(single)]
    end
  end

  defp substitute_placeholders(value, substitutions) when is_binary(value) do
    Enum.reduce(substitutions, value, fn {key, replacement}, acc ->
      String.replace(acc, "{{#{key}}}", to_string(replacement))
    end)
    |> convert_value()
  end

  defp substitute_placeholders(value, _substitutions), do: value

  defp convert_value("{{" <> _ = value), do: value

  defp convert_value(value) when is_binary(value) do
    cond do
      String.match?(value, ~r/^\d{4}-\d{2}-\d{2}$/) ->
        case Date.from_iso8601(value) do
          {:ok, date} -> date
          _ -> value
        end

      true ->
        value
    end
  end

  defp parse_iteration_template(name, iteration) do
    identical_rate = iteration["identical_rate"] || 0.7

    template = IterationTemplate.new(name, identical_rate)

    template =
      if drop_rate = iteration["drop_rate"] do
        IterationTemplate.set_drop_rate(template, drop_rate)
      else
        template
      end

    template =
      if new_record_pct = iteration["new_record_percentage"] do
        IterationTemplate.set_new_record_percentage(template, new_record_pct)
      else
        template
      end

    template =
      if resamplers = iteration["column_resamplers"] do
        Enum.reduce(resamplers, template, fn resampler, acc ->
          column = String.to_atom(resampler["column"])
          probability = resampler["probability"] || 1.0
          generator = parse_generator(resampler["generator"], %{})

          IterationTemplate.add_column_resampler(acc, column, probability, generator)
        end)
      else
        template
      end

    template =
      if transformers = iteration["column_transformers"] do
        Enum.reduce(transformers, template, fn transformer, acc ->
          column = String.to_atom(transformer["column"])
          transform_fn = parse_transformer(transformer)

          IterationTemplate.add_column_transformer(acc, column, transform_fn)
        end)
      else
        template
      end

    template
  end

  defp parse_transformer(spec) do
    type = spec["type"]

    case type do
      "add" ->
        value = spec["value"]
        fn val, _row -> val + value end

      "multiply" ->
        value = spec["value"]
        fn val, _row -> val * value end

      "shift_date" ->
        months = spec["months"] || 0
        days = spec["days"] || 0

        fn date, _row ->
          # Handle various date formats
          date_value =
            cond do
              is_struct(date, Date) ->
                date

              is_struct(date, NaiveDateTime) ->
                NaiveDateTime.to_date(date)

              is_binary(date) ->
                case Date.from_iso8601(date) do
                  {:ok, d} -> d
                  _ -> raise "Invalid date string: #{date}"
                end

              is_nil(date) ->
                raise "Cannot shift_date on nil value"

              true ->
                raise "Unsupported date type: #{inspect(date)}"
            end

          date_value
          |> NaiveDateTime.new!(~T[00:00:00])
          |> NaiveDateTime.shift(month: months, day: days)
          |> NaiveDateTime.to_date()
        end

      "custom" ->
        elixir_code = spec["elixir"]
        {fun, _} = Code.eval_string(elixir_code)
        fun

      _ ->
        raise "Unknown transformer type: #{type}"
    end
  end
end
