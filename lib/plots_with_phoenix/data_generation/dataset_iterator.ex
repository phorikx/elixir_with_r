defmodule PlotsWithPhoenix.DatasetIterator do
  alias PlotsWithPhoenix.{DataGenerator, DatasetTemplate, IterationTemplate}

  def iterate_dataset(previous_data, original_template, iteration_template) do
    previous_rows =
      if is_list(previous_data), do: previous_data, else: Enum.to_list(previous_data)

    sequence_maxes = find_sequence_maxes(previous_rows, original_template)

    {surviving_records, modified_records} =
      process_existing_records(previous_rows, iteration_template)

    new_record_count = round(length(previous_rows) * iteration_template.new_record_percentage)
    new_records = generate_new_records(original_template, new_record_count, sequence_maxes)

    all_records = surviving_records ++ modified_records ++ new_records

    transformed_records = apply_column_transformations(all_records, iteration_template)

    {:ok, transformed_records}
  end

  defp find_sequence_maxes(previous_rows, %DatasetTemplate{variables: variables}) do
    sequence_vars =
      variables
      |> Enum.filter(fn {_name, var} ->
        match?({:sequence, _}, var.generator)
      end)
      |> Enum.map(fn {name, _var} -> name end)

    Enum.reduce(sequence_vars, %{}, fn var_name, acc ->
      max_value =
        previous_rows
        |> Enum.map(fn row -> Map.get(row, var_name, 0) end)
        |> Enum.max(fn -> 0 end)

      Map.put(acc, var_name, max_value)
    end)
  end

  defp process_existing_records(
         previous_rows,
         %IterationTemplate{
           identical_rate: identical_rate,
           drop_rate: drop_rate,
           column_resamplers: resamplers
         }
       ) do
    {identical, non_identical} =
      Enum.split_with(previous_rows, fn _row ->
        :rand.uniform() < identical_rate
      end)

    {_dropped, candidates_for_modification} =
      Enum.split_with(non_identical, fn _row ->
        :rand.uniform() < drop_rate / (1.0 - identical_rate)
      end)

    modified =
      Enum.map(candidates_for_modification, fn row ->
        apply_column_resamplers(row, resamplers)
      end)

    {identical, modified}
  end

  defp apply_column_resamplers(row, resamplers) do
    Enum.reduce(resamplers, row, fn resampler, current_row ->
      %IterationTemplate.ColumnResampler{
        column: column,
        probability: probability,
        generator: generator
      } = resampler

      if :rand.uniform() < probability do
        new_value = DataGenerator.generate_single_value(generator, 0, current_row)
        Map.put(current_row, column, new_value)
      else
        current_row
      end
    end)
  end

  defp generate_new_records(_template, 0, _seq_maxes), do: []

  defp generate_new_records(original_template, new_record_count, sequence_maxes) do
    updated_template = continue_sequences(original_template, sequence_maxes)

    {:ok, data_stream, _column_order} =
      DataGenerator.generate_dataset(updated_template, new_record_count)

    Enum.to_list(data_stream) |> List.flatten()
  end

  defp continue_sequences(
         %DatasetTemplate{variables: variables} = original_template,
         sequence_maxes
       ) do
    updated_variables =
      Enum.reduce(variables, %{}, fn {name, var}, acc ->
        updated_var =
          case var.generator do
            {:sequence, _start} ->
              new_start = Map.get(sequence_maxes, name, 0) + 1
              %{var | generator: {:sequence, new_start}}

            _ ->
              var
          end

        Map.put(acc, name, updated_var)
      end)

    %{original_template | variables: updated_variables}
  end

  defp apply_column_transformations(
         records,
         %IterationTemplate{column_transformers: transformers}
       ) do
    Enum.reduce(transformers, records, fn transformer, current_records ->
      %IterationTemplate.ColumnTransformer{column: column, transformer_fn: transformer_fn} =
        transformer

      Enum.map(current_records, fn record ->
        current_value = Map.get(record, column)
        new_value = transformer_fn.(current_value, record)
        Map.put(record, column, new_value)
      end)
    end)
  end
end
