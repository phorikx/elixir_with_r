defmodule PlotsWithPhoenix.DataGenerator do
  alias PlotsWithPhoenix.DatasetTemplate

  def generate_dataset(%DatasetTemplate{} = template, n_rows) when n_rows > 0 do
    generation_order = topological_sort(template)

    data_stream =
      0..(n_rows - 1)
      |> Stream.map(fn row_idx ->
        generate_row(template, generation_order, row_idx, %{})
      end)
      |> Stream.chunk_every(10_000)

    {:ok, data_stream, generation_order}
  end

  defp generate_row(template, [var_name | rest], row_idx, row_data) do
    variable = template.variables[var_name]
    value = generate_value(variable, row_idx, row_data)
    new_row_data = Map.put(row_data, var_name, value)
    generate_row(template, rest, row_idx, new_row_data)
  end

  defp generate_row(_template, [], _row_idx, row_data), do: row_data

  defp generate_value(%DatasetTemplate.Variable{generator: generator}, row_idx, row_data) do
    case generator do
      {:normal, mean, sd} ->
        :rand.normal(mean, sd)

      {:uniform, min, max} ->
        :rand.uniform() * (max - min) + min

      {:categorical, options} when is_list(options) ->
        Enum.random(options)

      {:categorical, {options, weights}} ->
        weighted_sample(options, weights)

      {:sequence, start} ->
        start + row_idx

      {:dependent, dependency, mapper} ->
        dependent_value = Map.get(row_data, dependency)
        mapper.(dependent_value, row_idx)

      {:custom, fun} when is_function(fun, 2) ->
        fun.(row_idx, row_data)
    end
  end

  defp weighted_sample(options, weights) do
    total = Enum.sum(weights)
    target = :rand.uniform() * total

    {option, _} =
      Enum.zip(options, weights)
      |> Enum.reduce_while({nil, 0}, fn {opt, weight}, {_, acc} ->
        new_acc = acc + weight
        if new_acc >= target, do: {:halt, {opt, new_acc}}, else: {:cont, {nil, new_acc}}
      end)

    option
  end

  defp topological_sort(%DatasetTemplate{variables: variables, dependencies: deps}) do
    in_degree =
      variables
      |> Map.keys()
      |> Map.new(fn var -> {var, 0} end)

    in_degree =
      Enum.reduce(deps, in_degree, fn {to, _from}, acc ->
        Map.update!(acc, to, &(&1 + 1))
      end)

    queue =
      in_degree
      |> Enum.filter(fn {_var, degree} -> degree == 0 end)
      |> Enum.map(fn {var, _} -> var end)

    kahn_sort(queue, deps, in_degree, [])
  end

  defp kahn_sort([node | queue], deps, in_degree, result) do
    dependents =
      deps
      |> Enum.filter(fn {_to, from} -> from == node end)
      |> Enum.map(fn {to, _from} -> to end)

    {new_queue, new_in_degree} =
      Enum.reduce(dependents, {queue, in_degree}, fn dependent, {q, degrees} ->
        new_degree = degrees[dependent] - 1
        new_degrees = Map.put(degrees, dependent, new_degree)

        if new_degree == 0 do
          {[dependent | q], new_degree}
        else
          {q, new_degrees}
        end
      end)

    kahn_sort(new_queue, deps, new_in_degree, [node | result])
  end

  defp kahn_sort([], _deps, in_degree, result) do
    if Enum.any?(in_degree, fn {_, degree} -> degree > 0 end) do
      raise "circular dependency detected in dataset template"
    end

    Enum.reverse(result)
  end
end
