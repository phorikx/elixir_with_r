defmodule PlotsWithPhoenix.DatasetTemplate do
  @derive Jason.Encoder
  defstruct [:name, :variables, :dependencies]

  defmodule Variable do
    @derive Jason.Encoder
    defstruct [:name, :type, :generator, :depends_on]
  end

  def new(name) do
    %__MODULE__{
      name: name,
      variables: %{},
      dependencies: MapSet.new()
    }
  end

  def add_variable(%__MODULE__{} = template, name, generator, depends_on \\ []) do
    variable = %Variable{
      name: name,
      type: infer_type(generator),
      generator: generator,
      depends_on: depends_on
    }

    new_dependencies =
      Enum.reduce(depends_on, template.dependencies, fn dep, acc ->
        MapSet.put(acc, {name, dep})
      end)

    %{
      template
      | variables: Map.put(template.variables, name, variable),
        dependencies: new_dependencies
    }
  end

  defp infer_type({:normal, _, _}), do: :float
  defp infer_type({:uniform, _, _}), do: :float
  defp infer_type({:categorical, _}), do: :string
  defp infer_type({:sequence, _}), do: :integer
  defp infer_type({:dependent, _, _}), do: :dynamic
  defp infer_type({:constant, _}), do: :dynamic
  defp infer_type({:custom, _}), do: :dynamic
end
