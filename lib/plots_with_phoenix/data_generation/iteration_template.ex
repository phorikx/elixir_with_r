defmodule PlotsWithPhoenix.IterationTemplate do
  defstruct [
    :name,
    :identical_rate,
    :drop_rate,
    :new_record_percentage,
    :column_resamplers,
    :column_transformers
  ]

  defmodule ColumnResampler do
    defstruct [:column, :probability, :generator]
  end

  defmodule ColumnTransformer do
    defstruct [:column, :transformer_fn]
  end

  def new(name, identical_rate \\ 0.7) do
    %__MODULE__{
      name: name,
      identical_rate: identical_rate,
      drop_rate: 0.0,
      new_record_percentage: 0.0,
      column_resamplers: [],
      column_transformers: []
    }
  end

  def set_new_record_percentage(%__MODULE__{} = template, percentage) do
    %{template | new_record_percentage: percentage}
  end

  def set_drop_rate(%__MODULE__{} = template, percentage) do
    if percentage > 1.0 - template.identical_rate do
      raise ArgumentError,
            "drop rate (#{percentage}) cannot exceed 1 - #{template.identical_rate} ( 1 - identical_rate) "
    end

    %{template | drop_rate: percentage}
  end

  def add_column_resampler(%__MODULE__{} = template, column, probability, generator) do
    resampler = %ColumnResampler{
      column: column,
      probability: probability,
      generator: generator
    }

    %{template | column_resamplers: [resampler | template.column_resamplers]}
  end

  def add_column_resampler(
        %__MODULE__{} = template,
        column,
        probability,
        %PlotsWithPhoenix.DatasetTemplate{} = previous_template
      ) do
    generator =
      previous_template
      |> Map.get(:variables)
      |> Enum.filter(fn {col, _, _, _} -> col == column end)
      |> Enum.at(0)
      |> Map.get(:generator)

    resampler = %ColumnResampler{
      column: column,
      probability: probability,
      generator: generator
    }

    %{template | column_resamplers: [resampler | template.column_resamplers]}
  end

  def add_column_transformer(%__MODULE__{} = template, column, transform_fn) do
    transformer = %ColumnTransformer{
      column: column,
      transformer_fn: transform_fn
    }

    %{template | column_transformers: [transformer | template.column_transformers]}
  end
end
