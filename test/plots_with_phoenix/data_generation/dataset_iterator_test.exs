defmodule PlotsWithPhoenix.DatasetIteratorTest do
  use ExUnit.Case, async: true
  alias PlotsWithPhoenix.{DataGenerator, DatasetIterator, DatasetTemplate, IterationTemplate}

  setup do
    template =
      DatasetTemplate.new("test")
      |> DatasetTemplate.add_variable(:id, {:sequence, 1})
      |> DatasetTemplate.add_variable(:value, {:uniform, 0, 100})
      |> DatasetTemplate.add_variable(:category, {:categorical, ["A", "B", "C"]})

    {:ok, stream} = DataGenerator.generate_dataset(template, 100)
    previous_data = stream |> Enum.to_list() |> List.flatten()

    {:ok, template: template, previous_data: previous_data}
  end

  describe "iterate_dataset/3" do
    test "returns correct data structure", %{template: template, previous_data: previous_data} do
      iteration_template = IterationTemplate.new("iter")

      {:ok, result} = DatasetIterator.iterate_dataset(previous_data, template, iteration_template)

      assert is_list(result)
      assert length(result) > 0
    end

    test "maintains approximately identical_rate records", %{
      template: template,
      previous_data: previous_data
    } do
      iteration_template =
        IterationTemplate.new("iter", 0.8)
        |> IterationTemplate.set_drop_rate(0.0)
        |> IterationTemplate.set_new_record_percentage(0.0)

      {:ok, result} = DatasetIterator.iterate_dataset(previous_data, template, iteration_template)

      assert length(result) >= 70
      assert length(result) <= length(previous_data)
    end

    test "drops records based on drop_rate", %{template: template, previous_data: previous_data} do
      iteration_template =
        IterationTemplate.new("iter", 0.5)
        |> IterationTemplate.set_drop_rate(0.3)
        |> IterationTemplate.set_new_record_percentage(0.0)

      {:ok, result} = DatasetIterator.iterate_dataset(previous_data, template, iteration_template)

      assert length(result) < length(previous_data)
    end

    test "adds new records based on percentage", %{
      template: template,
      previous_data: previous_data
    } do
      iteration_template =
        IterationTemplate.new("iter", 1.0)
        |> IterationTemplate.set_new_record_percentage(0.2)

      {:ok, result} = DatasetIterator.iterate_dataset(previous_data, template, iteration_template)

      expected_min = length(previous_data) + round(length(previous_data) * 0.2)
      assert length(result) >= expected_min
    end

    test "continues sequence from max value", %{template: template, previous_data: previous_data} do
      max_id = Enum.max_by(previous_data, fn row -> row.id end).id

      iteration_template =
        IterationTemplate.new("iter", 0.0)
        |> IterationTemplate.set_new_record_percentage(0.1)

      {:ok, result} = DatasetIterator.iterate_dataset(previous_data, template, iteration_template)

      # Note: iterate_dataset now returns records with string keys
      new_records = Enum.filter(result, fn row -> row["id"] > max_id end)
      assert length(new_records) > 0

      new_ids = Enum.map(new_records, fn row -> row["id"] end) |> Enum.sort()
      assert hd(new_ids) == max_id + 1
    end

    test "applies column resamplers", %{template: template, previous_data: previous_data} do
      iteration_template =
        IterationTemplate.new("iter", 0.0)
        |> IterationTemplate.set_new_record_percentage(0.0)
        |> IterationTemplate.add_column_resampler(:category, 1.0, {:constant, "RESAMPLED"})

      {:ok, result} = DatasetIterator.iterate_dataset(previous_data, template, iteration_template)

      # Note: iterate_dataset now returns records with string keys
      assert Enum.all?(result, fn row -> row["category"] == "RESAMPLED" end)
    end

    test "applies column transformers", %{template: template, previous_data: previous_data} do
      iteration_template =
        IterationTemplate.new("iter", 1.0)
        |> IterationTemplate.set_new_record_percentage(0.0)
        |> IterationTemplate.add_column_transformer(:value, fn val, _row -> val * 2 end)

      {:ok, result} = DatasetIterator.iterate_dataset(previous_data, template, iteration_template)

      previous_map = Map.new(previous_data, fn row -> {row.id, row.value} end)

      # Note: iterate_dataset now returns records with string keys
      Enum.each(result, fn row ->
        original_value = previous_map[row["id"]]
        assert_in_delta row["value"], original_value * 2, 0.001
      end)
    end

    test "handles empty previous data", %{template: template} do
      iteration_template =
        IterationTemplate.new("iter")
        |> IterationTemplate.set_new_record_percentage(0.1)

      {:ok, result} = DatasetIterator.iterate_dataset([], template, iteration_template)

      assert result == []
    end

    test "handles streams as input", %{template: template, previous_data: previous_data} do
      stream = Stream.map(previous_data, & &1)

      iteration_template =
        IterationTemplate.new("iter", 1.0)
        |> IterationTemplate.set_new_record_percentage(0.0)

      {:ok, result} = DatasetIterator.iterate_dataset(stream, template, iteration_template)

      assert length(result) == length(previous_data)
    end
  end
end
