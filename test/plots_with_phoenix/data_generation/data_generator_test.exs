defmodule PlotsWithPhoenix.DataGeneratorTest do
  use ExUnit.Case, async: true
  alias PlotsWithPhoenix.{DataGenerator, DatasetTemplate}

  describe "generate_dataset/2" do
    test "generates correct number of rows" do
      template =
        DatasetTemplate.new("test")
        |> DatasetTemplate.add_variable(:id, {:sequence, 1})
        |> DatasetTemplate.add_variable(:value, {:constant, 42})

      {:ok, stream} = DataGenerator.generate_dataset(template, 100)
      rows = stream |> Enum.to_list() |> List.flatten()

      assert length(rows) == 100
    end

    test "generates data in chunks of 10,000" do
      template =
        DatasetTemplate.new("test")
        |> DatasetTemplate.add_variable(:id, {:sequence, 1})

      {:ok, stream} = DataGenerator.generate_dataset(template, 25_000)
      chunks = Enum.to_list(stream)

      assert length(chunks) == 3
      assert length(Enum.at(chunks, 0)) == 10_000
      assert length(Enum.at(chunks, 1)) == 10_000
      assert length(Enum.at(chunks, 2)) == 5_000
    end

    test "respects topological order for dependent variables" do
      template =
        DatasetTemplate.new("test")
        |> DatasetTemplate.add_variable(:base, {:constant, 10})
        |> DatasetTemplate.add_variable(:doubled, {:dependent, :base, fn val, _ -> val * 2 end}, [
          :base
        ])

      {:ok, stream} = DataGenerator.generate_dataset(template, 5)
      rows = stream |> Enum.to_list() |> List.flatten()

      Enum.each(rows, fn row ->
        assert row.doubled == row.base * 2
      end)
    end

    test "raises error for circular dependencies" do
      template =
        DatasetTemplate.new("test")
        |> DatasetTemplate.add_variable(:a, {:dependent, :b, fn val, _ -> val + 1 end}, [:b])
        |> DatasetTemplate.add_variable(:b, {:dependent, :a, fn val, _ -> val + 1 end}, [:a])

      assert_raise RuntimeError, "circular dependency detected in dataset template", fn ->
        {:ok, stream} = DataGenerator.generate_dataset(template, 1)
        Enum.to_list(stream)
      end
    end
  end

  describe "generate_single_value/3" do
    test "generates normal distribution values" do
      values =
        for _ <- 1..1000 do
          DataGenerator.generate_single_value({:normal, 50, 10}, 0, %{})
        end

      mean = Enum.sum(values) / length(values)
      assert_in_delta mean, 50, 5
    end

    test "generates uniform distribution values" do
      values =
        for _ <- 1..1000 do
          DataGenerator.generate_single_value({:uniform, 10, 20}, 0, %{})
        end

      assert Enum.all?(values, fn v -> v >= 10 and v <= 20 end)
    end

    test "generates categorical values without weights" do
      options = ["red", "green", "blue"]

      values =
        for _ <- 1..100 do
          DataGenerator.generate_single_value({:categorical, options}, 0, %{})
        end

      assert Enum.all?(values, fn v -> v in options end)
      assert length(Enum.uniq(values)) > 1
    end

    test "generates categorical values with weights" do
      options = ["common", "rare"]
      weights = [0.9, 0.1]

      values =
        for _ <- 1..1000 do
          DataGenerator.generate_single_value({:categorical, {options, weights}}, 0, %{})
        end

      common_count = Enum.count(values, fn v -> v == "common" end)
      assert common_count > 800
    end

    test "generates sequence values" do
      value1 = DataGenerator.generate_single_value({:sequence, 100}, 0, %{})
      value2 = DataGenerator.generate_single_value({:sequence, 100}, 5, %{})

      assert value1 == 100
      assert value2 == 105
    end

    test "generates dependent values" do
      row_data = %{base: 20}

      value =
        DataGenerator.generate_single_value(
          {:dependent, :base, fn val, idx -> val * 2 + idx end},
          3,
          row_data
        )

      assert value == 43
    end

    test "generates custom values" do
      custom_fn = fn row_idx, row_data ->
        Map.get(row_data, :multiplier, 1) * row_idx
      end

      value1 = DataGenerator.generate_single_value({:custom, custom_fn}, 5, %{multiplier: 3})
      value2 = DataGenerator.generate_single_value({:custom, custom_fn}, 10, %{})

      assert value1 == 15
      assert value2 == 10
    end

    test "generates constant values" do
      value = DataGenerator.generate_single_value({:constant, "fixed"}, 42, %{foo: "bar"})
      assert value == "fixed"
    end
  end
end
