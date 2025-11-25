defmodule PlotsWithPhoenix.IterationTemplateTest do
  use ExUnit.Case, async: true
  alias PlotsWithPhoenix.IterationTemplate

  describe "new/2" do
    test "creates template with default identical_rate of 0.7" do
      template = IterationTemplate.new("test")

      assert template.name == "test"
      assert template.identical_rate == 0.7
      assert template.drop_rate == 0.0
      assert template.new_record_percentage == 0.0
      assert template.column_resamplers == []
      assert template.column_transformers == []
    end

    test "creates template with custom identical_rate" do
      template = IterationTemplate.new("test", 0.85)

      assert template.identical_rate == 0.85
    end
  end

  describe "set_new_record_percentage/2" do
    test "sets new record percentage" do
      template =
        IterationTemplate.new("test")
        |> IterationTemplate.set_new_record_percentage(0.15)

      assert template.new_record_percentage == 0.15
    end

    test "allows chaining" do
      template =
        IterationTemplate.new("test")
        |> IterationTemplate.set_new_record_percentage(0.1)
        |> IterationTemplate.set_drop_rate(0.05)

      assert template.new_record_percentage == 0.1
      assert template.drop_rate == 0.05
    end
  end

  describe "set_drop_rate/2" do
    test "sets drop rate when valid" do
      template =
        IterationTemplate.new("test", 0.7)
        |> IterationTemplate.set_drop_rate(0.2)

      assert template.drop_rate == 0.2
    end

    test "raises error when drop rate exceeds allowed threshold" do
      template = IterationTemplate.new("test", 0.7)

      assert_raise ArgumentError, ~r/drop rate .* cannot exceed/, fn ->
        IterationTemplate.set_drop_rate(template, 0.4)
      end
    end

    test "accepts drop rate equal to 1 - identical_rate" do
      template =
        IterationTemplate.new("test", 0.6)
        |> IterationTemplate.set_drop_rate(0.4)

      assert template.drop_rate == 0.4
    end
  end

  describe "add_column_resampler/4" do
    test "adds column resampler with generator" do
      template =
        IterationTemplate.new("test")
        |> IterationTemplate.add_column_resampler(:age, 0.3, {:normal, 35, 10})

      assert length(template.column_resamplers) == 1
      resampler = hd(template.column_resamplers)
      assert resampler.column == :age
      assert resampler.probability == 0.3
      assert resampler.generator == {:normal, 35, 10}
    end

    test "supports multiple resamplers" do
      template =
        IterationTemplate.new("test")
        |> IterationTemplate.add_column_resampler(:age, 0.3, {:normal, 35, 10})
        |> IterationTemplate.add_column_resampler(
          :status,
          0.5,
          {:categorical, ["active", "inactive"]}
        )

      assert length(template.column_resamplers) == 2
    end

    test "adds resamplers in LIFO order" do
      template =
        IterationTemplate.new("test")
        |> IterationTemplate.add_column_resampler(:first, 0.1, {:constant, 1})
        |> IterationTemplate.add_column_resampler(:second, 0.2, {:constant, 2})

      [second, first] = template.column_resamplers
      assert first.column == :first
      assert second.column == :second
    end
  end

  describe "add_column_transformer/3" do
    test "adds column transformer" do
      transform_fn = fn value, _row -> value * 2 end

      template =
        IterationTemplate.new("test")
        |> IterationTemplate.add_column_transformer(:price, transform_fn)

      assert length(template.column_transformers) == 1
      transformer = hd(template.column_transformers)
      assert transformer.column == :price
      assert is_function(transformer.transformer_fn, 2)
    end

    test "supports multiple transformers" do
      template =
        IterationTemplate.new("test")
        |> IterationTemplate.add_column_transformer(:price, fn v, _ -> v * 2 end)
        |> IterationTemplate.add_column_transformer(:age, fn v, _ -> v + 1 end)

      assert length(template.column_transformers) == 2
    end

    test "transformer function receives value and row" do
      transform_fn = fn value, row ->
        multiplier = Map.get(row, :multiplier, 1)
        value * multiplier
      end

      template =
        IterationTemplate.new("test")
        |> IterationTemplate.add_column_transformer(:amount, transform_fn)

      transformer = hd(template.column_transformers)
      result = transformer.transformer_fn.(10, %{multiplier: 3})
      assert result == 30
    end
  end

  describe "full workflow" do
    test "creates complete iteration template" do
      template =
        IterationTemplate.new("monthly_update", 0.75)
        |> IterationTemplate.set_drop_rate(0.1)
        |> IterationTemplate.set_new_record_percentage(0.05)
        |> IterationTemplate.add_column_resampler(
          :status,
          0.2,
          {:categorical, ["active", "churned"]}
        )
        |> IterationTemplate.add_column_transformer(:months_active, fn v, _ -> v + 1 end)

      assert template.name == "monthly_update"
      assert template.identical_rate == 0.75
      assert template.drop_rate == 0.1
      assert template.new_record_percentage == 0.05
      assert length(template.column_resamplers) == 1
      assert length(template.column_transformers) == 1
    end
  end
end
