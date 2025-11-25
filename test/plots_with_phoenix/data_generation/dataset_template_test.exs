defmodule PlotsWithPhoenix.DatasetTemplateTest do
  use ExUnit.Case, async: true
  alias PlotsWithPhoenix.DatasetTemplate

  describe "new/1" do
    test "creates a new template with given name" do
      template = DatasetTemplate.new("my_dataset")

      assert template.name == "my_dataset"
      assert template.variables == %{}
      assert MapSet.size(template.dependencies) == 0
    end
  end

  describe "add_variable/4" do
    test "adds a variable without dependencies" do
      template =
        DatasetTemplate.new("test")
        |> DatasetTemplate.add_variable(:age, {:normal, 30, 5})

      assert Map.has_key?(template.variables, :age)
      assert template.variables[:age].name == :age
      assert template.variables[:age].type == :float
      assert template.variables[:age].generator == {:normal, 30, 5}
      assert template.variables[:age].depends_on == []
    end

    test "adds a variable with dependencies" do
      template =
        DatasetTemplate.new("test")
        |> DatasetTemplate.add_variable(:base, {:constant, 10})
        |> DatasetTemplate.add_variable(
          :derived,
          {:dependent, :base, fn v, _ -> v * 2 end},
          [:base]
        )

      assert Map.has_key?(template.variables, :derived)
      assert template.variables[:derived].depends_on == [:base]
      assert MapSet.member?(template.dependencies, {:derived, :base})
    end

    test "correctly infers float type for normal distribution" do
      template =
        DatasetTemplate.new("test")
        |> DatasetTemplate.add_variable(:value, {:normal, 0, 1})

      assert template.variables[:value].type == :float
    end

    test "correctly infers float type for uniform distribution" do
      template =
        DatasetTemplate.new("test")
        |> DatasetTemplate.add_variable(:value, {:uniform, 0, 100})

      assert template.variables[:value].type == :float
    end

    test "correctly infers string type for categorical" do
      template =
        DatasetTemplate.new("test")
        |> DatasetTemplate.add_variable(:category, {:categorical, ["a", "b", "c"]})

      assert template.variables[:category].type == :string
    end

    test "correctly infers integer type for sequence" do
      template =
        DatasetTemplate.new("test")
        |> DatasetTemplate.add_variable(:id, {:sequence, 1})

      assert template.variables[:id].type == :integer
    end

    test "correctly infers dynamic type for dependent" do
      template =
        DatasetTemplate.new("test")
        |> DatasetTemplate.add_variable(:base, {:constant, 1})
        |> DatasetTemplate.add_variable(:derived, {:dependent, :base, fn v, _ -> v end}, [:base])

      assert template.variables[:derived].type == :dynamic
    end

    test "correctly infers dynamic type for constant" do
      template =
        DatasetTemplate.new("test")
        |> DatasetTemplate.add_variable(:fixed, {:constant, 42})

      assert template.variables[:fixed].type == :dynamic
    end

    test "correctly infers dynamic type for custom" do
      template =
        DatasetTemplate.new("test")
        |> DatasetTemplate.add_variable(:custom, {:custom, fn _, _ -> "value" end})

      assert template.variables[:custom].type == :dynamic
    end

    test "tracks multiple dependencies correctly" do
      template =
        DatasetTemplate.new("test")
        |> DatasetTemplate.add_variable(:a, {:constant, 1})
        |> DatasetTemplate.add_variable(:b, {:constant, 2})
        |> DatasetTemplate.add_variable(
          :sum,
          {:custom, fn _, row -> row.a + row.b end},
          [:a, :b]
        )

      assert MapSet.member?(template.dependencies, {:sum, :a})
      assert MapSet.member?(template.dependencies, {:sum, :b})
    end
  end
end
