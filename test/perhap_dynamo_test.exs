defmodule PerhapDynamoTest do
  use ExUnit.Case
  doctest PerhapDynamo

  test "greets the world" do
    assert PerhapDynamo.hello() == :world
  end
end
