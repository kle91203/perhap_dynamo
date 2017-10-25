defmodule PerhapTest.Adapters.Dynamo do
  use PerhapTest.Helper, port: 4499
  alias Perhap.Adapters.Eventstore.Dynamo

  setup do
    Application.put_env(:perhap, :eventstore, Perhap.Adapters.Eventstore.Dynamo, [])
  end


  test "put_event" do
    random_context = Enum.random([:a, :b, :c, :d, :e])
    random_event = make_random_event(
      %Perhap.Event.Metadata{context: random_context, entity_id: Perhap.Event.get_uuid_v4()} )
    assert ExAws.Dynamo.get_item("Events", %{event_id: random_event.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request! == %{}
    Dynamo.put_event(random_event)
    refute ExAws.Dynamo.get_item("Events", %{event_id: random_event.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request! == %{}

    #cleanup
    ExAws.Dynamo.delete_item("Events", %{event_id: random_event.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Index", %{context: random_context, entity_id: random_event.metadata.entity_id}) |> ExAws.request!
  end

  test "get_event" do
    random_context = Enum.random([:a, :b, :c, :d, :e])
    random_event = make_random_event(
      %Perhap.Event.Metadata{context: random_context, entity_id: Perhap.Event.get_uuid_v4()} )
    assert Dynamo.get_event(random_event.event_id) == {:error, "Event not found"}
    Dynamo.put_event(random_event)
    assert Dynamo.get_event(random_event.event_id) == {:ok, random_event}

    #cleanup
    ExAws.Dynamo.delete_item("Events", %{event_id: random_event.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Index", %{context: random_context, entity_id: random_event.metadata.entity_id}) |> ExAws.request!
  end

  test "get_events with entity_id" do
    :ok
  end

  test "get_events without entity_id" do
    :ok
  end

end
