defmodule PerhapTest.Adapters.Dynamo do
  use PerhapTest.Helper, port: 4499
  alias Perhap.Adapters.Eventstore.Dynamo

  @pause_interval 500

  setup do
    Application.put_env(:perhap, :eventstore, Perhap.Adapters.Eventstore.Dynamo, [])
    Perhap.Adapters.Eventstore.Dynamo.start_link([])
    :ok
  end


  test "put_event" do
    random_context = Enum.random([:a, :b, :c, :d, :e])
    random_event = make_random_event(
      %Perhap.Event.Metadata{context: random_context, entity_id: Perhap.Event.get_uuid_v4()} )
    check1 = ExAws.Dynamo.get_item("Events", %{event_id: random_event.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    Dynamo.put_event(random_event)
    :timer.sleep(@pause_interval)
    check2 = ExAws.Dynamo.get_item("Events", %{event_id: random_event.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!

    #cleanup
    ExAws.Dynamo.delete_item("Events", %{event_id: random_event.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Index", %{context: random_context, entity_id: random_event.metadata.entity_id}) |> ExAws.request!

    assert check1 == %{}
    refute check2 == %{}
  end

  test "get_event" do
    random_context = Enum.random([:a, :b, :c, :d, :e])
    random_event = make_random_event(
      %Perhap.Event.Metadata{context: random_context, entity_id: Perhap.Event.get_uuid_v4()} )
    check1 = Dynamo.get_event(random_event.event_id)
    Dynamo.put_event(random_event)
    :timer.sleep(@pause_interval)
    check2 = Dynamo.get_event(random_event.event_id)

    #cleanup
    ExAws.Dynamo.delete_item("Events", %{event_id: random_event.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Index", %{context: random_context, entity_id: random_event.metadata.entity_id}) |> ExAws.request!

    assert check1== {:error, "Event not found"}
    assert check2 == {:ok, random_event}
  end

  test "get_events with entity_id" do
    :ok
  end

  test "get_events without entity_id" do
    :ok
  end

end
