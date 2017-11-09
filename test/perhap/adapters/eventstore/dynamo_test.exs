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
    #long
    #Use Dynamo#get_event?
    check1 = ExAws.Dynamo.get_item("Events", %{event_id: random_event.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    Dynamo.put_event(random_event)
    :timer.sleep(@pause_interval)
    #second time seeing this. will probably come up more. so, it should be extracted
    check2 = ExAws.Dynamo.get_item("Events", %{event_id: random_event.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!

    #cleanup
    ExAws.Dynamo.delete_item("Events", %{event_id: random_event.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Index", %{context: random_context, entity_id: random_event.metadata.entity_id}) |> ExAws.request!

    assert check1 == %{}
    refute check2 == %{} #you don't want to assert the event ID?
  end

  @tag dothis: true
  test "put_event_more" do
    random_context = Enum.random([:a, :b, :c, :d, :e])

    entity_id_1 = Perhap.Event.get_uuid_v4()
    IO.puts "entity_id_1: #{entity_id_1}"
    entity_id_2 = Perhap.Event.get_uuid_v4()
    IO.puts "entity_id_2: #{entity_id_2}"

    random_event_1_entity_1 = make_random_event(%Perhap.Event.Metadata{context: random_context, entity_id: entity_id_1} )
    random_event_2_entity_1 = make_random_event(%Perhap.Event.Metadata{context: random_context, entity_id: entity_id_1} )
    random_event_1_entity_2 = make_random_event(%Perhap.Event.Metadata{context: random_context, entity_id: entity_id_2} )
    IO.puts "random_event_1_entity_1: #{inspect random_event_1_entity_1}\n"
    IO.puts "random_event_2_entity_1: #{inspect random_event_2_entity_1}\n"
    IO.puts "random_event_1_entity_2: #{inspect random_event_1_entity_2}\n"


    #long
    #Use Dynamo#get_event?
    check1a = ExAws.Dynamo.get_item("Events", %{event_id: random_event_1_entity_1.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    check1b = ExAws.Dynamo.get_item("Events", %{event_id: random_event_2_entity_1.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    check1c = ExAws.Dynamo.get_item("Events", %{event_id: random_event_1_entity_2.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    Dynamo.put_event(random_event_1_entity_1)
    Dynamo.put_event(random_event_2_entity_1)
    Dynamo.put_event(random_event_1_entity_2)
    :timer.sleep(1000)
    #second time seeing this. will probably come up more. so, it should be extracted
    check2a = ExAws.Dynamo.get_item("Events", %{event_id: random_event_1_entity_1.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    check2b = ExAws.Dynamo.get_item("Events", %{event_id: random_event_2_entity_1.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    check2c = ExAws.Dynamo.get_item("Events", %{event_id: random_event_1_entity_2.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!

#-----------------------2nd batch

    entity_id_3 = Perhap.Event.get_uuid_v4()

    random_event_3_entity_1 = make_random_event(%Perhap.Event.Metadata{context: random_context, entity_id: entity_id_1} )
    random_event_2_entity_2 = make_random_event(%Perhap.Event.Metadata{context: random_context, entity_id: entity_id_2} )
    random_event_1_entity_3 = make_random_event(%Perhap.Event.Metadata{context: random_context, entity_id: entity_id_3} )
    random_event_2_entity_3 = make_random_event(%Perhap.Event.Metadata{context: random_context, entity_id: entity_id_3} )
    #long
    #Use Dynamo#get_event?
    check1d = ExAws.Dynamo.get_item("Events", %{event_id: random_event_3_entity_1.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    check1e = ExAws.Dynamo.get_item("Events", %{event_id: random_event_2_entity_2.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    check1f = ExAws.Dynamo.get_item("Events", %{event_id: random_event_1_entity_3.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    check1g = ExAws.Dynamo.get_item("Events", %{event_id: random_event_2_entity_3.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    Dynamo.put_event(random_event_3_entity_1)
    Dynamo.put_event(random_event_2_entity_2)
    Dynamo.put_event(random_event_1_entity_3)
    Dynamo.put_event(random_event_2_entity_3)
    :timer.sleep(@pause_interval)
    #second time seeing this. will probably come up more. so, it should be extracted
    check2d = ExAws.Dynamo.get_item("Events", %{event_id: random_event_3_entity_1.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    check2e = ExAws.Dynamo.get_item("Events", %{event_id: random_event_2_entity_2.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    check2f = ExAws.Dynamo.get_item("Events", %{event_id: random_event_1_entity_3.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    check2g = ExAws.Dynamo.get_item("Events", %{event_id: random_event_2_entity_3.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!

    #cleanup
    ExAws.Dynamo.delete_item("Events", %{event_id: random_event_1_entity_1.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Events", %{event_id: random_event_2_entity_1.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Events", %{event_id: random_event_1_entity_2.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Events", %{event_id: random_event_3_entity_1.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Events", %{event_id: random_event_2_entity_2.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Events", %{event_id: random_event_1_entity_3.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Events", %{event_id: random_event_2_entity_3.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!

    ExAws.Dynamo.delete_item("Index", %{context: random_context, entity_id: random_event_1_entity_1.metadata.entity_id}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Index", %{context: random_context, entity_id: random_event_2_entity_1.metadata.entity_id}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Index", %{context: random_context, entity_id: random_event_1_entity_2.metadata.entity_id}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Index", %{context: random_context, entity_id: random_event_3_entity_1.metadata.entity_id}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Index", %{context: random_context, entity_id: random_event_2_entity_2.metadata.entity_id}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Index", %{context: random_context, entity_id: random_event_1_entity_3.metadata.entity_id}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Index", %{context: random_context, entity_id: random_event_2_entity_3.metadata.entity_id}) |> ExAws.request!

    assert check1a == %{}
    assert check1b == %{}
    assert check1c == %{}
    assert check1d == %{}
    assert check1e == %{}
    assert check1f == %{}
    assert check1g == %{}

    refute check2a == %{} #you don't want to assert the event ID?
    refute check2b == %{} #you don't want to assert the event ID?
    refute check2c == %{} #you don't want to assert the event ID?
    refute check2d == %{} #you don't want to assert the event ID?
    refute check2e == %{} #you don't want to assert the event ID?
    refute check2f == %{} #you don't want to assert the event ID?
    refute check2g == %{} #you don't want to assert the event ID?
  end











  # @tag dothis: true
  test "1 entity lots of events" do
    random_context = Enum.random([:a, :b, :c, :d, :e])

    entity_id_1 = Perhap.Event.get_uuid_v4()
    IO.puts "entity_id_1: #{entity_id_1}"

    random_event_1_entity_1 = make_random_event(%Perhap.Event.Metadata{context: random_context, entity_id: entity_id_1} )
    random_event_2_entity_1 = make_random_event(%Perhap.Event.Metadata{context: random_context, entity_id: entity_id_1} )
    random_event_3_entity_1 = make_random_event(%Perhap.Event.Metadata{context: random_context, entity_id: entity_id_1} )
    IO.puts "random_event_1_entity_1: #{inspect random_event_1_entity_1}\n"
    IO.puts "random_event_2_entity_1: #{inspect random_event_2_entity_1}\n"
    IO.puts "random_event_3_entity_1: #{inspect random_event_3_entity_1}\n"


    #long
    #Use Dynamo#get_event?
    check1a = ExAws.Dynamo.get_item("Events", %{event_id: random_event_1_entity_1.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    check1b = ExAws.Dynamo.get_item("Events", %{event_id: random_event_2_entity_1.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    check1c = ExAws.Dynamo.get_item("Events", %{event_id: random_event_3_entity_1.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    Dynamo.put_event(random_event_1_entity_1)
    :timer.sleep(700)
    Dynamo.put_event(random_event_2_entity_1)
    :timer.sleep(700)
    Dynamo.put_event(random_event_3_entity_1)
    :timer.sleep(700)

    #cleanup
    ExAws.Dynamo.delete_item("Events", %{event_id: random_event_1_entity_1.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Events", %{event_id: random_event_2_entity_1.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Events", %{event_id: random_event_3_entity_1.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!

    assert check1a == %{}
    assert check1b == %{}
    assert check1c == %{}
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

  #markt these with a tag so they show up as ignored when the tests are run?
  test "get_events with entity_id" do
    :ok
  end

  test "get_events without entity_id" do
    :ok
  end

end
