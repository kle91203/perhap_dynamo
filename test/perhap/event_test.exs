defmodule Perhap.EventTestDynamo do
  use ExUnit.Case, async: false #which on requires this?
  import PerhapTest.Helper, only: :functions

  #why test Perhap functionality in the adapter?

  @interval 1
  @pause_interval 500

  setup do
    Perhap.Adapters.Eventstore.Dynamo.start_link([])
    :ok
  end

  #why?
  test "timestamp returns system time in microseconds" do
    assert_in_delta(Perhap.Event.timestamp(), :erlang.system_time(:microsecond), 10)
  end

  test "unique_integer returns monotonically increasing integers" do
    unique_integers = for _n <- 1..10, do: Perhap.Event.unique_integer()
                        #loose equals
                        #why the sort?
    assert unique_integers == Enum.sort(unique_integers |> Enum.dedup)
  end

  test "get_uuid_v1 returns valid uuid_v1" do
            #where did :uuid come from?
            #how do we call a function on an atom?
    assert :uuid.is_v1(Perhap.Event.get_uuid_v1() |> :uuid.string_to_uuid)
  end

  test "knows a uuid_v1 when it sees one" do
    assert Perhap.Event.is_uuid_v1?(make_v1())
  end

  test "flips the time so it can be sorted, and back again" do
    uuid = make_v1()
    [ulow, umid, uhigh, _, _] = String.split(uuid, "-")
    flipped = Perhap.Event.uuid_v1_to_time_order(uuid)
    [fhigh, fmid, flow, _, _] = String.split(flipped, "-")
    double_flipped = Perhap.Event.time_order_to_uuid_v1(flipped)
    refute uuid == flipped                            #I like this. This is a good test. I like the variables names too.
    assert uuid == double_flipped                     #
    assert {ulow, umid, uhigh} == {flow, fmid, fhigh} #
  end

  #mentions "extract datetime" but extract_uuid_v1_time is being called
  test "extract datetime returns the time the event was created" do
    uuid_time = make_v1() |> Perhap.Event.extract_uuid_v1_time
    system_time = System.system_time(:microsecond)
    assert_in_delta(system_time, uuid_time, 100_000)
  end

  test "returns a valid uuid_v4" do
    assert :uuid.is_v4(Perhap.Event.get_uuid_v4() |> :uuid.string_to_uuid())
  end

  test "knows a valid uuid_v4 when it sees one" do
    assert Perhap.Event.is_uuid_v4?(make_v4())
  end

  #ok to have all of these different tests in one test?
  test "invalidates an event" do
    assert Perhap.Event.validate(%{}) == {:invalid, "Invalid event struct"}
    assert Perhap.Event.validate( %Perhap.Event{:metadata => %{}}) == {:invalid, "Invalid event struct"}
    assert Perhap.Event.validate(%Perhap.Event{}) == {:invalid, "Invalid event_id"}
    #long
    assert Perhap.Event.validate(PerhapTest.Helper.make_random_event(%Perhap.Event.Metadata{:entity_id => "not a UUIDv4"})) == {:invalid, "Invalid entity_id"}
    event_with_context_not_set =
      PerhapTest.Helper.make_random_event()
      |> (fn x -> %Perhap.Event{x | metadata: %Perhap.Event.Metadata{x.metadata | context: nil}} end).()
    assert Perhap.Event.validate(event_with_context_not_set) == {:invalid, "Context not set for event"}
  end

  test "validates a valid event" do
    assert Perhap.Event.validate(PerhapTest.Helper.make_random_event()) == :ok
  end

  test "won't save an invalid event" do
    random_event = PerhapTest.Helper.make_random_event(%Perhap.Event.Metadata{:entity_id => ""})
    assert Perhap.Event.save_event(random_event) == {:invalid, "Invalid entity_id"}
  end

  test "saves and retrieves an event" do
    random_event = PerhapTest.Helper.make_random_event()

    response = Perhap.Event.save_event(random_event)
    :timer.sleep(@pause_interval)

    #cleanup
    ExAws.Dynamo.delete_item("Events", %{event_id: random_event.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Index", %{context: random_event.metadata.context, entity_id: random_event.metadata.entity_id}) |> ExAws.request!
    #/cleanup

    assert response == {:ok, random_event}
  end

  test "doesn't retrieve an event that doesn't exist" do
    assert Perhap.Event.retrieve_event(Perhap.Event.get_uuid_v1) == {:error, "Event not found"}
  end

  test "retrieves an event" do
    random_context = Enum.random([:k, :l, :m, :n, :o])
    random_event = make_random_event(
      %Perhap.Event.Metadata{context: random_context} )
    Perhap.Event.save_event(random_event)
    :timer.sleep(@pause_interval)
    result = Perhap.Event.retrieve_event(random_event.event_id)

    #are we ok if the first succeeds and the second fails? Relational databases have transactions.
    #cleanup
    ExAws.Dynamo.delete_item("Events", %{event_id: random_event.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Index", %{context: random_event.metadata.context, entity_id: random_event.metadata.entity_id}) |> ExAws.request!
    #/cleanup

    assert result == {:ok, random_event}
  end

  test "retrieves events by context and entity ID" do
    random_context = Enum.random([:p, :q, :r, :s, :t])
    random_entity_id = Perhap.Event.get_uuid_v4()
    rando1 = make_random_event(
      %Perhap.Event.Metadata{context: random_context, entity_id: random_entity_id} )
    Perhap.Event.save_event(rando1)
    rando2 = make_random_event(
      %Perhap.Event.Metadata{context: random_context, entity_id: random_entity_id} )
    Perhap.Event.save_event(rando2)
    :timer.sleep(@pause_interval)
    results = Perhap.Event.retrieve_events(random_context, entity_id: random_entity_id)

    #at times this clean-up will fail and test records will be left in the datastore. Do they get cleaned out periodically?
    #cleanup
    ExAws.Dynamo.delete_item("Events", %{event_id: rando1.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Events", %{event_id: rando2.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Index", %{context: random_context, entity_id: random_entity_id}) |> ExAws.request!
    #/cleanup

    {:ok, events} = results
    assert Enum.member?(events, rando1)
    assert Enum.member?(events, rando2)
  end

  #better if it saved them under two separate entities?
  test "retrieves events by context" do
    random_context = Enum.random([:u, :v, :w, :x, :y])
    random_entity_id = Perhap.Event.get_uuid_v4()
    rando1 = make_random_event(
      %Perhap.Event.Metadata{context: random_context, entity_id: random_entity_id} )
    Perhap.Event.save_event(rando1)
    :timer.sleep(@pause_interval)
    rando2 = make_random_event(
      %Perhap.Event.Metadata{context: random_context, entity_id: random_entity_id} )
    Perhap.Event.save_event(rando2)
    :timer.sleep(@pause_interval)
    results = Perhap.Event.retrieve_events(random_context)

    #cleanup
    #if we're always flipping back and forth between the two time ordergins why don't we just always use the one we use in the datastore?
    ExAws.Dynamo.delete_item("Events", %{event_id: rando1.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Events", %{event_id: rando2.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    #what does the index table do?
    ExAws.Dynamo.delete_item("Index", %{context: random_context, entity_id: random_entity_id}) |> ExAws.request!
    #/cleanup

    {:ok, events} = results
    assert Enum.member?(events, rando1)
    assert Enum.member?(events, rando2)
  end

  test "returns an empty list if events don't exist" do
    assert Perhap.Event.retrieve_events(:z) == {:ok, []}
    random_entity_id = Perhap.Event.get_uuid_v4()
    rando = make_random_event(
      %Perhap.Event.Metadata{context: :z, entity_id: random_entity_id} )
    Perhap.Event.save_event(rando)
    :timer.sleep(@pause_interval)
    check1 = Perhap.Event.retrieve_events(:z, entity_id: Perhap.Event.get_uuid_v4)
    result1 = Perhap.Event.retrieve_events(:z)
    result2 = Perhap.Event.retrieve_events(:z, entity_id: random_entity_id)

    #cleanup
    ExAws.Dynamo.delete_item("Events", %{event_id: rando.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request!
    ExAws.Dynamo.delete_item("Index", %{context: rando.metadata.context, entity_id: random_entity_id}) |> ExAws.request!
    #/cleanup
    {:ok, [event1]} = result1
    {:ok, [event2]} = result2

    assert check1 == {:ok, []}
    assert event1.event_id == rando.event_id
    assert event2.event_id == rando.event_id

  end


  test "retrieves events following an event for an entity" do
    #nice! I like tests like this that show the state before the test values are put in
    assert Perhap.Event.retrieve_events(:aa) == {:ok, []}
    #rename to random_entity_id?
    random_entity = Perhap.Event.get_uuid_v4
    #returns a list??
    random_event_ids = for _ <- 1..5, do: Perhap.Event.get_uuid_v1
    random_events = random_event_ids
                                 #renmae to ev_id?
                    |> Enum.map(fn(ev) ->
                      make_random_event(%Perhap.Event.Metadata{context: :aa, event_id: ev, entity_id: random_entity})
                    end)
    random_events |> Enum.each(fn(event) -> Perhap.Event.save_event(event) end)
    :timer.sleep(@pause_interval)
    [ ev1 | [ ev2 | rest ] ] = random_events |> Enum.reverse
    results = Perhap.Event.retrieve_events(:aa, entity_id: random_entity, after: ev2.event_id)

    #cleanup
    Enum.map(random_events, fn event -> ExAws.Dynamo.delete_item("Events", %{event_id: event.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request! end)
    ExAws.Dynamo.delete_item("Index", %{context: :aa, entity_id: random_entity}) |> ExAws.request!
    #/cleanup

    {:ok, events} = results
    assert Enum.member?(events, ev1)
    Enum.map(rest, fn event -> refute Enum.member?(events, event) end)



  end

  #do we need this?
  @tag :pending
  test "retrieves events filtered by an event_type" do
  end

  defp make_v1() do
    {uuid, _} = :uuid.get_v1(:uuid.new(self()))
    uuid |> :uuid.uuid_to_string |> to_string()
  end

  defp make_v4() do
    :uuid.get_v4() |> :uuid.uuid_to_string |> to_string()
  end

end
