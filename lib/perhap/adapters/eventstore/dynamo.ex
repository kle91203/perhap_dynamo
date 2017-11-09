defmodule Perhap.Adapters.Eventstore.Dynamo do
  use Perhap.Adapters.Eventstore
  use GenServer

  @type t :: [ events: events, index: indexes ]
  @type events  :: %{ required(Perhap.Event.UUIDv1.t) => Perhap.Event.t }
  @type indexes :: %{ required({atom(), Perhap.Event.UUIDv4.t}) => list(Perhap.Event.UUIDv1.t) }

  #@derive [ExAws.Dynamo.Encodable]
  #defstruct [:event]

  #alias __MODULE__

  @spec start_link(opts: any()) ::   {:ok, pid} | :ignore | {:error, {:already_started, pid} | term}
  def start_link(args) do
    GenServer.start_link(__MODULE__, [], name: :eventstore)
  end

  def init(args) do
    interval = 1 #miliseconds
    Process.send_after(self(), {:batch_write, interval}, interval)
    {:ok, []}
  end

  #how do all of these get tied together?
  @spec put_event(event: Perhap.Event.t) :: :ok | {:error, term}
  def put_event(event) do
    IO.puts "#put_event"
             #vvvv--can't use cast?
                   #vvvvvvvvv----the is wired to name: :eventstore in start_link -- :eventstore is the name of this instance of GenServer
    GenServer.call(:eventstore, {:put_event, event})
  end

  #this is one of the GenServer interface methods.
  def handle_call({:put_event, event}, _from, events) do
    IO.puts "#handle_call"
                  #vvvvvvvvvvvvvvv---easy peasy
    {:reply, :ok, [event | events]}
  end

  def handle_info({:batch_write, interval}, []) do
    Process.send_after(self(), {:batch_write, interval}, interval)
    {:noreply, []}
  end

  #“regular” messages sent by functions such Kernel.send/2, Process.send_after/4 and similar, can be handled inside the handle_info/2 callback.
  #Another use case for handle_info/2 is to perform periodic work, with the help of Process.send_after/4
  #the map in send_after matches to what's being received by this method.
  def handle_info({:batch_write, interval}, events) do
    IO.puts "#handle_info"
                           #vvvvvvv----- there is a put_to_dynamo() method
    Task.start(__MODULE__, :put_to_dynamo, [Enum.reverse(events)])
    Process.send_after(self(), {:batch_write, interval}, interval)
    {:noreply, []}
  end

  #in progress? delete?
  @doc """
  def put_event(event) do
    event = %Perhap.Event{event | event_id: event.event_id |> Perhap.Event.uuid_v1_to_time_order}
    ExAws.Dynamo.put_item(Application.get_env(:perhap_dynamo, :event_table_name, "Events"), %{Map.from_struct(event) | metadata: Map.from_struct(event.metadata)})
    |> ExAws.request!

    dynamo_object = ExAws.Dynamo.get_item(Application.get_env(:perhap_dynamo, :event_index_table_name, "Index"), %{context: event.metadata.context, entity_id: event.metadata.entity_id})
    |> ExAws.request!

    indexed_events = case dynamo_object do
      %{"Item" => data} ->
        Map.get(data, "events") |> ExAws.Dynamo.Decoder.decode
      %{} ->
        []
    end

    ExAws.Dynamo.put_item(Application.get_env(:perhap_dynamo, :event_index_table_name, "Index"), %{context: event.metadata.context, entity_id: event.metadata.entity_id, events: [event.event_id | indexed_events]})
    |> ExAws.request!

    :ok
  end
  """


  @spec get_event(event_id: Perhap.Event.UUIDv1) :: {:ok, Perhap.Event.t} | {:error, term}
  def get_event(event_id) do
    #possible to have more than one
    event_id_time_order = event_id |> Perhap.Event.uuid_v1_to_time_order
    #long
    dynamo_object = ExAws.Dynamo.get_item(Application.get_env(:perhap_dynamo, :event_table_name, "Events"), %{event_id: event_id_time_order})
    |> ExAws.request!

    case dynamo_object do
      %{"Item" => result} ->
        metadata = ExAws.Dynamo.decode_item(Map.get(result, "metadata"), as: Perhap.Event.Metadata)
        metadata = %Perhap.Event.Metadata{metadata | context: String.to_atom(metadata.context), type: String.to_atom(metadata.type)}
        #I'm swimming in white space!
        event = ExAws.Dynamo.decode_item(dynamo_object, as: Perhap.Event)

        {:ok, %Perhap.Event{event | event_id: metadata.event_id, metadata: metadata}}
      %{} ->
        {:error, "Event not found"}
    end
  end

  @spec get_events(atom(), [entity_id: Perhap.Event.UUIDv4.t, after: Perhap.Event.UUIDv1.t]) :: {:ok, list(Perhap.Event.t)} | {:error, term}
  def get_events(context, opts \\ []) do
    event_ids = case Keyword.has_key?(opts, :entity_id) do
      true ->
        dynamo_object = ExAws.Dynamo.get_item(Application.get_env(:perhap_dynamo, :event_index_table_name, "Index"), %{context: context, entity_id: opts[:entity_id]})
        |> ExAws.request!

        case dynamo_object do
          %{"Item" => data} ->
            ExAws.Dynamo.Decoder.decode(data)
            |> Map.get("events", [])
          %{} ->
            []
        end
      _ ->
        #wow.... no way to shorten this (arg list)?
        dynamo_object = ExAws.Dynamo.query(Application.get_env(:perhap_dynamo, :event_index_table_name, "Index"),
                                           expression_attribute_values: [context: context],
                                           key_condition_expression: "context = :context")
                        |> ExAws.request!
                        |> Map.get("Items")
                        |> Enum.map(fn x -> ExAws.Dynamo.Decoder.decode(x) end)
                        |> Enum.map(fn x -> Map.get(x, "events") end)
                        |> List.flatten

    end

    if event_ids == [] do
      {:ok, []}
    else
      #method
      event_ids2 = case Keyword.has_key?(opts, :after) do
        true ->
          after_event = time_order(opts[:after])
          event_ids |> Enum.filter(fn ev -> ev > after_event end)
        _ -> event_ids
      end

      event_ids3 = for event_id <- event_ids2, do: [event_id: event_id]

      events = Enum.chunk_every(event_ids3, 100) |> batch_get([])

      {:ok, events}
    end
  end

  defp batch_get([], events) do
    events
  end

  defp batch_get([chunk | rest], event_accumulator) do
    events = ExAws.Dynamo.batch_get_item(%{Application.get_env(:perhap_dynamo, :event_table_name, "Events") => [keys: chunk]})
             |> ExAws.request!
             |> Map.get("Responses")
             |> Map.get("Events")
             |> Enum.map(fn event -> {event, ExAws.Dynamo.decode_item(event["metadata"], as: Perhap.Event.Metadata)} end)
             |> Enum.map(fn {event, metadata} ->
               #holy line wraps batman!
               %Perhap.Event{ExAws.Dynamo.decode_item(event, as: Perhap.Event) | event_id: metadata.event_id,
                                                                                 metadata: %Perhap.Event.Metadata{metadata | context: String.to_atom(metadata.context),
                                                                                                                             type: String.to_atom(metadata.type)}} end)

    batch_get(rest, event_accumulator ++ events)
  end

  defp time_order(maybe_uuidv1) do
    case Perhap.Event.is_time_order?(maybe_uuidv1) do
      true -> maybe_uuidv1
      _ -> maybe_uuidv1 |> Perhap.Event.uuid_v1_to_time_order
    end
  end

  defp decode_data(data) do
    Enum.reduce(data, %{}, fn({key, value}, map) ->
      Map.put(map, String.to_atom(key), value) end)
  end

  #
  def put_to_dynamo(events) do
    IO.puts "\n\nput_to_dynamo"
    IO.puts "--events before"
    Enum.each(events, &IO.puts("   #{inspect &1}"))
    #way too much crap on one line
    events =
      events
      |> Enum.map(
        #make a method for this func
        fn event ->
          %Perhap.Event{event |
                        event_id: event.event_id |> Perhap.Event.uuid_v1_to_time_order,
                        metadata: Map.from_struct(event.metadata)}
                        |> Map.from_struct end)
    IO.puts "\n--events after"
    Enum.each(events, &IO.puts("   #{inspect &1}"))
    IO.puts "\n---chunking---"
    Enum.chunk_every(events, 25) |> batch_put()
  end

  defp batch_put([]) do
    :ok
  end

  #that's not a chunk, that's an event.
  defp batch_put([chunk | rest]) do
    IO.puts "-------start #batch_put------"
    IO.puts "   chunk size: #{length(chunk)}    rest size: #{length(rest)}"
    #keys plural? chunck is an event so it's just one key --- yes, it can have many --- how, since chunk is an event?
    index_keys = chunk
              |> Enum.map(fn event -> %{context: event.metadata.context, entity_id: event.metadata.entity_id} end)
              |> Enum.dedup
    IO.puts "\n--------index_keys--------"
    Enum.each(index_keys, &IO.puts("   #{inspect &1}"))
    IO.puts "--------end index_keys--------\n"

    #extract method
    #so we're getting the existing indexes. then below it looks like we're creating more. is there an index entry for every event per context?
    index = ExAws.Dynamo.batch_get_item(
               #vvvvvv can we pull this out and save it in state or make a function to get it?
              %{Application.get_env(:perhap_dynamo, :event_index_table_name, "Index") => [keys: index_keys]})
              |> ExAws.request!
              |> Map.get("Responses")
              |> Map.get("Index")
                                                         #decode? Convert dynamo format to elixir
                            #extract function
              |> Enum.map(fn index_item -> ExAws.Dynamo.Decoder.decode(index_item) end)
              #extract function
                                                        #an index has an item named context, what else does it have?
              |> Enum.map(fn index_item -> %{index_item | "context" => String.to_atom(index_item["context"])} end)
              #all of this gets put in a variable named index but
              |> Enum.reduce(%{}, fn (index, map) ->
                Map.put(                                   #
                  map,                                     #will this work for my person struct?
                  {index["context"], index["entity_id"]},  # --- i don't think so. it looks like the index is a map. that's not a list of keys.
                  index["events"]) end)                    #
              #unprocessed keys
    IO.puts "\n----------- index -----------------"
    Enum.each(index, &IO.puts("   #{inspect &1}"))
    IO.puts "----------- end index -----------------"

    IO.puts "************************ building index put request **************************************"
    index_put_request = process_index(chunk, index)
    IO.puts "index_put_request:   #{inspect index_put_request}"

    event_put_request = chunk |> Enum.map(fn event -> [put_request: [item: event]] end)

    #extract method
    case ExAws.Dynamo.batch_write_item(%{Application.get_env(:perhap_dynamo, :event_table_name, "Events") => event_put_request}) |> ExAws.request do
      #extract method(s)
      {:error, reason} ->
        IO.puts "Error writing events to dynamo, reason: #{inspect reason}"
        IO.inspect chunk
      _ ->
        case ExAws.Dynamo.batch_write_item(%{Application.get_env(:perhap_dynamo, :event_index_table_name, "Index") => index_put_request}) |> ExAws.request do
          {:error, reason} ->
            IO.puts "Error writing index to dynamo, reason: #{inspect reason}"
            IO.inspect {:events, chunk}
            IO.inspect {:index, index_put_request}

            Enum.each(chunk, fn event -> ExAws.Dynamo.delete_item(Application.get_env(:perhap_dynamo, :event_table_name, "Events"), %{event_id: event.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request! end)
          _ ->
            :ok
        end
    end
    batch_put(rest)
  end

  defp process_index([], index) do
    index
    |> Map.keys
    |> Enum.map(fn {context, entity_id} -> [put_request: [item: %{context: context, entity_id: entity_id, events: Map.get(index, {context, entity_id})}]] end)
                  #^^^^^^^^^^^^^^^^^^^^
                  #this is only one arg, a tuple
  end

  defp process_index([event | rest], index) do
    IO.puts "#process_index:   index:   #{inspect index}"
    index_key = {event.metadata.context, event.metadata.entity_id}
    indexed_events = [event.event_id | Map.get(index, index_key, [])]
    process_index(rest, Map.put(index, index_key, indexed_events))
  end

end
