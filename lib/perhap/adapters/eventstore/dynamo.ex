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

  @spec put_event(event: Perhap.Event.t) :: :ok | {:error, term}
  def put_event(event) do
    IO.puts "#put_event"
             #vvvv--can't use cast? -- no. cast is like UDP, no guarantee it was received or processed.
    GenServer.call(:eventstore, {:put_event, event})
  end

  def handle_call({:put_event, event}, _from, events) do
    IO.puts "#handle_call"
    {:reply, :ok, [event | events]}
  end

  def handle_info({:batch_write, interval}, []) do
    Process.send_after(self(), {:batch_write, interval}, interval)
    {:noreply, []}
  end

  def handle_info({:batch_write, interval}, events) do
    IO.puts "#handle_info"
    Task.start(__MODULE__, :put_to_dynamo, [Enum.reverse(events)])
    Process.send_after(self(), {:batch_write, interval}, interval)
    {:noreply, []}
  end

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

  def put_to_dynamo(events) do
    #way too much crap on one line
    events = events |> Enum.map( fn event -> %Perhap.Event{event | event_id: event.event_id |> Perhap.Event.uuid_v1_to_time_order, metadata: Map.from_struct(event.metadata)} |> Map.from_struct end)
    Enum.chunk_every(events, 25) |> batch_put()
  end

  defp batch_put([]) do
    :ok
  end

  defp batch_put([chunk | rest]) do
    index_keys = chunk
              |> Enum.map(fn event -> %{context: event.metadata.context, entity_id: event.metadata.entity_id} end)
              |> Enum.dedup
    Enum.each(index_keys, &IO.puts("   #{inspect &1}"))

    #extract method
    index = ExAws.Dynamo.batch_get_item(
               #can we pull this out and save it in state or make a function to get it?
              %{Application.get_env(:perhap_dynamo, :event_index_table_name, "Index") => [keys: index_keys]})
              |> ExAws.request!
              |> Map.get("Responses")
              |> Map.get("Index")
              |> Enum.map(fn index_item -> ExAws.Dynamo.Decoder.decode(index_item) end)
              #extract function
              |> Enum.map(fn index_item -> %{index_item | "context" => String.to_atom(index_item["context"])} end)
              |> Enum.reduce(%{}, fn (index, map) ->
                Map.put(
                  map,
                  {index["context"], index["entity_id"]},
                  index["events"]) end)

    index_put_request = process_index(chunk, index)

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
  end

  defp process_index([event | rest], index) do
    index_key = {event.metadata.context, event.metadata.entity_id}
    indexed_events = [event.event_id | Map.get(index, index_key, [])]
    process_index(rest, Map.put(index, index_key, indexed_events))
  end

end
