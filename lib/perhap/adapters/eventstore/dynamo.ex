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
    interval = 100 #miliseconds
    Process.send_after(self(), {:batch_write, interval}, interval)
    {:ok, %{pending: [], posting: []}}
  end

  @spec put_event(event: Perhap.Event.t) :: :ok | {:error, term}
  def put_event(event) do
    GenServer.call(:eventstore, {:put_event, event})
  end

  def handle_call({:put_event, event}, _from, events) do
    {:reply, :ok, %{events | pending: [event | events.pending]}}
  end

  def handle_info({:batch_write, interval}, events = %{pending: []}) do
    Process.send_after(self(), {:batch_write, interval}, interval)
    {:noreply, events}
  end

  def handle_info({:batch_write, interval}, events) do
    chunked = events.pending
    |> Enum.chunk_every(25)
    |> Enum.map(fn chunk -> with {:ok, pid} <- Task.start(__MODULE__, :put_to_dynamo, [chunk])
                            do {pid, chunk}
                            else err -> raise err
                            end
                end)

    Process.send_after(self(), {:batch_write, interval}, interval)
    {:noreply, %{pending: [], posting: [chunked | events.posting]}}
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
    dynamo_object = ExAws.Dynamo.get_item(Application.get_env(:perhap_dynamo, :event_table_name, "Events"), %{event_id: event_id_time_order})
    |> ExAws.request!

    case dynamo_object do
      %{"Item" => result} ->
        metadata = ExAws.Dynamo.decode_item(Map.get(result, "metadata"), as: Perhap.Event.Metadata)
        metadata = %Perhap.Event.Metadata{metadata | context: String.to_atom(metadata.context), type: String.to_atom(metadata.type)}

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
    events = events
             |> Enum.map(fn event -> %Perhap.Event{event | event_id: event.event_id |> Perhap.Event.uuid_v1_to_time_order,
                                                           metadata: Map.from_struct(event.metadata)}
                                     |> Map.from_struct end)

    batch_put(events)
  end

  defp batch_put([]) do
    :ok
  end

  defp batch_put(events) do
    index_keys = make_index_keys(events)

    index = retrieve_index(index_keys)

    index_put_request = process_index(events, index)

    event_put_request = events
                        |> Enum.map(fn event -> [put_request: [item: event]] end)

    do_write_events(events, event_put_request, index_put_request)

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

  defp make_index_keys(events) do
    events
    |> Enum.map(fn event -> %{context: event.metadata.context, entity_id: event.metadata.entity_id} end)
    |> Enum.dedup
  end

  defp retrieve_index(index_keys) do
    ExAws.Dynamo.batch_get_item(%{Application.get_env(:perhap_dynamo, :event_index_table_name, "Index") => [keys: index_keys]})
              |> ExAws.request!
              |> Map.get("Responses")
              |> Map.get("Index")
              |> Enum.map(fn index_item -> ExAws.Dynamo.Decoder.decode(index_item) end)
              |> Enum.map(fn index_item -> %{index_item | "context" => String.to_atom(index_item["context"])} end)
              |> Enum.reduce(%{}, fn (index, map) -> Map.put(map, {index["context"], index["entity_id"]},  index["events"]) end)
              #unprocessed keys
  end

  defp do_write_events(events, event_put_request, index_put_request) do
    case ExAws.Dynamo.batch_write_item(%{Application.get_env(:perhap_dynamo, :event_table_name, "Events") => event_put_request}) |> ExAws.request do
      {:error, reason} ->
        IO.puts "Error writing events to dynamo, reason: #{inspect reason}"
        IO.inspect events
      _ ->
        case ExAws.Dynamo.batch_write_item(%{Application.get_env(:perhap_dynamo, :event_index_table_name, "Index") => index_put_request}) |> ExAws.request do
          {:error, reason} ->
            IO.puts "Error writing index to dynamo, reason: #{inspect reason}"
            IO.inspect {:events, events}
            IO.inspect {:index, index_put_request}

            Enum.each(events, fn event -> ExAws.Dynamo.delete_item(Application.get_env(:perhap_dynamo, :event_table_name, "Events"), %{event_id: event.event_id |> Perhap.Event.uuid_v1_to_time_order}) |> ExAws.request! end)
          _ ->
            :ok
        end
    end
  end

end
