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
    {:ok, pid} = GenServer.start_link(__MODULE__, args)
  end

  @spec put_event(event: Perhap.Event.t) :: :ok | {:error, term}
  def put_event(event) do
    event = %Perhap.Event{event | event_id: event.event_id |> Perhap.Event.uuid_v1_to_time_order}
    ExAws.Dynamo.put_item("Events", %{Map.from_struct(event) | metadata: Map.from_struct(event.metadata)})
    |> ExAws.request!

    dynamo_object = ExAws.Dynamo.get_item("Index", %{context: event.metadata.context, entity_id: event.metadata.entity_id})
    |> ExAws.request!

    indexed_events = case dynamo_object do
      %{"Item" => data} ->
        Map.get(data, "events") |> ExAws.Dynamo.Decoder.decode
      %{} ->
        []
    end
    #IO.inspect indexed_events

    ExAws.Dynamo.put_item("Index", %{context: event.metadata.context, entity_id: event.metadata.entity_id, events: [event.event_id | indexed_events]})
    |> ExAws.request!

    :ok
  end

  @spec get_event(event_id: Perhap.Event.UUIDv1) :: {:ok, Perhap.Event.t} | {:error, term}
  def get_event(event_id) do
    event_id_time_order = event_id |> Perhap.Event.uuid_v1_to_time_order
    dynamo_object = ExAws.Dynamo.get_item("Events", %{event_id: event_id_time_order})
    |> ExAws.request!

    case dynamo_object do
      %{"Item" => result} ->
        metadata = ExAws.Dynamo.decode_item(Map.get(result, "metadata"), as: Perhap.Event.Metadata)
        metadata = %Perhap.Event.Metadata{metadata | context: String.to_atom(metadata.context), type: String.to_atom(metadata.type)}
        #data = ExAws.Dynamo.Decoder.decode(Map.get(result, "data"))

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
        dynamo_object = ExAws.Dynamo.get_item("Index", %{context: context, entity_id: opts[:entity_id]})
        |> ExAws.request!

        case dynamo_object do
          %{"Item" => data} ->
            ExAws.Dynamo.Decoder.decode(data)
            |> Map.get("events", [])
          %{} ->
            []
        end
      _ ->
        dynamo_object = ExAws.Dynamo.query("Index",
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

      #possible this can only do 100 at a time, run through a loop if more
      events = ExAws.Dynamo.batch_get_item(%{"Events" => [keys: event_ids3]})
               |> ExAws.request!
               |> Map.get("Responses")
               |> Map.get("Events")
               |> Enum.map(fn event -> {event, ExAws.Dynamo.decode_item(event["metadata"], as: Perhap.Event.Metadata)} end)
               |> Enum.map(fn {event, metadata} ->
                 %Perhap.Event{ExAws.Dynamo.decode_item(event, as: Perhap.Event) | event_id: metadata.event_id, metadata: %Perhap.Event.Metadata{metadata | context: String.to_atom(metadata.context), type: String.to_atom(metadata.type)}} end)
      {:ok, events}
    end
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
end
