
#what do these do?

defmodule Fixture do
  import Perhap
  use Perhap, app: :fixture, port: 4499

  context :test,
    domain1: [
      single: Fixture.Domain1, events: [:domain1event] ],
    domain2: [ model: Fixture.Domain2,
               events: [:domain1event, :domain2event1, :domain2event2] ]
  context :test2,
    domain1: [ single: Fixture.Domain3, events: [:domain3event] ]
end

defmodule Fixture.Domain1 do
  use Perhap.Domain

  defstruct value: 0

  def reducer(_event_type, %__MODULE__{value: v}, _event) do
    {%__MODULE__{value: v + 1}, []}
  end
end

defmodule Fixture.Domain2 do
  use Perhap.Domain

  defstruct value: 0

  def reducer(:domain1event, %__MODULE__{value: v}, _event) do
    {%__MODULE__{value: 2 * (v + 1)}, []}
  end
  def reducer(:domain2event1, %__MODULE__{value: v}, _event) do
    {%__MODULE__{value: v + 1}, []}
  end
  def reducer(:domain2event2, %__MODULE__{value: v}, _event) do
    {%__MODULE__{value: v - 1}, []}
  end
end

defmodule Fixture.Domain3 do
  use Perhap.Domain

  defstruct value: 0

  def reducer(_event_type, %__MODULE__{value: v}, _event) do
    {%__MODULE__{value: v}, [] }
  end
end
