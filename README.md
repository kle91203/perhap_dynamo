# PerhapDynamo

Perhap Dynamo is a persistance adapter for [Perhap](https://github.com/perhap/perhap).

# Current Status

Perhap dynamo is a work in progress.  It can store events and will store models.

## Installation

Perhap Dynamo can be added to your project by adding `perhap_dynamo`
to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    #...
    {:perhap, github: "perhap/perhap"},
    {:perhap_dynamo, github: "perhap/perhap_dynamo"}
  ]
end
```

You should then add perhap and perhap_dynamo to your applications in `mix.exs`:

```elixir
# Run "mix help compile.app" to learn about applications.
def application do
  [
    applications: [:perhap, :perhap_dynamo],
    extra_applications: [:logger],
    #...
  ]
end
```

Perhap Dynamo can then be configured like the following in `config/config.exs`:

```elixir

config :perhap,
  port: 9000,
  eventstore: Perhap.Adapters.Eventstore.Dynamo,
  modelstore: Perhap.Adapters.Modelstore.Memory

config :logger,
  backends: [:console],
  compile_time_purge_level: :info,
  level: :warn

config :swarm,
  sync_nodes_timeout: 10
```
