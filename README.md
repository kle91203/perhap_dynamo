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

You can also set your aws credentials using the following config options:

```elixir

config :ex_aws,
  access_key_id: "your-access-key",
  secret_access_key: "your-secret-key"

```

Don't commit that to your source control.  ExAws, the AWS library this one
uses will also accept the environment variables AWS_ACCESS_KEY_ID and
AWS_SECRET_ACCESS_KEY.  So you can also pass in your keys by setting those e.g.

```sh
AWS_ACCESS_KEY_ID="your-access-key" AWS_SECRET_ACCESS_KEY="your-secret-key" mix test
```

See [ExAws](https://github.com/ex-aws/ex_aws) for more AWS options.
