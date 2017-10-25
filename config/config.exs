# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

# This configuration is loaded before any dependency and is restricted
# to this project. If another project depends on this project, this
# file won't be loaded nor affect the parent project. For this reason,
# if you want to provide default values for your application for
# 3rd-party users, it should be done in your "mix.exs" file.

# You can configure your application as:
#
#     config :perhap_dynamo, key: :value
#
# and access this configuration in your application as:
#
#     Application.get_env(:perhap_dynamo, :key)
#
# You can also configure a 3rd-party app:
#
#     config :logger, level: :info
#

# It is also possible to import configuration files, relative to this
# directory. For example, you can emulate configuration per environment
# by uncommenting the line below and defining dev.exs, test.exs and such.
# Configuration from the imported file will override the ones defined
# here (which is why it is important to import them last).
#
#     import_config "#{Mix.env}.exs"


config :perhap,
  eventstore: Perhap.Adapters.Eventstore.Dynamo,
  modelstore: Perhap.Adapters.Modelstore.Memory

config :ssl, protocol_version: :"tlsv1.2"

config :logger,
  backends: [:console],
  utc_log: true,
  compile_time_purge_level: :debug,
  level: :error

config :logger, :access_log,
  metadata: [:application, :module, :function],
  level: :info

config :logger, :error_log,
  metadata: [:application, :module, :function, :file, :line],
  level: :error

config :libcluster,
  topologies: [
    perhap: [ strategy: Cluster.Strategy.Epmd,
              config: [hosts: [:"perhap1@127.0.0.1", :"perhap2@127.0.0.1", :"perhap3@127.0.0.1"] ]]
  ]

config :swarm, sync_nodes_timeout: 1_000 #,debug: true
