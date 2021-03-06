defmodule PerhapDynamo.Mixfile do
  use Mix.Project

  def project do
    [
      app: :perhap_dynamo,
      version: "0.1.0",
      elixir: "~> 1.5",
      start_permanent: Mix.env == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :perhap, :ex_aws, :hackney, :poison]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"},
      {:perhap, github: "perhap/perhap"},
      {:ex_aws, "~> 1.1.5"},
      {:poison, "~> 3.1"},
      {:hackney, "~> 1.6"}
    ]
  end
end
