---
title: Configuration
weight: 500
---
# Configuring Grafana Loki

Grafana Loki is configured in a YAML file (usually referred to as `loki.yaml` )
which contains information on the Loki server and its individual components,
depending on which mode Loki is launched in.

Configuration examples can be found in the [Configuration Examples](examples/) document.

## Printing Loki Config At Runtime

If you pass Loki the flag `-print-config-stderr` or `-log-config-reverse-order`, (or `-print-config-stderr=true`)
Loki will dump the entire config object it has created from the built in defaults combined first with
overrides from config file, and second by overrides from flags.

The result is the value for every config object in the Loki config struct, which is very large...

Many values will not be relevant to your install such as storage configs which you are not using and which you did not define,
this is expected as every option has a default value if it is being used or not.

This config is what Loki will use to run, it can be invaluable for debugging issues related to configuration and
is especially useful in making sure your config files and flags are being read and loaded properly.

`-print-config-stderr` is nice when running Loki directly e.g. `./loki ` as you can get a quick output of the entire Loki config.

`-log-config-reverse-order` is the flag we run Loki with in all our environments, the config entries are reversed so
that the order of configs reads correctly top to bottom when viewed in Grafana's Explore.

## Configuration Reference

For a complete reference documentation of Loki's configuration, refer to [Configuration Reference]({{< relref "reference.md" >}}).

## Runtime Configuration file

Loki has a concept of "runtime config" file, which is simply a file that is reloaded while Loki is running. It is used by some Loki components to allow operator to change some aspects of Loki configuration without restarting it. File is specified by using `-runtime-config.file=<filename>` flag and reload period (which defaults to 10 seconds) can be changed by `-runtime-config.reload-period=<duration>` flag. Previously this mechanism was only used by limits overrides, and flags were called `-limits.per-user-override-config=<filename>` and `-limits.per-user-override-period=10s` respectively. These are still used, if `-runtime-config.file=<filename>` is not specified.

At the moment, two components use runtime configuration: limits and multi KV store.

Options for runtime configuration reload can also be configured via YAML:

```yaml
# Configuration file to periodically check and reload.
[file: <string>: default = empty]

# How often to check the file.
[period: <duration>: default 10s]
```

Example runtime configuration file:

```yaml
overrides:
  tenant1:
    ingestion_rate_mb: 10
    max_streams_per_user: 100000
    max_chunks_per_query: 100000
  tenant2:
    max_streams_per_user: 1000000
    max_chunks_per_query: 1000000

multi_kv_config:
    mirror-enabled: false
    primary: consul
```

## Accept out-of-order writes

Since the beginning of Loki, log entries had to be written to Loki in order
by time.
This limitation has been lifted.
Out-of-order writes are enabled globally by default, but can be disabled/enabled
on a cluster or per-tenant basis.

- To disable out-of-order writes for all tenants,
place in the `limits_config` section:

    ```
    limits_config:
        unordered_writes: false
    ```

- To disable out-of-order writes for specific tenants,
configure a runtime configuration file:

    ```
    runtime_config: overrides.yaml
    ```

    In the `overrides.yaml` file, add `unordered_writes` for each tenant
    permitted to have out-of-order writes:

    ```
    overrides:
      "tenantA":
        unordered_writes: false
    ```

How far into the past accepted out-of-order log entries may be
is configurable with `max_chunk_age`.
`max_chunk_age` defaults to 1 hour.
Loki calculates the earliest time that out-of-order entries may have
and be accepted with

```
time_of_most_recent_line - (max_chunk_age/2)
```

Log entries with timestamps that are after this earliest time are accepted.
Log entries further back in time return an out-of-order error.

For example, if `max_chunk_age` is 2 hours
and the stream `{foo="bar"}` has one entry at `8:00`,
Loki will accept data for that stream as far back in time as `7:00`.
If another log line is written at `10:00`,
Loki will accept data for that stream as far back in time as `9:00`.
