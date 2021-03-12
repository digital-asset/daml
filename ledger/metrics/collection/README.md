## Basic Usage

1. Launch Grafana & Graphite:

   ```
   $ docker-compose up -d
   ```

2. Explore available dashboards:

   ```
   $ open http://localhost:3000/

   # login with admin/admin
   ```

3. Power off when done:

   ```
   $ docker-compose down
   ```

## Fresh Restart

This will purge all data and configuration changes, and restart Graphite and Grafana:
```
$ reset-all.sh
```

## Customizing Graphite

After startup, you can explore and adjust the current Graphite configuration in `./graphite/conf`.
Configuration changes remain effective after restarts, but they will be purged by `reset-all.sh`.
At the first start, i.e., if the configuration directory is empty, Graphite will reset the configuration to the default.

If you want to change the default configuration, put your adjusted configuration files into `./graphite/default_conf`.
Changes to the default configuration will not be purged by `reset-all.sh`. 

The following changes have been made to the default configuration:
- `storage-aggregation.conf`: Removed a section that would aggregate metrics ending in `.count` using `sum`.
  Such metrics will be aggregated by using the `avg` function instead.
  **Rationale:** Codahale `Meter`s and `Counter`s will report metrics ending in `.count` containing the total number of events measured.
  Such metrics need to be aggregated with the `avg` function, as they reflect the total number of events (as opposed to the delta w.r.t. the last report).

- `storage-aggregation.conf`: Changed the default `xFilesFactor` from `0.3` to `0.0`.
  **Rationale:** If a metric is reported only once per minute, Graphite would discard the metric values after the first aggregation step.

- `storage-schemas.conf`: Changed the default retention schedule to `10s:7d,1m:30d,10m:1800d`.

## Customizing Grafana

The easiest way to customize Grafana is through the web ui.
Changes will be persisted to the Grafana db and remain effective after restarts.

You can also import dashboards programmatically:
1. Export the dashboard to a json file through the Grafana UI. (Go to the dashboard, share, export, ...)
2. Put the dashboard file into `./grafana/dashboards`
3. Restart: `docker-compose restart`.

