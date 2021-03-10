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

## Customizing Grafana

The easiest way to customize Grafana is through the web ui.
Changes will be persisted to the Grafana db and remain effective after restarts.

You can also import dashboards programmatically:
1. Export the dashboard to a json file through the Grafana UI. (Go to the dashboard, share, export, ...)
2. Put the dashboard file into `./grafana/dashboards`
3. Restart: `docker-compose restart`.

