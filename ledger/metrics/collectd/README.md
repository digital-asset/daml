# Collectd metrics


## Collectd installation - macOS

First, check whether the directory `/usr/local/sbin` exists on your machine.
This directory does not exist on MacOs by default, and homebrew will try to link
collectd into this directory. If it does not exist, run:

```
sudo mkdir /usr/local/sbin
sudo chown -R `whoami`:admin /usr/local/sbin
```

To install collectd, run:

```
brew install libpq
brew install --build-from-source collectd
```

Note: The collectd bottle does not contain the postgresql plugin.
You therefore need to install `libpq` first, then build collectd from source.

If everything went well, you should have the following file on your machine:

```
/usr/local/Cellar/collectd/5.11.0/lib/collectd/postgresql.so
```

## Collectd configuration

Once installed, replace `/usr/local/etc/collectd.conf` with the [file from this directory](./collectd.conf)

## Postgres configuration

To allow collectd to monitor PostgreSQL,
create a `collectd` user and grant it priviledges to your databases.

```
psql -d postgres
# create user collectd with encrypted password 'collectd';
# grant all privileges on database damlsandbox to collectd;
# grant all privileges on database damlsandboxindex to collectd;
```

Note: this assumes your databaseses are called `damlsandbox` and `damlsandboxindex`.
If you want to monitor different databases, change also the corresponding entries in `collectd.conf`.

## Running collectd

To run collectd as a foreground process, run:

```
sudo /usr/local/sbin/collectd -f -C /usr/local/etc/collectd.conf
```
