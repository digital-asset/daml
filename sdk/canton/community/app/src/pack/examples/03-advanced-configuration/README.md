# Advanced Configuration Example

This example directory contains a collection of configuration files that can be used to setup domains or 
participants for various purposes. The directory contains a set of sub-folders:

  - storage: contains "storage mixins" such as [memory.conf](storage/memory.conf) or [postgres.conf](storage/postgres.conf)
  - nodes: contains a set of node defintions for domains and participants
  - api: contains "api mixins" that modify the API behaviour such as binding to a public address or including jwt authorization
  - remote: contains a set of remote node definitions for the nodes in the nodes directory.
  - parameters: contains "parameter mixins" that modify the node behaviour in various ways.
  
## Persistence 

For every setup, you need to decide which persistence layer you want to use. Supported are [memory.conf](storage/memory.conf),
[postgres.conf](storage/postgres.conf) or Oracle (Enterprise). Please [consult the manual](https://docs.daml.com/canton/usermanual/installation.html#persistence-using-postgres)
for further instructions. The examples here will illustrate the usage using the in-memory configuration.

There is a small helper script in [dbinit.py](storage/dbinit.py) which you can use to create the appropriate SQL commands
to create users and databases for a series of nodes. This is convenient if you are setting up a test-network. You can 
run it using:

```
    python3 examples/03-advanced-configuration/storage/dbinit.py \
      --type=postgres --user=canton --pwd=<choose-wisely> --participants=2 --domains=1 --drop
```

Please run the script with ``--help`` to get an overview of all commands. Generally, you would just pipe the output
to your SQL console.

## Nodes

The nodes directory contains a set of base configuration files that can be used together with the mix-ins.

### Domain

Start a domain with the following command:

```
    ./bin/canton -c examples/03-advanced-configuration/storage/memory.conf,examples/03-advanced-configuration/nodes/domain1.conf
```

The domain can be started without any bootstrap script, as it self-initialises by default, waiting for incoming connections.

If you pass in multiple configuration files, they will be combined. It doesn't matter if you separate the 
configurations using `,` or if you pass them with several `-c` options.

NOTE: If you unpacked the zip directory, then you might have to make the canton startup script executable
 (`chmod u+x bin/canton`).

### Participants

The participant(s) can be started the same way, just by pointing to the participant configuration file. 
However, before we can use the participant for any Daml processing, we need to connect it to a domain. You can 
connect to the domain interactively, or use the [initialisation script](participant-init.canton).

```
    ./bin/canton -c examples/03-advanced-configuration/storage/memory.conf \
        -c examples/03-advanced-configuration/nodes/participant1.conf,examples/03-advanced-configuration/nodes/participant2.conf \
        --bootstrap=examples/03-advanced-configuration/participant-init.canton
```

The initialisation script assumes that the domain can be reached via `localhost`, which needs to change if the domain
runs on a different server.

A setup with more participant nodes can be created using the [participant](nodes/participant1.conf) as a template. 
The same applies to the domain configuration. The instance names should be changed (`participant1` to something else), 
as otherwise, distinguishing the nodes in a trial run will be difficult. 

## API 

By default, all the APIs only bind to localhost. If you want to expose them on the network, you should secure them using 
TLS and JWT. You can use the mixins configuration in the ``api`` subdirectory for your convenience.

## Parameters

The parameters directory contains a set of mix-ins to modify the behaviour of your nodes.

- [nonuck.conf](nodes/nonuck.conf) enable non-UCK mode such that you can use multiple domains per participant node (preview). 

## Test Your Setup

Assuming that you have started both participants and a domain, you can verify that the system works by having
participant2 pinging participant1 (the other way around also works). A ping here is just a built-in Daml 
contract which gets sent from one participant to another, and the other responds by exercising a choice.

First, just make sure that the `participant2` is connected to the domain by testing whether the following command 
returns `true`
```
@ participant2.domains.active("mydomain")
```

In order to ping participant1, participant2 must know participant1's `ParticipantId`. You could obtain this from 
participant1's instance of the Canton console using the command `participant1.id` and copy-pasting the resulting 
`ParticipantId` to participant2's Canton console. Another option is to lookup participant1's ID directly using
participant2's console:
```
@ val participant1Id = participant2.parties.list(filterParticipant="participant1").head.participants.head.participant
```
Using the console for participant2, you can now get the two participants to ping each other:
```
@ participant2.health.ping(participant1Id)
```

## Running as Background Process

If you start Canton with the commands above, you will always be in interactive mode within the Canton console. 
You can start Canton as well as a non-interactive process using 
```
    ./bin/canton daemon -c examples/03-advanced-configuration/storage/memory.conf \
                        -c examples/03-advanced-configuration/nodes/participant1.conf \
                        --bootstrap examples/03-advanced-configuration/participant-init.canton
```

## Connect To Remote Nodes

In many cases, the nodes will run in a background process, started as `daemon`, while the user would 
still like the convenience of using the console. This can be achieved by defining remote domains and 
participants in the configuration file. 

A participant or domain configuration can be turned into a remote config using

```
    ./bin/canton generate remote-config -c examples/03-advanced-configuration/storage/memory.conf,examples/03-advanced-configuration/nodes/participant1.conf
```

Then, if you start Canton using
```
    ./bin/canton -c remote-participant1.conf
```
you will have a new instance `participant1`, which will expose most but not all commands
that a node exposes. As an example, run:
```
    participant1.health.status
```

Please note that depending on your setup, you might have to adjust the target ip address.

