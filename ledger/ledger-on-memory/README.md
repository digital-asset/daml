# Overview
This document is to help internal engineers work with the ledger-on-memory.
Ledger on memory is a kv-based ledger that uses a simple in-memory map as an underlying
storage, it uses either H2 or Postgres as its index db.

# Ledger On Memory

To build a fat JAR with the server built from HEAD run

    bazel build //ledger/ledger-on-memory:app_deploy.jar

ledger-on-memory application can be run from command line with the following command:

    java -Xmx4G -XX:+UseG1GC -jar bazel-bin/ledger/ledger-on-memory/app_deploy.jar --participant participant-id=foo,port=6865

as run from the main project root directory (adjust the location of the JAR according to 
your working directory).

## Docker

Ledger on memory can be put into a docker container by first building it

    bazel build ledger/ledger-on-memory:app-image

Then by running the bazel target that registers it in the local docker repository

    bazel run ledger/ledger-on-memory:app-image
    
The container will be registered under a name `bazel/ledger/ledger-on-memory`.
It can be then run using docker:

    docker run \
        -it \
        --rm \
        --name ledger
        --network docker_default \
        -p 6861:6865 
        bazel/ledger/ledger-on-memory:app-image \
            --participant participant-id=blah,port=6865,address=0.0.0.0
            
 Above command maked the ledger available to clients from the host machine on the 
 `localhost:6861`. It is also available to other docker containers on the `docker_default`
 network under `ledger:6865`.