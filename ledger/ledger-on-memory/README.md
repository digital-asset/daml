# Overview

This document is to help internal engineers work with `ledger-on-memory`.

`ledger-on-memory` is a key/value-based ledger that uses a simple in-memory map as an
underlying storage. It uses either H2 or PostgreSQL as its index database.

# Ledger On Memory

To build a fat JAR with the server built from HEAD run

    bazel build //ledger/ledger-on-memory:app_deploy.jar

The application can be run from command line with the following command:

    java -Xmx4G -XX:+UseG1GC -jar bazel-bin/ledger/ledger-on-memory/app_deploy.jar --participant participant-id=foo,port=6861

As run from the main project root directory (adjust the location of the JAR according to 
your working directory).

Alternatively, the application can be run using the Bazel command:

    bazel run //ledger/ledger-on-memory:app -- --participant participant-id=foo,port=6861

## Docker

Ledger On Memory can be put into a Docker container by first building it:

    bazel build ledger/ledger-on-memory:app-image

Then by running the Bazel target that imports the image into your local Docker repository, and then
runs a container from that image.

    bazel run ledger/ledger-on-memory:app-image
    
The container will fail to run, but the image has now been loaded into your local Docker
repository under the name `bazel/ledger/ledger-on-memory`.

It can then be run using Docker:

    docker run \
        -it \
        --rm \
        --name ledger \
        --network docker_default \
        -p 6861:6865 \
        bazel/ledger/ledger-on-memory:app-image \
            --participant participant-id=foo,port=6865,address=0.0.0.0
            
The above command makes the ledger available to clients from the host machine at the address 
`localhost:6861`. It is also available to other docker containers on the `docker_default`
network under `ledger:6865`.

## Creating ledger dumps

Ledger On Memory can be used to generate ledger dumps through an environment variable:

    export KVUTILS_LEDGER_DUMP=/path/to/dump/file

Then launch the ledger using the Bazel or Java command as described above.

In case the ledger is run from within the Docker container, use following syntax:

    export KVUTILS_LEDGER_DUMP=/path/to/dump/directory"
    docker run \
            -it \
            --rm \
            --name ledger \
            --network docker_default \
            -p 6861:6865 \
            -v ${KVUTILS_LEDGER_DUMP}:/dump \
            -e KVUTILS_LEDGER_DUMP=/dump/my-ledger.dump \
            bazel/ledger/ledger-on-memory:app-image \
                --participant participant-id=foo,port=6865,address=0.0.0.0
