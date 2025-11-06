# BFT Sequencers - Standalone + Observability Example

Deployment Example of 4 standalone BFT sequencers based on the [Canton Observability Example].

Standalone deployments are exclusively meant for performance testing purposes and currently only support a fixed,
pre-configured topology.

Note that, in standalone mode, each sequencer is deployed as its own Canton single-sequencer + single-mediator
synchronizer, but gRPC services for writing and reading are directly exposed on the P2P ports and
data exchange with the sequencer runtime is disabled.

The presence of mediators has the sole purpose of successfully bootstrapping the synchronizers, which seems to be
the easiest way to get the sequencers fully initialized, so that they start their orderer component.
The mediator component takes up some limited amount of memory, as it is never actually used.

The standalone portions of the Canton sequencer configurations have been generated using
[`GenStandaloneConfig`].

[`GenStandaloneConfig`]: ../../../../../../../../../../../../../../../community/synchronizer/src/test/scala/com/digitalasset/canton/synchronizer/sequencer/block/bftordering/performance/GenStandaloneConfig.scala
[Canton Observability Example]: ../../../../../../../../../../../../../../../community/app/src/test/resources/examples/13-observability

## 🚦 Prerequisites 🚦

* [**Docker**](https://docs.docker.com/get-docker/).
* [**Docker Compose V2 (as plugin `2.x`)**](https://github.com/docker/compose).
* Build Canton as follows:
  * `sbt packRelease`
  * `cd community/app/target/release/canton`
  * Copy [the `Dockerfile` from the observability example] there.
  * `docker build . -t canton-community:latest` (or anything matching your [.env](.env)'s `CANTON_IMAGE`).

⚠️ **Docker compose V1 is deprecated and incompatible with this project**, check [Docker documentation](https://docs.docker.com/compose/migrate/).
One sign you are using the wrong version is the command syntax with a dash instead of a space:
`docker compose` (V2 ✔️) VS `docker-compose` (V1 ❌).

[the `Dockerfile` from the observability example]: ../../../../../../../../../../../../../../../community/app/src/test/resources/examples/13-observability/canton/Dockerfile

## Quickstart

To quickly get up and running, **make sure you have all the prerequisites installed** and then:

* Ensure you have enough CPU/RAM/disk to run this project; if resource limits are reached, a container can be killed.
Canton can use over 4GB of RAM for example.
* Start everything: `docker compose up`
* Create workload: use the `DaBftBenchmarkTool`.
* Log in to the Grafana at [http://localhost:3000/](http://localhost:3000/) using the default
user and password `digitalasset`. After you open any dashboard, you can lower the time range to 5 minutes and
refresh to 10 seconds to see results quickly.
* After you stop, you must [cleanup everything](#cleanup-everything) and start fresh next time:
`docker compose down -v`

See the [Canton Observability Example] for more details on the deployment.

[Canton Observability Example]: ../../../../../../../../../../../../../../../community/app/src/test/resources/examples/13-observability
