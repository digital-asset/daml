// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins.toxiproxy

import com.digitalasset.canton.config.*
import com.digitalasset.canton.integration.ConfigTransforms.*
import com.digitalasset.canton.integration.plugins.toxiproxy.ProxyConfig.postgresConfig
import com.digitalasset.canton.integration.plugins.toxiproxy.UseToxiproxy.*
import com.digitalasset.canton.integration.{ConfigTransform, EnvironmentSetupPlugin}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.{BaseTest, SequencerAlias, UniquePortGenerator}
import eu.rekawek.toxiproxy.ToxiproxyClient
import monocle.macros.syntax.lens.*
import org.testcontainers.Testcontainers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy
import org.testcontainers.utility.DockerImageName

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

/** Test plugin for using toxiproxy to simulate network failures.
  *
  * Tests using this plugin must include the tag `ToxiproxyTest`
  *
  * If you want to use this alongside the `UsePostgres` plugin, make sure you register the
  * `UseToxiproxy` plugin second so that it can use the transformations from the `UsePostgres`
  * plugin
  */
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
final case class UseToxiproxy(toxiproxyConfig: ToxiproxyConfig)
    extends EnvironmentSetupPlugin
    with BaseTest {
  private val TOXIPROXY_CONTROL_PORT: Int = 8474
  var runningToxiproxy: RunningToxiproxy = _
  var toxiContainer: ToxiproxyContainer = _

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig = {
    val ci = sys.env.contains("CI") && !sys.env.contains("MACHINE")
    setup(config, ci)
  }

  def setup(config: CantonConfig, ci: Boolean): CantonConfig = {

    val instanceConfigs = toxiproxyConfig.proxies.map { proxyConfig =>
      proxyConfig.generate(config)
    }

    val client = if (!ci) {
      logger.info(s"Setup UseToxiproxy to run locally")
      toxiContainer = new ToxiproxyContainer()

      // Expose the control port
      toxiContainer.addExposedPorts(TOXIPROXY_CONTROL_PORT)

      toxiContainer.setWaitStrategy(
        new HttpWaitStrategy().forPath("/version").forPort(TOXIPROXY_CONTROL_PORT)
      )

      val hostPorts = (instanceConfigs.map(c => c.upstreamPort.unwrap))

      for (i <- PROXIED_PORTS) {
        toxiContainer.addExposedPorts(i)
      }

      Testcontainers.exposeHostPorts(hostPorts*)

      toxiContainer.start()

      new ToxiproxyClient(
        toxiContainer.getHost,
        toxiContainer.getMappedPort(TOXIPROXY_CONTROL_PORT),
      )
    } else {
      logger.info(s"Setup UseToxiproxy to run in ci")
      new ToxiproxyClient()
    }

    // Wait for a query to toxiproxy to be successful
    eventually(timeUntilSuccess = 60.seconds) {
      try {
        val version = client.version()
        logger.info(s"Starting toxiproxy with version $version")
      } catch {
        case ex: Throwable => fail(ex)
      }
    }

    val proxies = instanceConfigs.foldLeft(Map.empty[String, RunningProxy]) {
      case (map, instance) =>
        val proxyPort: Int =
          if (ci) {
            // Use unique ports in ci as toxiproxy runs in a sidecar container on the same network
            UniquePortGenerator.next.unwrap
          } else {
            // Use statically chosen ports locally as toxiproxy runs in an isolated docker container
            PROXIED_PORTS(numberOfProxies.getAndIncrement())
          }

        val listen =
          if (ci) s"localhost:$proxyPort"
          else
            // When running locally (in a docker container), toxiproxy listens on the default route as requests from the host machine will
            // not appear on localhost
            s"0.0.0.0:$proxyPort"

        val up =
          if (ci) s"${instance.upstreamHost}:${instance.upstreamPort}"
          else {
            val upstreamHost = instance.upstreamHost
            // From the container's perspective, the address of the host is not local host but is instead `CONTAINER_HOST`
            val upstreamHostFromDocker: String =
              if (LOCAL_HOST_NAMES.contains(upstreamHost)) CONTAINER_HOST else upstreamHost

            s"$upstreamHostFromDocker:${instance.upstreamPort}"
          }
        logger.info(s"Create proxy from $listen to $up")
        val proxy = client.createProxy(instance.name, listen, up)
        val running = new RunningProxy(proxy, Option(toxiContainer), client)
        map + (instance.name -> running)
    }

    logger.info(s"Updating canton config to use the proxies")
    val updates = instanceConfigs
      .flatMap(c => routeThroughProxy(c, proxies))

    val transformedConfig = updates.foldLeft(config) { case (config, update) => update(config) }

    runningToxiproxy = new RunningToxiproxy(client, proxies, loggerFactory)

    logger.info(s"Running the startup function")
    toxiproxyConfig.runOnStart(runningToxiproxy)

    logger.info(s"Toxiproxy is running")
    transformedConfig
  }

  def routeThroughProxy(
      cfg: ProxyInstanceConfig,
      proxies: Map[String, RunningProxy],
  ): List[ConfigTransform] = {
    cfg match {

      case ParticipantAwsKmsInstanceConfig(
            name,
            upstreamHost,
            upstreamPost,
            from,
          ) =>
        val proxy = tryProxy(proxies, name)

        // Update the participant config to use AWS KMS through the proxy
        List(
          updateParticipantConfig(from.participant)(cfg =>
            cfg.focus(_.crypto.kms).modify {
              case Some(aws: KmsConfig.Aws) =>
                Some(
                  aws.copy(
                    endpointOverride =
                      Some("https://" + proxy.ipFromHost + ":" + proxy.portFromHost),
                    disableSslVerification = true,
                  )
                )
              case other =>
                other
            }
          )
        )

      case ParticipantGcpKmsInstanceConfig(
            name,
            upstreamHost,
            upstreamPost,
            from,
          ) =>
        val proxy = tryProxy(proxies, name)

        // Update the participant config to use GCP KMS through the proxy
        List(
          updateParticipantConfig(from.participant)(cfg =>
            cfg.focus(_.crypto.kms).modify {
              case Some(gcp: KmsConfig.Gcp) =>
                Some(
                  gcp.copy(
                    endpointOverride = Some(proxy.ipFromHost + ":" + proxy.portFromHost)
                  )
                )
              case other =>
                other
            }
          )
        )

      case ParticipantPostgresInstanceConfig(
            name,
            upstreamHost,
            upstreamPost,
            from,
            dbName,
            postgres,
            dbTimeoutMillis,
          ) =>
        val proxy = tryProxy(proxies, name)

        // Update the participant config to use the database through the proxy
        List(
          updateParticipantConfig(from.participant)(cfg =>
            cfg
              .focus(_.storage)
              .replace {
                postgresConfig(dbName, proxy, dbTimeoutMillis, postgres)
              }
          )
        )

      case MediatorPostgresInstanceConfig(
            name,
            upstreamHost,
            upstreamPost,
            from,
            dbName,
            postgres,
            dbTimeoutMillis,
          ) =>
        val proxy = tryProxy(proxies, name)

        // Update the mediator config to use the database through the proxy
        List(
          updateMediatorConfig(from.mediator)(cfg =>
            cfg.focus(_.storage).replace {
              postgresConfig(dbName, proxy, dbTimeoutMillis, postgres)
            }
          )
        )

      case SequencerPostgresInstanceConfig(
            name,
            upstreamHost,
            upstreamPort,
            from,
            dbName,
            postgres,
            dbTimeoutMillis,
          ) =>
        val proxy = tryProxy(proxies, name)

        List(
          updateSequencerConfig(from.sequencer)(cfg =>
            cfg.focus(_.storage).replace {
              postgresConfig(dbName, proxy, dbTimeoutMillis, postgres)
            }
          )
        )

      case _: BasicProxyInstanceConfig => List.empty
    }
  }

  private def tryProxy(proxies: Map[String, RunningProxy], name: String): RunningProxy =
    proxies.getOrElse(
      name,
      throw new RuntimeException(
        s"Could not get proxy with name $name from set of proxies $proxies"
      ),
    )

  override def afterEnvironmentDestroyed(config: CantonConfig): Unit = {
    logger.info(s"Closing UseToxiproxy container")
    if (toxiContainer != null) {
      toxiContainer.close()
      toxiContainer = null
    }
  }

}

class RunningToxiproxy(
    client: ToxiproxyClient,
    proxies: Map[String, RunningProxy],
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  def getProxy(name: String): Option[RunningProxy] = proxies.get(name)

  val controllingToxiproxyClient: ToxiproxyClient = client
}

object UseToxiproxy {
  val LOCAL_HOST_NAMES = List("127.0.0.1", "localhost")
  val CONTAINER_HOST = "host.testcontainers.internal"

  final case class ToxiproxyConfig(
      proxies: Seq[ProxyConfig],
      runOnStart: RunningToxiproxy => Unit = _ => (),
  )

  class ToxiproxyContainer
      extends GenericContainer(DockerImageName.parse(s"ghcr.io/shopify/toxiproxy:2.1.5"))

  def generateSynchronizerConnectionConfig(
      config: SynchronizerConnectionConfig,
      proxyConf: ParticipantToSequencerPublicApi,
      toxiproxy: RunningToxiproxy,
  ): SynchronizerConnectionConfig = {
    val proxy = toxiproxy
      .getProxy(proxyConf.name)
      .getOrElse(
        throw new RuntimeException(
          s"Cannot find proxy with name " +
            s"${proxyConf.name}"
        )
      )
    val connection = s"http://${proxy.ipFromHost}:${proxy.portFromHost}"
    SynchronizerConnectionConfig.grpc(
      SequencerAlias.Default,
      config.synchronizerAlias,
      connection,
      config.manualConnect,
      config.synchronizerId,
      None,
      config.priority,
      config.initialRetryDelay,
      config.maxRetryDelay,
      config.timeTracker,
    )
  }

  val numberOfProxies: AtomicInteger = new AtomicInteger(0)

  // Support up to 100 toxiproxy-proxies running locally
  // 100 is an arbitrary choice
  val PROXIED_PORTS: List[Int] = (0 until 100).toList.map { index =>
    UniquePortGenerator.PortRangeStart + index
  }
}
