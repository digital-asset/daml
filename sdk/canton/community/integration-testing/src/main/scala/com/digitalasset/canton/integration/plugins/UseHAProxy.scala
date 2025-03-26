// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import better.files.*
import com.digitalasset.canton.UniquePortGenerator
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.{CantonConfig, DefaultProcessingTimeouts}
import com.digitalasset.canton.integration.util.BackgroundRunner
import com.digitalasset.canton.integration.{ConfigTransform, EnvironmentSetupPlugin}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext
import org.testcontainers.Testcontainers
import org.testcontainers.containers.{BindMode, GenericContainer}

import java.nio.file.Path

/** Depending on how HAProxy is run we will have a slightly different setup. These values are
  * provided to tests for generating an appropriate HAProxy configuration.
  * @param cantonConfig
  *   The canton configuration that the plugin has received (may have had other plugin transforms
  *   applied from the original test environment definition config)
  * @param hostAddress
  *   The address canton processes that HAProxy can use to address traffic to (may not be localhost
  *   for docker).
  * @param haproxyPort
  *   The port where HAProxy should be configured to run.
  */
final case class HAProxySetup(
    cantonConfig: CantonConfig,
    hostAddress: String,
    haproxyPort: Port,
)

/** Primarily this defines the configuration to run HA config with. However you also need to supply
  * the host ports to expose to HAProxy in case we're running on a Mac and they don't exist on the
  * same host.
  */
final case class HAProxyConfig(hostPortsToExposeToProxy: Seq[Port], config: String) {
  def writeConfigToTemporaryFile(): Path = {
    // prefer placing the config file under /tmp as this is exposed through MacOS docker's file sharing by default
    val tmpDirectoryIfExists = Option(File("/tmp")).filter(_.exists)
    val proxyConfigFile =
      File.temporaryFile("ha-proxy-", ".cfg", parent = tmpDirectoryIfExists).get()
    proxyConfigFile.deleteOnExit()
    proxyConfigFile.writeText(config)
    proxyConfigFile.path
  }
}

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
class UseHAProxy(
    mkConfig: HAProxySetup => HAProxyConfig,
    useLoadBalancer: (String, Port) => ConfigTransform,
    protected val loggerFactory: NamedLoggerFactory,
) extends EnvironmentSetupPlugin {

  private var proxy: HAProxy = _

  trait HAProxy extends AutoCloseable {
    val hostname: String
    val port: Port
  }

  object HAProxy {
    def apply(config: CantonConfig): HAProxy =
      if (sys.env.contains("CI")) mkNative(config)
      else mkDocker(config)

    private def mkNative(cantonConfig: CantonConfig): HAProxy = new HAProxy {
      override val hostname: String = "localhost"
      // take a port that won't conflict with any integration tests
      override val port: Port = UniquePortGenerator.next

      private val config = mkConfig(HAProxySetup(cantonConfig, hostname, port))
      private val configFile = config.writeConfigToTemporaryFile()

      private val runner = new BackgroundRunner(
        "haproxy",
        Seq("haproxy", "-f", configFile.toString),
        Map(),
        DefaultProcessingTimeouts.testing,
        loggerFactory,
      )

      override def close(): Unit = runner.close()
    }

    private def mkDocker(cantonConfig: CantonConfig): HAProxy = new HAProxy {
      val logger = TracedLogger(getClass, loggerFactory.append("service", "haproxy"))

      private class HAProxyContainer extends GenericContainer[HAProxyContainer]("haproxy:2.4")
      private val HAPROXY_CFG_PATH = "/usr/local/etc/haproxy/haproxy.cfg"

      private val hostAddressToTestContainer = "host.testcontainers.internal"
      private val loadBalancerPort =
        Port.tryCreate(15000) // should be above 1024 so haproxy has access as not running as root
      private val config = mkConfig(
        HAProxySetup(cantonConfig, hostAddressToTestContainer, loadBalancerPort)
      )
      private val configFile = config.writeConfigToTemporaryFile()

      // Exposes ports on the host to the container even if running on MacOS where the container is actually in a separate virtualized host
      // https://www.testcontainers.org/features/networking/#exposing-host-ports-to-the-container
      Testcontainers.exposeHostPorts(config.hostPortsToExposeToProxy.map(_.unwrap)*)

      private val container = new HAProxyContainer()
        .withExposedPorts(
          loadBalancerPort.unwrap
        ) // expose the haproxy port on the host (this will be remapped to a different port on the host)
        .withFileSystemBind(configFile.toString, HAPROXY_CFG_PATH, BindMode.READ_ONLY)

      container.start()

      // copy stdout logs to our test log
      container.followOutput { frame =>
        logger.debug(frame.getUtf8String)(TraceContext.empty)
      }

      override val hostname: String = container.getHost
      override val port: Port = Port.tryCreate(container.getMappedPort(loadBalancerPort.unwrap))

      override def close(): Unit = container.close()
    }
  }

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig = {
    proxy = HAProxy(config)

    useLoadBalancer(proxy.hostname, proxy.port)(config)
  }

  override def afterEnvironmentDestroyed(config: CantonConfig): Unit = {
    Option(proxy).foreach(_.close())
    proxy = null
  }
}
