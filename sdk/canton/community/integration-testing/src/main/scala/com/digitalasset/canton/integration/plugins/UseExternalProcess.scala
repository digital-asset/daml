// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import better.files.File
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.integration.EnvironmentSetupPlugin
import com.digitalasset.canton.integration.plugins.UseExternalProcess.{RunVersion, ShutdownPhase}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.config.{LocalParticipantConfig, RemoteParticipantConfig}
import com.digitalasset.canton.synchronizer.mediator.{MediatorNodeConfig, RemoteMediatorConfig}
import com.digitalasset.canton.synchronizer.sequencer.config.{
  RemoteSequencerConfig,
  SequencerNodeConfig,
}
import com.digitalasset.canton.tracing.TraceContext
import monocle.macros.syntax.lens.*

import java.nio.file.{Path, Paths}
import scala.collection.concurrent.TrieMap

/** Run Canton processes in separate JVM instances such that we can kill them at any time
  *
  * A specific release version of Canton can be specified to run the external process during startup
  * In order to test crash recovery, we need the ability to kill the system while running. This
  * plugin allows to move the execution of several nodes to separate JVM processes (one per node).
  *
  * Using participant1 as the example, what happens is the following:
  *   - Assuming, you create a new integration test using this plugin, passing `participant1` as
  *     external node.
  *   - The plugin will modify the [[CantonConfig]]. First, the participant node configuration will
  *     be written into a new separate configuration file. Then, the existing participant1 config
  *     will be deleted and replaced with a remote configuration.
  *   - Then, upon start of the environment, the given participant node will start up in a separate
  *     java process.
  *
  * The latter works by inspecting System.getProperty("java.class.path"). If the path contains
  * `sbt-launch.jar`, then we assume that the process is running as part of SBT. If not, then we
  * assume we are running as part of IntelliJ
  *
  * In SBT: we assume that dumpClassPath (of build.sbt) has been executed beforehand such that the
  * classpath is stored in a file named classpath.txt.
  *
  * In IntelliJ, we assume that the system property contains the necessary classpath.
  *
  * Finally, we start the process in background using a ProcessBuilder. The config file will be
  * named `external-participant1-<admin-api-port>.conf` and the process will log into
  * `log/external-participant1-<admin-api-port>.log`.
  *
  * The nodes can then subsequently be controlled using `plugin.start("nodename")`
  * `plugin.kill("nodename")` `plugin.restart("nodename")`
  *
  * @param externalParticipants
  *   which participants to run as background nodes
  * @param configDirectory
  *   Where configuration files for the external nodes are written to.
  */
class UseExternalProcess(
    override protected val loggerFactory: NamedLoggerFactory,
    val configDirectory: String = "tmp/",
    override val fileNameHint: String,
    override val shutdownPhase: ShutdownPhase = ShutdownPhase.BeforeEnvironment,
    override val clockServer: Option[FullClientConfig] = None,
    addEnvironment: Map[String, String] = Map("JDK_JAVA_OPTIONS" -> "-Xmx784m"),
    val externalParticipants: Set[String] = Set.empty,
    val externalSequencers: Set[String] = Set.empty,
    val externalMediators: Set[String] = Set.empty,
    val removeConfigPaths: Set[(String, Option[(String, Any)])] = Set.empty,
) extends EnvironmentSetupPlugin
    with UseExternalProcessBase[LocalNodeConfig] {

  val configs: TrieMap[String, CantonConfig] = TrieMap.empty

  private val tmpDir = File(configDirectory)
  if (!tmpDir.exists) {
    tmpDir.createDirectoryIfNotExists(createParents = true)
  }
  require(tmpDir.isDirectory, s"$configDirectory exists but is not a directory")

  private def storeConfigFile(config: CantonConfig, path: Path): Unit =
    CantonConfig.save(config, path.toString, removeConfigPaths)

  private def configFile(name: String, config: NodeConfig): Path =
    Paths
      .get(configDirectory)
      .resolve(s"${filename(localToRemoteNameTransformer(name), config)}.conf")

  private def makeCommand(
      name: String,
      config: NodeConfig,
      runVersionO: Option[RunVersion] = None,
  ): Seq[String] = {
    val command = runVersionO match {
      case Some(RunVersion.Release(binaryPath)) => Seq(binaryPath) ++ threadingProps ++ loggingProps
      case _ =>
        Seq(
          "java"
        ) ++ classpathProps ++ threadingProps ++ loggingProps :+ "com.digitalasset.canton.CantonEnterpriseApp"
    }
    command ++ cantonInvocationArgs(
      logFile(name, config).toString,
      configFile(name, config).toString,
    )
  }

  def configureAndStore(
      config: CantonConfig,
      adjustedParameters: CantonParameters,
  )(
      name: String,
      nodeConfig: LocalNodeConfig,
  ): CantonConfig = {
    val nameInConfig = instanceNameInConfig(name)
    val singleConfig = config
      .asSingleNode(InstanceName.tryCreate(name))
    val finalConfig =
      singleConfig
        .focus(_.parameters)
        .replace(adjustedParameters)
        .copy(
          sequencers = singleConfig.sequencers.collect { case (k, v) =>
            (InstanceName.tryCreate(instanceNameInConfig(k.unwrap)), v)
          },
          mediators = singleConfig.mediators.collect { case (k, v) =>
            (InstanceName.tryCreate(instanceNameInConfig(k.unwrap)), v)
          },
          participants = singleConfig.participants.collect { case (k, v) =>
            (InstanceName.tryCreate(instanceNameInConfig(k.unwrap)), v)
          },
        )
    storeConfigFile(finalConfig, configFile(nameInConfig, nodeConfig))
    finalConfig
  }

  /** Registers node in background process handler and optionally starts it (if manualStart is
    * false).
    */
  private def configureAndOptStart(
      config: CantonConfig,
      adjustedParameters: CantonParameters,
  )(
      name: String,
      nodeConfig: LocalNodeConfig,
  ): Unit = {
    val remoteName = localToRemoteNameTransformer(name)
    val nameInConfig = instanceNameInConfig(name)
    val finalConfig = configureAndStore(config, adjustedParameters)(name, nodeConfig)
    configs.put(remoteName, finalConfig)
    handler.tryAdd(
      remoteName,
      makeCommand(nameInConfig, nodeConfig),
      addEnvironment,
      nodeConfig,
      finalConfig.parameters.manualStart,
    )(TraceContext.empty)
  }

  /** Re-registers node in background process handler with a different config. */
  def configureAndReplace(
      config: CantonConfig,
      adjustedParameters: CantonParameters,
  )(
      name: String,
      nodeConfig: LocalNodeConfig,
  ): Unit = {
    val finalConfig = configureAndStore(config, adjustedParameters)(name, nodeConfig)
    configs.replace(name, finalConfig)
  }

  private def adjustClockIfNecessary(
      parameters: CantonParameters,
      remote: Option[FullClientConfig],
  ): CantonParameters =
    parameters.clock match {
      case ClockConfig.SimClock =>
        val remoteApi =
          clockServer
            .orElse(remote)
            .getOrElse(sys.error("Static time requires at least one node that exists locally"))
        parameters.focus(_.clock).replace(ClockConfig.RemoteClock(remoteApi))
      case _ => parameters
    }

  protected def localToRemoteNameTransformer(name: String): String = name

  /** Override this method if you want to change the name of the node in the config file */
  private def instanceNameInConfig(name: String): String = name

  private def splitLocalAndRemote[LocalConfig, RemoteConfig](
      nodes: Map[String, LocalConfig],
      external: Set[String],
      toRemote: LocalConfig => RemoteConfig,
  ): (Map[String, LocalConfig], Map[String, RemoteConfig]) = {
    external.foreach { name =>
      require(
        nodes.keySet.contains(name),
        s"Couldn't find configuration for Canton component '$name'. Following names are known: ${nodes.keySet} ",
      )
    }

    // modify config, extract the participants that should run externally from the config file and leave them
    // in here as remote participants
    val local = nodes.filterNot(x => external.contains(x._1))
    val remote = nodes.filter(x => external.contains(x._1)).map { case (k, v) =>
      (localToRemoteNameTransformer(k), toRemote(v))
    }
    (local, remote)
  }

  private def updateCantonConfig(
      config: CantonConfig
  ): (CantonConfig, CantonParameters) = {
    // computes new config but doesn't update the config used by a background process yet
    val (localParticipants, remoteParticipants) =
      splitLocalAndRemote[LocalParticipantConfig, RemoteParticipantConfig](
        config.participantsByString,
        externalParticipants,
        _.toRemoteConfig,
      )
    val (localMediators, remoteMediators) =
      splitLocalAndRemote[MediatorNodeConfig, RemoteMediatorConfig](
        config.mediatorsByString,
        externalMediators,
        _.toRemoteConfig,
      )

    val (localSequencers, remoteSequencers) =
      splitLocalAndRemote[SequencerNodeConfig, RemoteSequencerConfig](
        config.sequencersByString,
        externalSequencers,
        _.toRemoteConfig,
      )

    val newConfig = config
      .focus(_.participants)
      .replace(InstanceName.tryFromStringMap(localParticipants))
      .focus(_.sequencers)
      .replace(InstanceName.tryFromStringMap(localSequencers))
      .focus(_.remoteSequencers)
      .replace(InstanceName.tryFromStringMap(remoteSequencers))
      .focus(_.mediators)
      .replace(InstanceName.tryFromStringMap(localMediators))
      .focus(_.remoteParticipants)
      .replace(config.remoteParticipants ++ InstanceName.tryFromStringMap(remoteParticipants))
      .focus(_.remoteMediators)
      .replace(config.remoteMediators ++ InstanceName.tryFromStringMap(remoteMediators))

    // adjustment to the config of the external process only
    val adjustedParameters = adjustClockIfNecessary(
      config.parameters,
      (localSequencers.map(_._2.adminApi.clientConfig)
        ++ localMediators.map(_._2.adminApi.clientConfig)
        ++ localParticipants.map(_._2.adminApi.clientConfig)).headOption,
    )
    (newConfig, adjustedParameters)
  }

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig = {

    val (newConfig, adjustedParameters) = updateCantonConfig(config)

    val register = configureAndOptStart(config, adjustedParameters) _
    for (participant <- externalParticipants) {
      val participantConfig = config.participantsByString(participant)
      register(participant, participantConfig)
    }
    for (sequencer <- externalSequencers) {
      val sequencerConfig = config.sequencersByString(sequencer)
      register(sequencer, sequencerConfig)
    }
    for (mediator <- externalMediators) {
      val mediatorConfig = config.mediatorsByString(mediator)
      register(mediator, mediatorConfig)
    }
    newConfig
  }

  def start(instanceName: String): Unit = handler.tryStart(instanceName)

  /** Start a specific Canton version */
  def start(instanceName: String, cantonVersion: RunVersion): Unit = {
    val nodeConfig = config(instanceName)
    val nameInConfig = instanceNameInConfig(instanceName)
    handler.stopAndRemove(instanceName)
    handler.tryAdd(
      instanceName,
      makeCommand(nameInConfig, nodeConfig, Some(cantonVersion)),
      addEnvironment,
      nodeConfig,
      manualStart = false,
    )(
      TraceContext.empty
    )
  }

  private def filename(name: String, config: NodeConfig): String =
    s"external-$name--${config.clientAdminApi.port.unwrap}--$fileNameHint"

  def storageConfig(instanceName: String): StorageConfig = config(instanceName).storage

  private def logFile(name: String, config: NodeConfig): Path =
    Paths.get("log", s"${filename(name, config)}.log")

  def logFile(instanceName: String): Path =
    logFile(instanceNameInConfig(instanceName), config(instanceName))
}

object UseExternalProcess {
  sealed trait ShutdownPhase
  object ShutdownPhase {
    case object BeforeEnvironment extends ShutdownPhase
    case object AfterEnvironment extends ShutdownPhase
  }

  sealed trait RunVersion
  object RunVersion {
    case object Main extends RunVersion
    final case class Release(binaryPath: String) extends RunVersion {
      val cantonExampleDarPath: File =
        File(binaryPath).parent.parent / "dars" / "CantonExamples.dar"
    }
  }
}
