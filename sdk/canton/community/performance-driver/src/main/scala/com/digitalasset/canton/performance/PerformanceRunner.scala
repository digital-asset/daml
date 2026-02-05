// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.commands.Command
import com.daml.metrics.api.{MetricName, MetricsContext}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{Port, PositiveInt}
import com.digitalasset.canton.config.{ProcessingTimeout, TlsClientConfig}
import com.digitalasset.canton.console.{ConsoleMacros, ParticipantReference}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.client.configuration.CommandClientConfiguration
import com.digitalasset.canton.lifecycle.LifeCycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.metrics.MetricsFactoryProvider
import com.digitalasset.canton.performance.PartyRole.{DvpIssuer, DvpTrader, Master}
import com.digitalasset.canton.performance.RateSettings.SubmissionRateSettings
import com.digitalasset.canton.performance.RateSettings.SubmissionRateSettings.TargetLatency
import com.digitalasset.canton.performance.elements.*
import com.digitalasset.canton.performance.model.java as M
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, WallClock}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{Party, PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{FutureUtil, PekkoUtil}
import com.digitalasset.canton.{LfPartyId, config}
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

sealed trait PartyRole extends Product with Serializable {
  def name: String
  def commandClientConfiguration: CommandClientConfiguration
}
sealed trait ActivePartyRole extends PartyRole {
  def role: M.orchestration.Role
  def settings: RateSettings
}

/** Control the rate of submissions */
final case class RateSettings(
    submissionRateSettings: SubmissionRateSettings,
    batchSize: Int = 3,
    factorOfMaxSubmissionsPerIteration: Double = 0.5,
    commandExpiryCheckSeconds: Int = 240,
    commandClientConfiguration: CommandClientConfiguration =
      RateSettings.defaultCommandClientConfiguration,
) {

  def duplicateSubmissionDelay: Option[NonNegativeFiniteDuration] =
    submissionRateSettings.duplicateSubmissionDelay

  require(batchSize > 0)
  require(factorOfMaxSubmissionsPerIteration > 0)
  require(commandExpiryCheckSeconds > 0)
}

object RateSettings {
  def defaults: RateSettings = RateSettings(TargetLatency())

  sealed trait SubmissionRateSettings extends Product with Serializable {
    def duplicateSubmissionDelay: Option[NonNegativeFiniteDuration]
  }

  object SubmissionRateSettings {

    /** @param duplicateSubmissionRatio
      *   how many of the idempotent commands should be submitted twice to simulate race conditions
      */
    final case class TargetLatency(
        startRate: Double = 3,
        adjustFactor: Double = 1.15,
        targetLatencyMs: Int = 5000,
        duplicateSubmissionRatio: Double = 0.0,
    ) extends SubmissionRateSettings {
      require(startRate > 0)
      require(targetLatencyMs > 0)
      require(adjustFactor >= 1.0)

      override def duplicateSubmissionDelay: Option[NonNegativeFiniteDuration] =
        // if commands should be sent twice, then we delay the submission up to the target latency
        // such that we sample rejections during different phases of the synchronisation
        if (duplicateSubmissionRatio > 0.0 && (1.0 - duplicateSubmissionRatio) < Math.random())
          Some(NonNegativeFiniteDuration.tryOfMillis((Math.random() * targetLatencyMs).toLong))
        else
          None
    }

    final case class FixedRate(rate: Double) extends SubmissionRateSettings {
      require(rate > 0)

      override def duplicateSubmissionDelay: Option[NonNegativeFiniteDuration] = None
    }
  }

  val defaultCommandClientConfiguration: CommandClientConfiguration = CommandClientConfiguration(
    maxCommandsInFlight = 1000,
    maxParallelSubmissions =
      1000, // We need a high value to work around https://github.com/digital-asset/daml/issues/8017
    defaultDeduplicationTime = java.time.Duration.ofSeconds(60),
  )
}

object PartyRole {

  final case class DvpTrader(
      name: String,
      override val settings: RateSettings = RateSettings.defaults,
  ) extends ActivePartyRole {
    override def role: M.orchestration.Role = M.orchestration.Role.TRADER
    override def commandClientConfiguration: CommandClientConfiguration =
      settings.commandClientConfiguration
  }

  final case class DvpIssuer(
      name: String,
      override val settings: RateSettings,
      selfRegistrar: Boolean = true,
  ) extends ActivePartyRole {
    override def role: M.orchestration.Role = M.orchestration.Role.ISSUER

    override def commandClientConfiguration: CommandClientConfiguration =
      RateSettings.defaultCommandClientConfiguration
  }

  /** amendable master configuration
    *
    * @param totalCycles:
    *   Number of propose & accept transactions each trader should initiate.
    * @param reportFrequency:
    *   How often a trader should report progress to the master
    * @param zeroTpsReportOnFinish:
    *   Whether to zero the throughput report when the run is finished.
    */
  final case class MasterDynamicConfig(
      totalCycles: Int = 10000000,
      reportFrequency: Int = 100,
      zeroTpsReportOnFinish: Boolean = true,
      runType: M.orchestration.RunType,
  ) {
    require(totalCycles > 0, s"totalCycles must be positive: $totalCycles.")
    require(reportFrequency > 0, s"reportFrequency must be positive: $reportFrequency.")
    require(
      reportFrequency < totalCycles,
      s"reportFrequency $reportFrequency must be less than totalCycles $totalCycles.",
    )

  }

  object MasterDynamicConfig {
    def fromContract(contract: M.orchestration.TestRun): MasterDynamicConfig =
      MasterDynamicConfig(
        totalCycles = contract.totalCycles.toInt,
        reportFrequency = contract.reportFrequency.toInt,
        runType = contract.runType,
      )
  }

  /** master test run configuration
    *
    * @param name
    *   party name of master party
    * @param quorumParticipants
    *   minimum number of participants when the test run should begin
    * @param quorumIssuers
    *   minimum number of issuers when the test run should begin
    * @param amendments
    *   optional amendments to the configuration while the system is running
    */
  final case class Master(
      name: String,
      quorumParticipants: Int = 2,
      quorumIssuers: Int = 1,
      runConfig: MasterDynamicConfig,
      amendments: Seq[AmendMasterConfig] = Seq(),
  ) extends PartyRole {
    require(quorumIssuers > 0, s"quorumIssuers must be positive: $quorumIssuers.")
    (runConfig.runType: @unchecked) match {
      case _: M.orchestration.runtype.DvpRun =>
        require(
          quorumParticipants > 1,
          s"quorumParticipants must be more than one: $quorumParticipants.",
        )
      case _: M.orchestration.runtype.NoRun =>
        throw new IllegalArgumentException("Not supported run type NoRun")
    }

    override def commandClientConfiguration: CommandClientConfiguration =
      RateSettings.defaultCommandClientConfiguration

  }
}

/** Connectivity information for the performance runner
  *
  * @param name
  *   the name of the participant node
  * @param host
  *   the hostname
  * @param port
  *   the port of the ledger api server
  * @param tls
  *   optional tls settings
  * @param reprocessAcs
  *   whether to use the AcsService on startup (in order to resume an existing run)
  */
final case class Connectivity(
    name: String,
    host: String = "localhost",
    port: Port,
    tls: Option[TlsClientConfig] = None,
    reprocessAcs: Boolean = true,
)

/** configure the performance runner
  *
  * @param master
  *   the name of the master party which is coordinating the performance run
  * @param localRoles
  *   the roles that this runner is going to play
  * @param ledger
  *   the ledger api connectivity configuration
  * @param darPath
  *   the optional dar path
  * @param baseSynchronizerId
  *   Main synchronizer to be used (e.g., assets creation)
  * @param otherSynchronizers
  *   Other synchronizers that can be used to submit trades
  * @param otherSynchronizersRatio
  *   Ratio of trades that can be submitted to one of the `otherSynchronizers`
  */
final case class PerformanceRunnerConfig(
    master: String,
    localRoles: Set[PartyRole],
    ledger: Connectivity,
    baseSynchronizerId: SynchronizerId,
    otherSynchronizers: Seq[SynchronizerId] = Nil,
    otherSynchronizersRatio: Double = 0.0,
    darPath: Option[String] = None,
) {
  def masterConfig: Option[Master] = localRoles.collectFirst { case x: Master =>
    x
  }

  def synchronizerIds(party: String): Set[SynchronizerId] =
    if (localRoles.exists(_.name == party)) (baseSynchronizerId +: otherSynchronizers).toSet
    else Set()
}

class PerformanceRunner(
    config: PerformanceRunnerConfig,
    metricsRegistry: MetricsFactoryProvider,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContextExecutor)
    extends NamedLogging
    with AutoCloseable
    with NoTracing
    with DriverControl {

  implicit val actorSystem: ActorSystem = PekkoUtil.createActorSystem(loggerFactory.threadName)
  implicit val executionSequencerFactory: ExecutionSequencerFactory =
    PekkoUtil.createExecutionSequencerFactory(loggerFactory.threadName, noTracingLogger)

  private val clock = new WallClock(ProcessingTimeout(), loggerFactory)
  private val setup = new SetupDriver(
    loggerFactory,
    config.darPath,
    config.baseSynchronizerId +: config.otherSynchronizers,
  )
  private val drivers = ListBuffer[BaseDriver]()
  private val active_ = new AtomicBoolean(true)

  override def active: Boolean = active_.get()

  override def disable(): Unit =
    if (active_.getAndSet(false)) {
      logger.warn("Disabling performance runner due to issues.")
    }

  def updateRateSettings(update: RateSettings => RateSettings): Unit =
    drivers.foreach(_.updateRateSettings(update))

  def setActive(active: Boolean): Unit = this.active_.set(active)

  def status(): Seq[DriverStatus] =
    // Create a defensive copy to prevent a ConcurrentModificationException
    drivers.toList.flatMap(_.status().toList)

  def master(): Option[MasterDriver] = drivers.collectFirst { case x: MasterDriver => x }

  def startup(): Future[Either[String, Unit]] = {
    logger.debug("Setting up up performance runner")
    val resultF = (for {
      startup <- EitherT(setup.run(config))
      _ = logger.debug("Performance runner setup complete")
      (masterLf, mapping) = startup
      // start both, master and drivers
      _ = logger.debug("Starting performance runner master")
      master = startMaster(config.masterConfig, masterLf)
      _ = logger.debug("Starting performance runner drivers")
      drivers = startDrivers(masterLf, mapping)
      // wait for them to have finished!
      _ <- master
      _ = logger.debug("Performance runner master successfully started")
      _ <- drivers
      _ = logger.debug("Performance runner drivers successfully started")
    } yield ()).value
    resultF.thereafter {
      case Success(Right(())) => logger.debug("Performance runner terminated gracefully.");
      case Success(Left(error)) =>
        logger.error(s"Performance runner terminated with an error: $error")
      case Failure(ex) => logger.error("Performance runner terminated with an exception", ex)
    }
  }

  private def startDrivers(
      masterParty: LfPartyId,
      mappings: Map[String, LfPartyId],
  ): EitherT[Future, String, Unit] =
    EitherT(
      FutureUtil.logOnFailure(
        Future
          .traverse(config.localRoles.collect { case x: ActivePartyRole => x }) { role =>
            val party = mappings
              .getOrElse(
                role.name,
                throw new IllegalStateException(s"Role ${role.name} should be in the mappings"),
              )
            val lf = loggerFactory.append("party-hash", party.take(20))
            val labeledMetricsFactory =
              metricsRegistry.generateMetricsFactory(MetricsContext("role" -> role.name))
            val driver = role match {
              case trd: DvpTrader =>
                new dvp.TraderDriver(
                  config.ledger,
                  party,
                  masterParty,
                  trd,
                  PerformanceRunner.prefix,
                  labeledMetricsFactory,
                  loggerFactory = lf,
                  control = this,
                  baseSynchronizerId = config.baseSynchronizerId,
                  otherSynchronizers = config.otherSynchronizers,
                  otherSynchronizersRatio = config.otherSynchronizersRatio,
                )
              case isr: DvpIssuer =>
                new dvp.IssuerDriver(
                  config.ledger,
                  party,
                  masterParty,
                  isr,
                  PerformanceRunner.prefix,
                  labeledMetricsFactory,
                  loggerFactory = lf,
                  control = this,
                  baseSynchronizerId = config.baseSynchronizerId,
                )
            }
            drivers.append(driver)
            FutureUtil.logOnFailure(driver.start(), s"Failed to start driver $role").flatMap {
              case Left(er) => Future.successful(Left(er))
              case Right(()) =>
                driver.done().map { _ =>
                  Either.unit
                }
            }
          }
          .map { seq =>
            seq.foldLeft(Either.unit[String])((acc, elem) =>
              acc.flatMap { _ =>
                elem
              }
            )
          },
        "startup",
      )
    )

  private def startMaster(
      cnf: Option[Master],
      masterParty: LfPartyId,
  ): EitherT[Future, String, Unit] =
    cnf match {
      case Some(masterConfig) =>
        val driver =
          new MasterDriver(
            config.ledger,
            masterParty,
            masterConfig,
            loggerFactory.appendUnnamedKey("driver", "master"),
            this,
          )
        drivers.append(driver)
        EitherT(driver.start().flatMap {
          case Left(err) => Future.successful(Left(err))
          case Right(()) => driver.done().map(_ => Right(()))
        })
      case None =>
        EitherT.rightT(())
    }

  private val closing = new AtomicBoolean(false)

  override def close(): Unit =
    if (closing.compareAndSet(false, true)) {
      logger.debug("Start closing performance runner")
      try {
        Await.result(closeF(), 10.seconds)
      } catch {
        case NonFatal(e) => logger.error("Orderly shutdown of perf runner failed", e)
      }
      logger.debug("Finished closing performance runner")
    }

  def closeF(): Future[Unit] = {
    closing.set(true)
    this.active_.set(false)
    val closingResult = Try(
      LifeCycle.close(
        clock,
        () => LifeCycle.close(drivers.toSeq*)(logger),
        () => executionSequencerFactory.close(),
      )(logger)
    )
    logger.info("Terminating actor system of the performance runner")
    actorSystem.terminate().transform(_.flatMap(_ => closingResult))
  }
}

object PerformanceRunner {

  private val prefix = MetricName("canton", "performance")

  /** Initialize active nodes and registry */
  def initializeRegistry(
      logger: TracedLogger,
      activeNodes: Seq[(PerformanceRunnerConfig, ParticipantReference)],
      registryNodes: NonEmpty[Seq[ParticipantReference]],
  )(implicit traceContext: TraceContext): Unit = {

    // allocate the issuer parties
    logger.info(s"Allocating issuer parties on ${activeNodes.length} nodes")
    val issuers = activeNodes
      .flatMap { case (config, node) =>
        config.localRoles.toSeq.collect {
          case issuer: PartyRole.DvpIssuer if !issuer.selfRegistrar => Seq((config, issuer, node))
          case _ => Seq.empty
        }
      }
      .flatten
      .map { case (runnerConfig, roleConfig, node) =>
        node.parties.enable(
          roleConfig.name
        ) -> (runnerConfig.synchronizerIds(roleConfig.name))
      }

    // allocate the dso on all synchronizers
    logger.info(s"Allocating the dso on all synchronizers and registry nodes")
    val one = registryNodes.head1
    val rest = registryNodes.drop(1)

    val dso = PartyId.tryCreate("registry", one.namespace)
    registryNodes.forgetNE
      .flatMap(p => p.synchronizers.list_connected().map(r => (p, r.physicalSynchronizerId)))
      .foreach { case (participant, synchronizerId) =>
        participant.topology.party_to_participant_mappings
          .propose(
            dso,
            newParticipants = Seq(one.id -> ParticipantPermission.Submission) ++ rest.map(
              _.id -> ParticipantPermission.Confirmation
            ),
            store = TopologyStoreId.Synchronizer(synchronizerId),
            threshold = PositiveInt.one,
          )
          .discard
      }

    logger.info(s"Waiting for the registry to appear on the first pn")
    ConsoleMacros.utils.retry_until_true(timeout = config.NonNegativeDuration.ofSeconds(30)) {
      one.synchronizers.list_connected().map(_.physicalSynchronizerId).forall { syncId =>
        val have = one.topology.party_to_participant_mappings
          .list(
            synchronizerId = syncId,
            filterParty = "registry",
          )
        have.nonEmpty
      }
    }

    logger.info(s"Creating the registry issuer delegation contract for each issuer")
    issuers.map(_._1).toSet.foreach { (issuer: Party) =>
      one.ledger_api.commands
        .submit(
          Seq(dso),
          (new M.dvp.asset.Registry(dso.toProtoPrimitive, issuer.toProtoPrimitive))
            .create()
            .commands()
            .asScala
            .toSeq
            .map(c => Command.fromJavaProto(c.toProtoCommand)),
        )
        .discard
    }

    logger.info("Removing submission rights for the registry party")
    one.synchronizers.list_connected().map(_.physicalSynchronizerId).foreach { synchronizerId =>
      one.topology.party_to_participant_mappings
        .propose(
          dso,
          newParticipants = Seq(one.id -> ParticipantPermission.Confirmation) ++ rest.map(
            _.id -> ParticipantPermission.Confirmation
          ),
          store = TopologyStoreId.Synchronizer(synchronizerId),
          threshold = PositiveInt.tryCreate(registryNodes.length),
          mustFullyAuthorize = true,
        )
        .discard
    }
  }

}
