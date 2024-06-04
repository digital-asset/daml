// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import com.digitalasset.canton.admin.api.client.commands.EnterpriseSequencerAdminCommands.LocatePruningTimestampCommand
import com.digitalasset.canton.admin.api.client.commands.*
import com.digitalasset.canton.admin.api.client.data.StaticDomainParameters as ConsoleStaticDomainParameters
import com.digitalasset.canton.config.RequireTypes.{ExistingFile, NonNegativeInt, Port, PositiveInt}
import com.digitalasset.canton.config.*
import com.digitalasset.canton.console.CommandErrors.NodeNotStarted
import com.digitalasset.canton.console.commands.*
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.mediator.{
  MediatorNode,
  MediatorNodeBootstrap,
  MediatorNodeConfigCommon,
  RemoteMediatorConfig,
}
import com.digitalasset.canton.domain.sequencing.config.{
  RemoteSequencerConfig,
  SequencerNodeConfigCommon,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.admin.EnterpriseSequencerBftAdminData.PeerNetworkStatus
import com.digitalasset.canton.domain.sequencing.sequencer.{
  SequencerClients,
  SequencerPruningStatus,
}
import com.digitalasset.canton.domain.sequencing.{SequencerNode, SequencerNodeBootstrap}
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.admin.data.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.metrics.MetricValue
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.participant.config.{
  BaseParticipantConfig,
  LocalParticipantConfig,
  RemoteParticipantConfig,
}
import com.digitalasset.canton.participant.{ParticipantNode, ParticipantNodeBootstrap}
import com.digitalasset.canton.sequencer.admin.v30.SequencerPruningAdministrationServiceGrpc
import com.digitalasset.canton.sequencer.admin.v30.SequencerPruningAdministrationServiceGrpc.SequencerPruningAdministrationServiceStub
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnections}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.TimeQuery
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.ErrorUtil

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.*
import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3

trait InstanceReference
    extends AdminCommandRunner
    with HasUniqueIdentifier
    with Helpful
    with NamedLogging
    with FeatureFlagFilter
    with PrettyPrinting {

  @inline final override def uid: UniqueIdentifier = id.uid

  val name: String
  protected val instanceType: String

  protected[canton] def executionContext: ExecutionContext

  override def pretty: Pretty[InstanceReference] =
    prettyOfString(inst => show"${inst.instanceType.unquoted} ${inst.name.singleQuoted}")

  val consoleEnvironment: ConsoleEnvironment

  override protected[console] def tracedLogger: TracedLogger = logger

  override def hashCode(): Int = {
    val init = this.getClass.hashCode()
    val t1 = MurmurHash3.mix(init, consoleEnvironment.hashCode())
    val t2 = MurmurHash3.mix(t1, name.hashCode)
    t2
  }

  // this is just testing, because the cached values should remain unchanged in operation
  @Help.Summary("Clear locally cached variables", FeatureFlag.Testing)
  @Help.Description(
    "Some commands cache values on the client side. Use this command to explicitly clear the caches of these values."
  )
  def clear_cache(): Unit = {
    topology.clearCache()
  }

  type Status <: NodeStatus.Status

  def id: NodeIdentity

  def maybeId: Option[NodeIdentity]

  def health: HealthAdministration[Status]

  def keys: KeyAdministrationGroup

  @Help.Summary("Inspect parties")
  @Help.Group("Parties")
  def parties: PartiesAdministrationGroup

  def topology: TopologyAdministrationGroup

  private lazy val trafficControl_ =
    new TrafficControlAdministrationGroup(
      this,
      consoleEnvironment,
      loggerFactory,
    )
  @Help.Summary("Traffic control related commands")
  @Help.Group("Traffic")
  def traffic_control: TrafficControlAdministrationGroup = trafficControl_

}

object InstanceReference {
  implicit class Active[A <: InstanceReference](seq: Seq[A]) {
    def active: Seq[A] = seq.filter(_.health.active)
  }
}

/** Pointer for a potentially running instance by instance type (domain/participant) and its id.
  * These methods define the REPL interface for these instances (e.g. participant1 start)
  */
trait LocalInstanceReference extends InstanceReference with NoTracing {

  val name: String
  val consoleEnvironment: ConsoleEnvironment
  private[console] val nodes: Nodes[CantonNode, CantonNodeBootstrap[CantonNode]]

  @Help.Summary("Database related operations")
  @Help.Group("Database")
  object db extends Helpful {

    @Help.Summary("Migrates the instance's database if using a database storage")
    @Help.Description(
      """When instances reside on different nodes, their database migration can be run in parallel
        |to save time. Please not that the migration commands must however must be run on each node
        |individually, because remote migration through `participants.remote...` is not supported.
        |"""
    )
    def migrate(): Unit = consoleEnvironment.run(migrateDbCommand())

    @Help.Summary(
      "Only use when advised - repairs the database migration of the instance's database"
    )
    @Help.Description(
      """In some rare cases, we change already applied database migration files in a new release and the repair
        |command resets the checksums we use to ensure that in general already applied migration files have not been changed.
        |You should only use `db.repair_migration` when advised and otherwise use it at your own risk - in the worst case running
        |it may lead to data corruption when an incompatible database migration (one that should be rejected because
        |the already applied database migration files have changed) is subsequently falsely applied.
        |"""
    )
    def repair_migration(force: Boolean = false): Unit =
      consoleEnvironment.run(repairMigrationCommand(force))

  }

  @Help.Summary("Start the instance")
  def start(): Unit = consoleEnvironment.run(startCommand())

  @Help.Summary("Stop the instance")
  def stop(): Unit = consoleEnvironment.run(stopCommand())

  @Help.Summary("Check if the local instance is running")
  def is_running: Boolean = nodes.isRunning(name)

  @Help.Summary("Check if the local instance is running and is fully initialized")
  def is_initialized: Boolean = nodes.getRunning(name).exists(_.isInitialized)

  @Help.Summary("Config of node instance")
  def config: LocalNodeConfig

  @Help.Summary("Manage public and secret keys")
  @Help.Group("Keys")
  override def keys: LocalKeyAdministrationGroup = _keys

  private val _keys =
    new LocalKeyAdministrationGroup(this, this, consoleEnvironment, crypto, loggerFactory)(
      executionContext
    )

  @Help.Summary("Access the local nodes metrics")
  @Help.Group("Metrics")
  object metrics {

    private def filterByNodeAndAttribute(
        attributes: Map[String, String]
    )(value: MetricValue): Boolean = {
      value.attributes.get("node").contains(name) && attributes.forall { case (k, v) =>
        value.attributes.get(k).contains(v)
      }
    }

    private def getOne(
        metricName: String,
        attributes: Map[String, String] = Map(),
    ): Either[String, MetricValue] = check(FeatureFlag.Testing) {
      val candidates = consoleEnvironment.environment.configuredOpenTelemetry.onDemandMetricsReader
        .read()
      val res = candidates
        .find { data =>
          data.getName.equals(metricName)
        }
        .toList
        .flatMap(MetricValue.fromMetricData)
        .filter(filterByNodeAndAttribute(attributes))
      res match {
        case one :: Nil => Right(one)
        case Nil =>
          Left(s"No metric of name ${metricName} with instance name ${name} found.")
        case other => Left(s"Found ${other.length} matching metrics")
      }
    }

    private def getOneOfType[TargetType <: MetricValue](
        metricName: String,
        attributes: Map[String, String],
    )(implicit
        M: ClassTag[TargetType]
    ): TargetType =
      consoleEnvironment.run(ConsoleCommandResult.fromEither(for {
        item <- getOne(metricName, attributes)
        casted <- item
          .select[TargetType]
          .toRight(s"Metric is not a ${M.showType} but ${item.getClass}")
      } yield casted))

    @Help.Summary("Get a particular metric")
    @Help.Description(
      """Returns the metric with the given name and optionally matching attributes, or error if multiple matching are found."""
    )
    def get(
        metricName: String,
        attributes: Map[String, String] = Map(),
    ): MetricValue =
      consoleEnvironment.run(ConsoleCommandResult.fromEither(getOne(metricName, attributes)))

    @Help.Summary("Get a particular histogram")
    @Help.Description(
      """Returns the metric with the given name and optionally matching attributes, or error if multiple matching are found."""
    )
    def get_histogram(
        metricName: String,
        attributes: Map[String, String] = Map(),
    ): MetricValue.Histogram = getOneOfType[MetricValue.Histogram](metricName, attributes)

    @Help.Summary("Get a particular summary")
    @Help.Description(
      """Returns the metric with the given name and optionally matching attributes, or error if multiple matching are found."""
    )
    def get_summary(
        metricName: String,
        attributes: Map[String, String] = Map(),
    ): MetricValue.Summary = getOneOfType[MetricValue.Summary](metricName, attributes)

    @Help.Summary("Get a particular long point")
    @Help.Description(
      """Returns the metric with the given name and optionally matching attributes, or error if multiple matching are found."""
    )
    def get_long_point(
        metricName: String,
        attributes: Map[String, String] = Map(),
    ): MetricValue.LongPoint = getOneOfType[MetricValue.LongPoint](metricName, attributes)

    @Help.Summary("Get a particular double point")
    @Help.Description(
      """Returns the metric with the given name and optionally matching attributes, or error if multiple matching are found."""
    )
    def get_double_point(
        metricName: String,
        attributes: Map[String, String] = Map(),
    ): MetricValue.DoublePoint = getOneOfType[MetricValue.DoublePoint](metricName, attributes)

    @Help.Summary("List all metrics")
    @Help.Description(
      """Returns the metric with the given name and optionally matching attributes."""
    )
    def list(
        filterName: String = "",
        attributes: Map[String, String] = Map(),
    ): Map[String, Seq[MetricValue]] =
      check(FeatureFlag.Testing) {
        consoleEnvironment.environment.configuredOpenTelemetry.onDemandMetricsReader
          .read()
          .filter(_.getName.startsWith(filterName))
          .flatMap(dt => MetricValue.fromMetricData(dt).map((dt.getName, _)))
          .filter { case (_, value) =>
            filterByNodeAndAttribute(attributes)(value)
          }
          .groupMap { case (name, _) => name } { case (_, value) => value }
      }

  }

  private[console] def migrateDbCommand(): ConsoleCommandResult[Unit] =
    migrateInstanceDb().toResult(_.message, _ => ())

  private[console] def repairMigrationCommand(force: Boolean): ConsoleCommandResult[Unit] =
    repairMigrationOfInstance(force).toResult(_.message, _ => ())

  private[console] def startCommand(): ConsoleCommandResult[Unit] =
    startInstance()
      .toResult({
        case m: PendingDatabaseMigration =>
          s"${m.message} Please run `${m.name}.db.migrate` to apply pending migrations"
        case m => m.message
      })

  private[console] def stopCommand(): ConsoleCommandResult[Unit] =
    try {
      stopInstance().toResult(_.message)
    } finally {
      ErrorUtil.withThrowableLogging(clear_cache())
    }

  protected def migrateInstanceDb(): Either[StartupError, ?] = nodes.migrateDatabase(name)
  protected def repairMigrationOfInstance(force: Boolean): Either[StartupError, Unit] = {
    Either
      .cond(force, (), DidntUseForceOnRepairMigration(name))
      .flatMap(_ => nodes.repairDatabaseMigration(name))
  }

  protected def startInstance(): Either[StartupError, Unit] =
    nodes.startAndWait(name)
  protected def stopInstance(): Either[ShutdownError, Unit] = nodes.stopAndWait(name)
  protected[canton] def crypto: Crypto

  protected def runCommandIfRunning[Result](
      runner: => ConsoleCommandResult[Result]
  ): ConsoleCommandResult[Result] =
    if (is_running)
      runner
    else
      NodeNotStarted.ErrorCanton(this)

  override protected[console] def adminCommand[Result](
      grpcCommand: GrpcAdminCommand[?, ?, Result]
  ): ConsoleCommandResult[Result] = {
    runCommandIfRunning(
      consoleEnvironment.grpcAdminCommandRunner
        .runCommand(name, grpcCommand, config.clientAdminApi, None)
    )
  }

}

trait RemoteInstanceReference extends InstanceReference {
  @Help.Summary("Manage public and secret keys")
  @Help.Group("Keys")
  override val keys: KeyAdministrationGroup =
    new KeyAdministrationGroup(this, this, consoleEnvironment, loggerFactory)

  def config: NodeConfig

  override protected[console] def adminCommand[Result](
      grpcCommand: GrpcAdminCommand[?, ?, Result]
  ): ConsoleCommandResult[Result] =
    consoleEnvironment.grpcAdminCommandRunner.runCommand(
      name,
      grpcCommand,
      config.clientAdminApi,
      None,
    )
}

/** Bare, Canton agnostic parts of the ledger-api client
  *
  * This implementation allows to access any kind of ledger-api client, which does not need to be Canton based.
  * However, this comes at some cost, as some of the synchronization between nodes during transaction submission
  * is not supported
  *
  * @param hostname the hostname of the ledger api server
  * @param port the port of the ledger api server
  * @param tls the tls config to use on the client
  * @param token the jwt token to use on the client
  */
class ExternalLedgerApiClient(
    hostname: String,
    port: Port,
    tls: Option[TlsClientConfig],
    val token: Option[String] = None,
)(implicit val consoleEnvironment: ConsoleEnvironment)
    extends BaseLedgerApiAdministration
    with LedgerApiCommandRunner
    with FeatureFlagFilter
    with NamedLogging {

  override protected val name: String = s"$hostname:${port.unwrap}"

  override val loggerFactory: NamedLoggerFactory =
    consoleEnvironment.environment.loggerFactory.append("client", name)

  override protected def domainOfTransaction(transactionId: String): DomainId =
    throw new NotImplementedError("domain_of is not implemented for external ledger api clients")

  override protected[console] def ledgerApiCommand[Result](
      command: GrpcAdminCommand[?, ?, Result]
  ): ConsoleCommandResult[Result] =
    consoleEnvironment.grpcLedgerCommandRunner
      .runCommand("sourceLedger", command, ClientConfig(hostname, port, tls), token)

  override protected def optionallyAwait[Tx](
      tx: Tx,
      txId: String,
      optTimeout: Option[NonNegativeDuration],
  ): Tx = tx

}

/** Allows to query the public api of a sequencer (e.g., sequencer connect service).
  *
  * @param trustCollectionFile a file containing certificates of all nodes the client trusts. If none is specified, defaults to the JVM trust store
  */
class SequencerPublicApiClient(
    sequencerConnection: GrpcSequencerConnection,
    trustCollectionFile: Option[ExistingFile],
)(implicit val consoleEnvironment: ConsoleEnvironment)
    extends PublicApiCommandRunner
    with NamedLogging {

  private val endpoint = sequencerConnection.endpoints.head1

  private val name: String = endpoint.toString

  override val loggerFactory: NamedLoggerFactory =
    consoleEnvironment.environment.loggerFactory.append("sequencer-public-api", name)

  protected[console] def publicApiCommand[Result](
      command: GrpcAdminCommand[?, ?, Result]
  ): ConsoleCommandResult[Result] =
    consoleEnvironment.grpcDomainCommandRunner
      .runCommand(
        sequencerConnection.sequencerAlias.unwrap,
        command,
        ClientConfig(
          endpoint.host,
          endpoint.port,
          tls = trustCollectionFile.map(f =>
            TlsClientConfig(trustCollectionFile = Some(f), clientCert = None)
          ),
        ),
        token = None,
      )
}

object ExternalLedgerApiClient {

  def forReference(
      participant: LocalParticipantReference,
      token: String,
  )(implicit
      env: ConsoleEnvironment
  ): ExternalLedgerApiClient = {
    val cc = participant.config.ledgerApi.clientConfig
    new ExternalLedgerApiClient(
      cc.address,
      cc.port,
      cc.tls,
      Some(token),
    )
  }
}

abstract class ParticipantReference(
    override val consoleEnvironment: ConsoleEnvironment,
    val name: String,
) extends InstanceReference
    with ConsoleCommandGroup
    with ParticipantAdministration
    with LedgerApiAdministration
    with LedgerApiCommandRunner {
  override type Status = ParticipantStatus

  override protected val loggerFactory: NamedLoggerFactory =
    consoleEnvironment.environment.loggerFactory.append("participant", name)

  override protected val instanceType: String = ParticipantReference.InstanceType
  override protected def runner: AdminCommandRunner = this

  @Help.Summary(
    "Yields the globally unique id of this participant. " +
      "Throws an exception, if the id has not yet been allocated (e.g., the participant has not yet been started)."
  )
  override def id: ParticipantId = topology.idHelper(ParticipantId(_))

  @Help.Summary(
    "Yields Some(id) of this participant if id present. " +
      "Returns None, if the id has not yet been allocated (e.g., the participant has not yet been initialised)."
  )
  override def maybeId: Option[ParticipantId] = topology.maybeIdHelper(ParticipantId(_))

  def config: BaseParticipantConfig

  @Help.Summary("Health and diagnostic related commands")
  @Help.Group("Health")
  override def health: ParticipantHealthAdministration =
    new ParticipantHealthAdministration(this, consoleEnvironment, loggerFactory)

  override def parties: ParticipantPartiesAdministrationGroup

  private lazy val topology_ =
    new TopologyAdministrationGroup(
      this,
      health.status.successOption.map(_.topologyQueue),
      consoleEnvironment,
      loggerFactory,
    )
  @Help.Summary("Topology management related commands")
  @Help.Group("Topology")
  @Help.Description("This group contains access to the full set of topology management commands.")
  override def topology: TopologyAdministrationGroup = topology_

  @Help.Summary("Commands used for development and testing", FeatureFlag.Testing)
  @Help.Group("Testing")
  def testing: ParticipantTestingGroup

  @Help.Summary("Commands to pruning the archive of the ledger", FeatureFlag.Preview)
  @Help.Group("Ledger Pruning")
  def pruning: ParticipantPruningAdministrationGroup = pruning_

  private lazy val pruning_ =
    new ParticipantPruningAdministrationGroup(this, consoleEnvironment, loggerFactory)

  @Help.Summary("Manage participant replication")
  @Help.Group("Replication")
  def replication: ParticipantReplicationAdministrationGroup = replicationGroup

  lazy private val replicationGroup =
    new ParticipantReplicationAdministrationGroup(this, consoleEnvironment)

  @Help.Summary("Commands to repair the participant contract state", FeatureFlag.Repair)
  @Help.Group("Repair")
  def repair: ParticipantRepairAdministration

  /** Waits until for every participant p (drawn from consoleEnvironment.participants.all) that is running and initialized
    * and for each domain to which both this participant and p are connected
    * the vetted_package transactions in the authorized store are the same as in the domain store.
    */
  override protected def waitPackagesVetted(timeout: NonNegativeDuration): Unit = {
    val connected = domains.list_connected().map(_.domainId).toSet
    // for every participant
    consoleEnvironment.participants.all
      .filter(p => p.health.is_running() && p.health.initialized())
      .foreach { participant =>
        // for every domain this participant is connected to as well
        participant.domains.list_connected().foreach {
          case item if connected.contains(item.domainId) =>
            ConsoleMacros.utils.retry_until_true(timeout)(
              {
                // ensure that vetted packages on the domain match the ones in the authorized store
                val onDomain = participant.topology.vetted_packages
                  .list(
                    filterStore = item.domainId.filterString,
                    filterParticipant = id.filterString,
                    timeQuery = TimeQuery.HeadState,
                  )
                  .flatMap(_.item.packageIds)
                  .toSet

                // Vetted packages from the participant's authorized store
                val onParticipantAuthorizedStore = topology.vetted_packages
                  .list(filterStore = "Authorized", filterParticipant = id.filterString)
                  .filter(_.item.domainId.forall(_ == item.domainId))
                  .flatMap(_.item.packageIds)
                  .toSet

                val ret = onParticipantAuthorizedStore == onDomain
                if (!ret) {
                  logger.debug(
                    show"Still waiting for package vetting updates to be observed by Participant ${participant.name} on ${item.domainId}: vetted -- onDomain is ${onParticipantAuthorizedStore -- onDomain} while onDomain -- vetted is ${onDomain -- onParticipantAuthorizedStore}"
                  )
                }
                ret
              },
              show"Participant ${participant.name} has not observed all vetting txs of $id on domain ${item.domainId} within the given timeout.",
            )
          case _ =>
        }
      }
  }
  override protected def participantIsActiveOnDomain(
      domainId: DomainId,
      participantId: ParticipantId,
  ): Boolean = topology.domain_trust_certificates.active(domainId, participantId)

}
object ParticipantReference {
  val InstanceType = "Participant"
}

class RemoteParticipantReference(environment: ConsoleEnvironment, override val name: String)
    extends ParticipantReference(environment, name)
    with RemoteInstanceReference {

  @Help.Summary("Inspect and manage parties")
  @Help.Group("Parties")
  override def parties: ParticipantPartiesAdministrationGroup = partiesGroup

  // above command needs to be def such that `Help` works.
  lazy private val partiesGroup =
    new ParticipantPartiesAdministrationGroup(id, this, consoleEnvironment)

  @Help.Summary("Return remote participant config")
  def config: RemoteParticipantConfig =
    consoleEnvironment.environment.config.remoteParticipantsByString(name)

  override protected[console] def ledgerApiCommand[Result](
      command: GrpcAdminCommand[?, ?, Result]
  ): ConsoleCommandResult[Result] =
    consoleEnvironment.grpcLedgerCommandRunner.runCommand(
      name,
      command,
      config.clientLedgerApi,
      config.token,
    )

  override protected[console] def token: Option[String] = config.token

  private lazy val testing_ = new ParticipantTestingGroup(this, consoleEnvironment, loggerFactory)

  @Help.Summary("Commands used for development and testing", FeatureFlag.Testing)
  @Help.Group("Testing")
  override def testing: ParticipantTestingGroup = testing_

  private lazy val repair_ =
    new ParticipantRepairAdministration(consoleEnvironment, this, loggerFactory)

  @Help.Summary("Commands to repair the participant contract state", FeatureFlag.Repair)
  @Help.Group("Repair")
  def repair: ParticipantRepairAdministration = repair_

  override def equals(obj: Any): Boolean = {
    obj match {
      case x: RemoteParticipantReference =>
        x.consoleEnvironment == consoleEnvironment && x.name == name
      case _ => false
    }
  }

}

class LocalParticipantReference(
    override val consoleEnvironment: ConsoleEnvironment,
    name: String,
) extends ParticipantReference(consoleEnvironment, name)
    with LocalInstanceReference
    with BaseInspection[ParticipantNode] {

  override private[console] val nodes = consoleEnvironment.environment.participants

  @Help.Summary("Return participant config")
  def config: LocalParticipantConfig =
    consoleEnvironment.environment.config.participantsByString(name)

  override def runningNode: Option[ParticipantNodeBootstrap] =
    consoleEnvironment.environment.participants.getRunning(name)

  override def startingNode: Option[ParticipantNodeBootstrap] =
    consoleEnvironment.environment.participants.getStarting(name)

  /** secret, not publicly documented way to get the admin token */
  def adminToken: Option[String] = underlying.map(_.adminToken.secret)

  // TODO(#14048) these are "remote" groups. the normal participant node has "local" versions.
  //   but rather than keeping this, we should make local == remote and add local methods separately
  @Help.Summary("Inspect and manage parties")
  @Help.Group("Parties")
  def parties: LocalParticipantPartiesAdministrationGroup = partiesGroup
  // above command needs to be def such that `Help` works.
  lazy private val partiesGroup =
    new LocalParticipantPartiesAdministrationGroup(this, this, consoleEnvironment, loggerFactory)

  private lazy val testing_ =
    new LocalParticipantTestingGroup(this, consoleEnvironment, loggerFactory)
  @Help.Summary("Commands used for development and testing", FeatureFlag.Testing)
  @Help.Group("Testing")
  override def testing: LocalParticipantTestingGroup = testing_

  private lazy val commitments_ =
    new LocalCommitmentsAdministrationGroup(this, consoleEnvironment, loggerFactory)
  @Help.Summary("Commands to inspect and extract bilateral commitments", FeatureFlag.Preview)
  @Help.Group("Commitments")
  def commitments: LocalCommitmentsAdministrationGroup = commitments_

  private lazy val repair_ =
    new LocalParticipantRepairAdministration(consoleEnvironment, this, loggerFactory) {
      override protected def access[T](handler: ParticipantNode => T): T =
        LocalParticipantReference.this.access(handler)
    }

  @Help.Summary("Commands to repair the local participant contract state", FeatureFlag.Repair)
  @Help.Group("Repair")
  def repair: LocalParticipantRepairAdministration = repair_

  override protected[console] def ledgerApiCommand[Result](
      command: GrpcAdminCommand[?, ?, Result]
  ): ConsoleCommandResult[Result] =
    runCommandIfRunning(
      consoleEnvironment.grpcLedgerCommandRunner
        .runCommand(name, command, config.clientLedgerApi, adminToken)
    )

  override protected[console] def token: Option[String] = adminToken
}

object SequencerReference {
  val InstanceType = "Sequencer"
}

abstract class SequencerReference(
    val consoleEnvironment: ConsoleEnvironment,
    name: String,
) extends InstanceReference
    with ConsoleCommandGroup {

  override type Status = SequencerNodeStatus

  override protected def runner: AdminCommandRunner = this

  private def disable_member(member: Member): Unit =
    repair.disable_member(member)

  override def equals(obj: Any): Boolean = {
    obj match {
      case x: SequencerReference =>
        x.consoleEnvironment == consoleEnvironment && x.name == name
      case _ => false
    }
  }

  override protected val instanceType: String = SequencerReference.InstanceType
  override protected val loggerFactory: NamedLoggerFactory =
    consoleEnvironment.environment.loggerFactory.append("sequencer", name)

  private lazy val topology_ =
    new TopologyAdministrationGroup(
      this,
      health.status.successOption.map(_.topologyQueue),
      consoleEnvironment,
      loggerFactory,
    )

  protected def publicApiClient: SequencerPublicApiClient

  def sequencerConnection: GrpcSequencerConnection

  override def topology: TopologyAdministrationGroup = topology_

  private lazy val parties_ = new PartiesAdministrationGroup(this, consoleEnvironment)

  override def parties: PartiesAdministrationGroup = parties_

  private val staticDomainParameters: AtomicReference[Option[ConsoleStaticDomainParameters]] =
    new AtomicReference[Option[ConsoleStaticDomainParameters]](None)

  private val domainId: AtomicReference[Option[DomainId]] =
    new AtomicReference[Option[DomainId]](None)

  @Help.Summary(
    "Yields the globally unique id of this sequencer. " +
      "Throws an exception, if the id has not yet been allocated (e.g., the sequencer has not yet been started)."
  )
  override def id: SequencerId = topology.idHelper(SequencerId(_))

  @Help.Summary(
    "Yields Some(id) of this sequencer if id present. " +
      "Returns None, if the id has not yet been allocated (e.g., the sequencer has not yet been initialised)."
  )
  override def maybeId: Option[SequencerId] = topology.maybeIdHelper(SequencerId(_))

  private lazy val setup_ = new SequencerSetupGroup(this)

  @Help.Summary("Methods used for node initialization")
  def setup: SequencerSetupGroup = setup_

  @Help.Summary("Health and diagnostic related commands")
  @Help.Group("Health")
  override def health =
    new HealthAdministration[SequencerNodeStatus](
      this,
      consoleEnvironment,
      SequencerNodeStatus.fromProtoV30,
    )

  private lazy val sequencerTrafficControl = new TrafficControlSequencerAdministrationGroup(
    this,
    consoleEnvironment,
    loggerFactory,
  )

  @Help.Summary("Admin traffic control related commands")
  @Help.Group("Traffic")
  override def traffic_control: TrafficControlSequencerAdministrationGroup =
    sequencerTrafficControl

  @Help.Summary("Return domain id of the domain")
  def domain_id: DomainId = {
    domainId.get() match {
      case Some(id) => id
      case None =>
        val id = consoleEnvironment.run(
          publicApiClient.publicApiCommand(SequencerPublicCommands.GetDomainId)
        )
        domainId.set(Some(id))

        id
    }
  }

  object mediators {
    object groups {
      @Help.Summary("Propose a new mediator group")
      @Help.Description("""
         group: the mediator group identifier
         threshold: the minimum number of mediators that need to come to a consensus for a message to be sent to other members.
         active: the list of mediators that will take part in the mediator consensus in this mediator group
         observers: the mediators that will receive all messages but will not participate in mediator consensus
         """)
      def propose_new_group(
          group: NonNegativeInt,
          threshold: PositiveInt,
          active: Seq[MediatorReference],
          observers: Seq[MediatorReference] = Nil,
      ): Unit = {

        val domainId = domain_id

        val mediators = active ++ observers

        mediators.foreach { mediator =>
          val identityState = mediator.topology.transactions.identity_transactions()

          topology.transactions.load(identityState, domainId.filterString, ForceFlag.AlienMember)
        }

        topology.mediators
          .propose(
            domainId = domainId,
            threshold = threshold,
            active = active.map(_.id),
            observers = observers.map(_.id),
            group = group,
          )
          .discard

        mediators.foreach(
          _.setup.assign(
            domainId,
            SequencerConnections.single(sequencerConnection),
          )
        )
      }

      @Help.Summary("Propose an update to a mediator group")
      @Help.Description("""
         group: the mediator group identifier
         threshold: the minimum number of mediators that need to come to a consensus for a message to be sent to other members.
         additionalActive: the new mediators that will take part in the mediator consensus in this mediator group
         additionalObservers: the new mediators that will receive all messages but will not participate in mediator consensus
         """)
      def propose_delta(
          group: NonNegativeInt,
          threshold: PositiveInt,
          additionalActive: Seq[MediatorReference],
          additionalObservers: Seq[MediatorReference] = Nil,
      ): Unit = {
        val domainId = domain_id

        val currentMediators = topology.mediators
          .list(filterStore = domainId.filterString, group = Some(group))
          .maxByOption(_.context.serial)
          .getOrElse(throw new IllegalArgumentException(s"Unknown mediator group $group"))

        val currentActive = currentMediators.item.active
        val currentObservers = currentMediators.item.observers
        val current = currentActive ++ currentObservers
        val serial = currentMediators.context.serial.increment

        val newMediators =
          (additionalActive ++ additionalObservers).filterNot(m => current.contains(m.id))

        newMediators.foreach { med =>
          val identityState = med.topology.transactions.identity_transactions()

          topology.transactions.load(identityState, domainId.filterString, ForceFlag.AlienMember)
        }

        topology.mediators
          .propose(
            domainId = domainId,
            threshold = threshold,
            active = (currentActive ++ additionalActive.map(_.id)).distinct,
            observers = (currentObservers ++ additionalObservers.map(_.id)).distinct,
            group = group,
            serial = Some(serial),
          )
          .discard

        newMediators.foreach(
          _.setup.assign(
            domainId,
            SequencerConnections.single(sequencerConnection),
          )
        )
      }
    }
  }

  @Help.Summary("Domain parameters related commands")
  @Help.Group("Domain parameters")
  object domain_parameters {
    object static {
      @Help.Summary("Return static domain parameters of the domain")
      def get(): ConsoleStaticDomainParameters = {
        staticDomainParameters.get() match {
          case Some(parameters) => parameters
          case None =>
            val parameters = consoleEnvironment.run(
              publicApiClient.publicApiCommand(SequencerPublicCommands.GetStaticDomainParameters)
            )

            staticDomainParameters.set(Some(parameters))
            parameters
        }
      }
    }
  }

  @Help.Summary("Pruning of the sequencer")
  object pruning
      extends PruningSchedulerAdministration(
        runner,
        consoleEnvironment,
        new PruningSchedulerCommands[SequencerPruningAdministrationServiceStub](
          SequencerPruningAdministrationServiceGrpc.stub,
          _.setSchedule(_),
          _.clearSchedule(_),
          _.setCron(_),
          _.setMaxDuration(_),
          _.setRetention(_),
          _.getSchedule(_),
        ),
        loggerFactory,
      )
      with Helpful {
    @Help.Summary("Status of the sequencer and its connected clients")
    @Help.Description(
      """Provides a detailed breakdown of information required for pruning:
        | - the current time according to this sequencer instance
        | - domain members that the sequencer supports
        | - for each member when they were registered and whether they are enabled
        | - a list of clients for each member, their last acknowledgement, and whether they are enabled
        |"""
    )
    def status(): SequencerPruningStatus =
      this.consoleEnvironment.run {
        runner.adminCommand(SequencerAdminCommands.GetPruningStatus)
      }

    @Help.Summary("Remove unnecessary data from the Sequencer up until the default retention point")
    @Help.Description(
      """Removes unnecessary data from the Sequencer that is earlier than the default retention period.
        |The default retention period is set in the configuration of the canton processing running this
        |command under `parameters.retention-period-defaults.sequencer`.
        |This pruning command requires that data is read and acknowledged by clients before
        |considering it safe to remove.
        |
        |If no data is being removed it could indicate that clients are not reading or acknowledging data
        |in a timely fashion (typically due to nodes going offline for long periods).
        |You have the option of disabling the members running on these nodes to allow removal of this data,
        |however this will mean that they will be unable to reconnect to the domain in the future.
        |To do this run `force_prune(dryRun = true)` to return a description of which members would be
        |disabled in order to prune the Sequencer.
        |If you are happy to disable the described clients then run `force_prune(dryRun = false)` to
        |permanently remove their unread data.
        |
        |Once offline clients have been disabled you can continue to run `prune` normally.
        |"""
    )
    def prune(): String = {
      val defaultRetention =
        this.consoleEnvironment.environment.config.parameters.retentionPeriodDefaults.sequencer
      prune_with_retention_period(defaultRetention.underlying)
    }

    @Help.Summary(
      "Force remove data from the Sequencer including data that may have not been read by offline clients"
    )
    @Help.Description(
      """Will force pruning up until the default retention period by potentially disabling clients
        |that have not yet read data we would like to remove.
        |Disabling these clients will prevent them from ever reconnecting to the Domain so should only be
        |used if the Domain operator is confident they can be permanently ignored.
        |Run with `dryRun = true` to review a description of which clients will be disabled first.
        |Run with `dryRun = false` to disable these clients and perform a forced pruning.
        |"""
    )
    def force_prune(dryRun: Boolean): String = {
      val defaultRetention =
        this.consoleEnvironment.environment.config.parameters.retentionPeriodDefaults.sequencer
      force_prune_with_retention_period(defaultRetention.underlying, dryRun)
    }

    @Help.Summary("Remove data that has been read up until a custom retention period")
    @Help.Description(
      "Similar to the above `prune` command but allows specifying a custom retention period"
    )
    def prune_with_retention_period(retentionPeriod: FiniteDuration): String = {
      val status = this.status()
      val pruningTimestamp = status.now.minus(retentionPeriod.toJava)

      prune_at(pruningTimestamp)
    }

    @Help.Summary(
      "Force removing data from the Sequencer including data that may have not been read by offline clients up until a custom retention period"
    )
    @Help.Description(
      "Similar to the above `force_prune` command but allows specifying a custom retention period"
    )
    def force_prune_with_retention_period(
        retentionPeriod: FiniteDuration,
        dryRun: Boolean,
    ): String = {
      val status = this.status()
      val pruningTimestamp = status.now.minus(retentionPeriod.toJava)

      force_prune_at(pruningTimestamp, dryRun)
    }

    @Help.Summary("Remove data that has been read up until the specified time")
    @Help.Description(
      """Similar to the above `prune` command but allows specifying the exact time at which to prune.
        |The command will fail if a client has not yet read and acknowledged some data up to the specified time."""
    )
    def prune_at(timestamp: CantonTimestamp): String = {
      this.consoleEnvironment.run {
        runner.adminCommand(EnterpriseSequencerAdminCommands.Prune(timestamp))
      }
    }

    @Help.Summary(
      "Force removing data from the Sequencer including data that may have not been read by offline clients up until the specified time"
    )
    @Help.Description(
      "Similar to the above `force_prune` command but allows specifying the exact time at which to prune"
    )
    def force_prune_at(timestamp: CantonTimestamp, dryRun: Boolean): String = {
      val initialStatus = status()
      val clientsToDisable = initialStatus.clientsPreventingPruning(timestamp)

      if (dryRun) {
        formatDisableDryRun(timestamp, clientsToDisable)
      } else {
        clientsToDisable.members.toSeq.foreach(disable_member)

        // check we can now prune for the provided timestamp
        val statusAfterDisabling = status()
        val safeTimestamp = statusAfterDisabling.safePruningTimestamp

        if (safeTimestamp < timestamp)
          sys.error(
            s"We disabled all clients preventing pruning at $timestamp however the safe timestamp is set to $safeTimestamp"
          )

        val pruneMsg = prune_at(timestamp)
        if (clientsToDisable.members.nonEmpty) {
          s"$pruneMsg\nDisabled the following members:${clientsToDisable.members.toSeq.map(_.toString).sorted.mkString("\n  - ", "\n  - ", "\n")}"
        } else {
          pruneMsg
        }
      }
    }

    private def formatDisableDryRun(
        timestamp: CantonTimestamp,
        toDisable: SequencerClients,
    ): String = {
      val toDisableText =
        toDisable.members.toSeq.map(member => show"- $member").map(m => s"  $m (member)").sorted

      if (toDisableText.isEmpty) {
        show"The Sequencer can be safely pruned for $timestamp without disabling clients"
      } else {
        val sb = new StringBuilder()
        sb.append(s"To prune the Sequencer at $timestamp we will disable:")
        toDisableText foreach { item =>
          sb.append(System.lineSeparator())
          sb.append(item)
        }
        sb.append(System.lineSeparator())
        sb.append(
          "To disable these clients to allow for pruning at this point run force_prune with dryRun set to false"
        )
        sb.toString()
      }
    }

    @Help.Summary("Obtain a timestamp at or near the beginning of sequencer state")
    @Help.Description(
      """This command provides insight into the current state of sequencer pruning when called with
        |the default value of `index` 1.
        |When pruning the sequencer manually via `prune_at` and with the intent to prune in batches, specify
        |a value such as 1000 to obtain a pruning timestamp that corresponds to the "end" of the batch."""
    )
    def locate_pruning_timestamp(
        index: PositiveInt = PositiveInt.tryCreate(1)
    ): Option[CantonTimestamp] =
      check(FeatureFlag.Preview) {
        this.consoleEnvironment.run {
          runner.adminCommand(LocatePruningTimestampCommand(index))
        }
      }

  }

  @Help.Summary("Methods used for repairing the node")
  object repair {

    /** Disable the provided member at the sequencer preventing them from reading and writing, and allowing their
      * data to be pruned.
      */
    @Help.Summary(
      "Disable the provided member at the Sequencer that will allow any unread data for them to be removed"
    )
    @Help.Description(
      """This will prevent any client for the given member to reconnect the Sequencer
        |and allow any unread/unacknowledged data they have to be removed.
        |This should only be used if the domain operation is confident the member will never need
        |to reconnect as there is no way to re-enable the member.
        |To view members using the sequencer run `sequencer.status()`.""""
    )
    def disable_member(member: Member): Unit = consoleEnvironment.run {
      runner.adminCommand(EnterpriseSequencerAdminCommands.DisableMember(member))
    }
  }

  @Help.Summary("Methods used to manage BFT sequencers; they'll fail on non-BFT sequencers")
  object bft {

    @Help.Summary("Add a new peer endpoint")
    def add_peer_endpoint(endpoint: Endpoint): Unit = consoleEnvironment.run {
      runner.adminCommand(EnterpriseSequencerBftAdminCommands.AddPeerEndpoint(endpoint))
    }

    @Help.Summary("Remove a peer endpoint")
    def remove_peer_endpoint(endpoint: Endpoint): Unit = consoleEnvironment.run {
      runner.adminCommand(EnterpriseSequencerBftAdminCommands.RemovePeerEndpoint(endpoint))
    }

    @Help.Summary("Get peer network status")
    def get_peer_network_status(endpoints: Option[Iterable[Endpoint]]): PeerNetworkStatus =
      consoleEnvironment.run {
        runner.adminCommand(EnterpriseSequencerBftAdminCommands.GetPeerNetworkStatus(endpoints))
      }
  }
}

class LocalSequencerReference(
    override val consoleEnvironment: ConsoleEnvironment,
    val name: String,
) extends SequencerReference(consoleEnvironment, name)
    with LocalInstanceReference
    with BaseInspection[SequencerNode] {

  override protected[canton] def executionContext: ExecutionContext =
    consoleEnvironment.environment.executionContext

  @Help.Summary("Returns the sequencerx configuration")
  override def config: SequencerNodeConfigCommon =
    consoleEnvironment.environment.config.sequencersByString(name)

  override lazy val sequencerConnection: GrpcSequencerConnection =
    config.publicApi.toSequencerConnectionConfig.toConnection
      .fold(err => sys.error(s"Sequencer $name has invalid connection config: $err"), identity)

  private[console] val nodes: SequencerNodes[?] =
    consoleEnvironment.environment.sequencers

  override protected[console] def runningNode: Option[SequencerNodeBootstrap] =
    nodes.getRunning(name)

  override protected[console] def startingNode: Option[SequencerNodeBootstrap] =
    nodes.getStarting(name)

  protected lazy val publicApiClient: SequencerPublicApiClient = new SequencerPublicApiClient(
    sequencerConnection = sequencerConnection,
    trustCollectionFile = config.publicApi.tls.map(_.certChainFile),
  )(consoleEnvironment)
}

class RemoteSequencerReference(val environment: ConsoleEnvironment, val name: String)
    extends SequencerReference(environment, name)
    with RemoteInstanceReference {

  override protected[canton] def executionContext: ExecutionContext =
    consoleEnvironment.environment.executionContext

  @Help.Summary("Returns the remote sequencer configuration")
  def config: RemoteSequencerConfig =
    environment.environment.config.remoteSequencersByString(name)

  override def sequencerConnection: GrpcSequencerConnection =
    config.publicApi.toConnection
      .fold(err => sys.error(s"Sequencer $name has invalid connection config: $err"), identity)

  protected lazy val publicApiClient: SequencerPublicApiClient = new SequencerPublicApiClient(
    sequencerConnection = sequencerConnection,
    trustCollectionFile = config.publicApi.customTrustCertificates.map(_.pemFile),
  )(consoleEnvironment)
}

object MediatorReference {
  val InstanceType = "Mediator"
}

abstract class MediatorReference(val consoleEnvironment: ConsoleEnvironment, name: String)
    extends InstanceReference
    with ConsoleCommandGroup {
  override type Status = MediatorNodeStatus

  override protected def runner: AdminCommandRunner = this

  override protected val instanceType: String = MediatorReference.InstanceType
  override protected val loggerFactory: NamedLoggerFactory =
    consoleEnvironment.environment.loggerFactory
      .append(MediatorNodeBootstrap.LoggerFactoryKeyName, name)

  @Help.Summary(
    "Yields the mediator id of this mediator. " +
      "Throws an exception, if the id has not yet been allocated (e.g., the mediator has not yet been initialised)."
  )
  override def id: MediatorId = topology.idHelper(MediatorId(_))

  @Help.Summary(
    "Yields Some(id) of this mediator if id present. " +
      "Returns None, if the id has not yet been allocated (e.g., the mediator has not yet been initialised)."
  )
  override def maybeId: Option[MediatorId] = topology.maybeIdHelper(MediatorId(_))

  @Help.Summary("Health and diagnostic related commands")
  @Help.Group("Health")
  override def health =
    new HealthAdministration[MediatorNodeStatus](
      this,
      consoleEnvironment,
      MediatorNodeStatus.fromProtoV30,
    )

  private lazy val topology_ =
    new TopologyAdministrationGroup(
      this,
      health.status.successOption.map(_.topologyQueue),
      consoleEnvironment,
      loggerFactory,
    )

  override def topology: TopologyAdministrationGroup = topology_

  private lazy val parties_ = new PartiesAdministrationGroup(this, consoleEnvironment)

  override def parties: PartiesAdministrationGroup = parties_

  override def equals(obj: Any): Boolean =
    obj match {
      case x: MediatorReference => x.consoleEnvironment == consoleEnvironment && x.name == name
      case _ => false
    }

  private lazy val setup_ = new MediatorSetupGroup(this)

  @Help.Summary("Methods used to initialize the node")
  def setup: MediatorSetupGroup = setup_

  private lazy val testing_ = new MediatorTestingGroup(runner, consoleEnvironment, loggerFactory)

  @Help.Summary("Testing functionality for the mediator")
  @Help.Group("Testing")
  def testing: MediatorTestingGroup = testing_

  private lazy val pruning_ =
    new MediatorPruningAdministrationGroup(runner, consoleEnvironment, loggerFactory)

  @Help.Summary("Pruning functionality for the mediator")
  @Help.Group("Testing")
  def pruning: MediatorPruningAdministrationGroup = pruning_
}

class LocalMediatorReference(consoleEnvironment: ConsoleEnvironment, val name: String)
    extends MediatorReference(consoleEnvironment, name)
    with LocalInstanceReference
    with SequencerConnectionAdministration
    with BaseInspection[MediatorNode] {

  override protected[canton] def executionContext: ExecutionContext =
    consoleEnvironment.environment.executionContext

  @Help.Summary("Returns the mediator configuration")
  override def config: MediatorNodeConfigCommon =
    consoleEnvironment.environment.config.mediatorsByString(name)

  private[console] val nodes: MediatorNodes[?] = consoleEnvironment.environment.mediators

  override protected[console] def runningNode: Option[MediatorNodeBootstrap] =
    nodes.getRunning(name)

  override protected[console] def startingNode: Option[MediatorNodeBootstrap] =
    nodes.getStarting(name)
}

class RemoteMediatorReference(val environment: ConsoleEnvironment, val name: String)
    extends MediatorReference(environment, name)
    with RemoteInstanceReference {

  @Help.Summary("Returns the remote mediator configuration")
  def config: RemoteMediatorConfig =
    environment.environment.config.remoteMediatorsByString(name)

  override protected[canton] def executionContext: ExecutionContext =
    consoleEnvironment.environment.executionContext
}
