// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import ammonite.util.Bind
import cats.syntax.either.*
import com.digitalasset.canton.admin.api.client.data.CantonStatus
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveDouble, PositiveInt}
import com.digitalasset.canton.config.{
  ConsoleCommandTimeout,
  NonNegativeDuration,
  NonNegativeFiniteDuration,
  PositiveDurationSeconds,
  ProcessingTimeout,
}
import com.digitalasset.canton.console.CommandErrors.{
  CantonCommandError,
  CommandInternalError,
  GenericCommandError,
}
import com.digitalasset.canton.console.Help.{Description, Summary, Topic}
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.protocol.SerializableContract
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnection,
  SequencerConnections,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext, TracerProvider}
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.{DomainAlias, LfPartyId}
import com.typesafe.scalalogging.Logger
import io.opentelemetry.api.trace.Tracer

import java.time.{Duration as JDuration, Instant}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.Duration as SDuration
import scala.reflect.runtime.universe as ru
import scala.util.control.NonFatal

final case class NodeReferences[A, R <: A, L <: A](
    local: Seq[L],
    remote: Seq[R],
) {
  val all: Seq[A] = local ++ remote
}

object NodeReferences {
  type ParticipantNodeReferences =
    NodeReferences[ParticipantReference, RemoteParticipantReference, LocalParticipantReference]
}

/** The environment in which console commands are evaluated.
  */
@SuppressWarnings(Array("org.wartremover.warts.Any")) // required for `Binding[_]` usage
trait ConsoleEnvironment extends NamedLogging with FlagCloseable with NoTracing {
  type Env <: Environment
  type Status <: CantonStatus

  def consoleLogger: Logger = super.noTracingLogger

  def health: CantonHealthAdministration[Status]

  /** the underlying Canton runtime environment */
  val environment: Env

  /** determines the control exception thrown on errors */
  val errorHandler: ConsoleErrorHandler = ThrowErrorHandler

  /** the console for user facing output */
  val consoleOutput: ConsoleOutput

  /** The predef code itself which is executed before any script or repl command */
  private[console] def predefCode(interactive: Boolean, noTty: Boolean = false): String =
    consoleEnvironmentBindings.predefCode(interactive, noTty)
  protected def consoleEnvironmentBindings: ConsoleEnvironmentBinding

  private val tracerProvider =
    TracerProvider.Factory(environment.configuredOpenTelemetry, "console")
  private[console] val tracer: Tracer = tracerProvider.tracer

  /** Definition of the startup order of local instances.
    * Nodes support starting up in any order however to avoid delays/warnings we opt to start in the most desirable order
    * for simple execution. (e.g. domains started before participants).
    * Implementations should just return a int for the instance (typically just a static value based on type),
    * and then the console will start these instances for lower to higher values.
    */
  protected def startupOrderPrecedence(instance: LocalInstanceReference): Int

  /** The order that local nodes would ideally be started in. */
  final val startupOrdering: Ordering[LocalInstanceReference] =
    (x: LocalInstanceReference, y: LocalInstanceReference) =>
      startupOrderPrecedence(x) compare startupOrderPrecedence(y)

  /** allows for injecting a custom admin command runner during tests
    * @param apiName API name checked against and expected on the server-side
    */
  protected def createAdminCommandRunner(
      consoleEnvironment: ConsoleEnvironment,
      apiName: String,
  ): ConsoleGrpcAdminCommandRunner = new ConsoleGrpcAdminCommandRunner(consoleEnvironment, apiName)

  protected override val loggerFactory: NamedLoggerFactory = environment.loggerFactory

  private val commandTimeoutReference: AtomicReference[ConsoleCommandTimeout] =
    new AtomicReference[ConsoleCommandTimeout](environment.config.parameters.timeouts.console)

  private val featureSetReference: AtomicReference[HelperItems] =
    new AtomicReference[HelperItems](HelperItems(environment.config.features.featureFlags))

  private case class HelperItems(scope: Set[FeatureFlag]) {
    lazy val participantHelperItems = {
      // due to the use of reflection to grab the help-items, i need to write the following, repetitive stuff explicitly
      val subItems =
        if (participants.local.nonEmpty)
          participants.local.headOption.toList.flatMap(p =>
            Help.getItems(p, baseTopic = Seq("$participant"), scope = scope)
          )
        else if (participants.remote.nonEmpty)
          participants.remote.headOption.toList.flatMap(p =>
            Help.getItems(p, baseTopic = Seq("$participant"), scope = scope)
          )
        else Seq()
      Help.Item("$participant", None, Summary(""), Description(""), Topic(Seq()), subItems)
    }

    lazy val filteredHelpItems = {
      helpItems.filter(x => scope.contains(x.summary.flag))
    }

    lazy val all = filteredHelpItems :+ participantHelperItems

  }

  protected def timeouts: ProcessingTimeout = environment.config.parameters.timeouts.processing

  /** @return maximum runtime of a console command
    */
  def commandTimeouts: ConsoleCommandTimeout = commandTimeoutReference.get()

  def setCommandTimeout(newTimeout: NonNegativeDuration): Unit = {
    require(newTimeout.duration > SDuration.Zero, "The command timeout must be positive!")
    commandTimeoutReference.updateAndGet(cur => cur.copy(bounded = newTimeout)).discard
  }

  def setLedgerCommandTimeout(newTimeout: NonNegativeDuration): Unit = {
    require(newTimeout.duration > SDuration.Zero, "The ledger command timeout must be positive!")
    commandTimeoutReference.updateAndGet(cur => cur.copy(ledgerCommand = newTimeout)).discard
  }

  /** returns the currently enabled feature sets */
  def featureSet: Set[FeatureFlag] = featureSetReference.get().scope

  def updateFeatureSet(flag: FeatureFlag, include: Boolean): Unit = {
    val _ = featureSetReference.updateAndGet { x =>
      val scope = if (include) x.scope + flag else x.scope - flag
      HelperItems(scope)
    }
  }

  /** Holder for top level values including their name, their value, and a description to display when `help` is printed.
    */
  protected case class TopLevelValue[T](
      nameUnsafe: String,
      summary: String,
      value: T,
      topic: Seq[String] = Seq(),
  )(implicit tag: ru.TypeTag[T]) {

    // Surround with back-ticks to handle the case that name is a reserved keyword in scala.
    lazy val asBind: Either[InstanceName.InvalidInstanceName, Bind[T]] =
      InstanceName.create(nameUnsafe).map(name => Bind(s"`${name.unwrap}`", value))

    lazy val asHelpItem: Help.Item =
      Help.Item(nameUnsafe, None, Help.Summary(summary), Help.Description(""), Help.Topic(topic))
  }

  object TopLevelValue {

    /** Provide all details but the value itself. A subsequent call can then specify the value from another location.
      * This oddness is to allow the ConsoleEnvironment implementations to specify the values of node instances they
      * use as scala's runtime reflection can't easily take advantage of the type members we have available here.
      */
    case class Partial(name: String, summary: String, topics: Seq[String] = Seq.empty) {
      def apply[T](value: T)(implicit t: ru.TypeTag[T]): TopLevelValue[T] =
        TopLevelValue(name, summary, value, topics)
    }
  }

  // lazy to prevent publication of this before this has been fully initialized
  lazy val grpcAdminCommandRunner: ConsoleGrpcAdminCommandRunner =
    createAdminCommandRunner(this, CantonGrpcUtil.ApiName.AdminApi)

  lazy val grpcLedgerCommandRunner: ConsoleGrpcAdminCommandRunner =
    createAdminCommandRunner(this, CantonGrpcUtil.ApiName.LedgerApi)

  lazy val grpcDomainCommandRunner: ConsoleGrpcAdminCommandRunner =
    createAdminCommandRunner(this, CantonGrpcUtil.ApiName.SequencerPublicApi)

  def runE[E, A](result: => Either[E, A]): A = {
    run(ConsoleCommandResult.fromEither(result.leftMap(_.toString)))
  }

  /** Run a console command.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def run[A](result: => ConsoleCommandResult[A]): A = {
    val resultValue: ConsoleCommandResult[A] =
      try {
        result
      } catch {
        case err: Throwable =>
          CommandInternalError.ErrorWithException(err).logWithContext()
          err match {
            case NonFatal(_) =>
              // No need to rethrow err, as it has been logged and output
              errorHandler.handleInternalError()
            case _ =>
              // Rethrow err, as it is a bad practice to discard fatal errors.
              // As a result, the error may be printed several times,
              // but there is no guarantee that the log is still working.
              // So it is better to err on the safe side.
              throw err
          }
      }

    def invocationContext(): Map[String, String] =
      findInvocationSite() match {
        case Some((funcName, callSite)) => Map("function" -> funcName, "callsite" -> callSite)
        case None => Map()
      }

    resultValue match {
      case null =>
        CommandInternalError.NullError().logWithContext(invocationContext())
        errorHandler.handleInternalError()
      case CommandSuccessful(value) =>
        value
      case err: CantonCommandError =>
        err.logWithContext(invocationContext())
        errorHandler.handleCommandFailure()
      case err: GenericCommandError =>
        val errMsg = findInvocationSite() match {
          case Some((funcName, site)) =>
            err.cause + s"\n  Command ${funcName} invoked from ${site}"
          case None => err.cause
        }
        logger.error(errMsg)
        errorHandler.handleCommandFailure()
    }
  }

  private def findInvocationSite(): Option[(String, String)] = {
    val stack = Thread.currentThread().getStackTrace
    // assumption: first few stack elements are all in our set of known packages. our call-site is
    // the first entry outside of our package
    // also skip all scala packages because a collection's map operation is not an informative call site
    val myPackages =
      Seq("com.digitalasset.canton.console", "com.digitalasset.canton.environment", "scala.")

    def isKnown(element: StackTraceElement): Boolean =
      myPackages.exists(element.getClassName.startsWith)

    stack.sliding(2).collectFirst {
      case Array(callee, caller) if isKnown(callee) && !isKnown(caller) =>
        val drop = callee.getClassName.lastIndexOf(".") + 1
        val funcName = callee.getClassName.drop(drop) + "." + callee.getMethodName
        (funcName, s"${caller.getFileName}:${caller.getLineNumber}")
    }

  }

  /** Print help for items in the top level scope.
    */
  def help(): Unit = {
    consoleOutput.info(Help.format(featureSetReference.get().filteredHelpItems*))
  }

  /** Print detailed help for a top-level item in the top level scope.
    */
  def help(cmd: String): Unit =
    consoleOutput.info(Help.forMethod(featureSetReference.get().all, cmd))

  def helpItems: Seq[Help.Item] =
    topLevelValues.map(_.asHelpItem) ++
      Help.fromObject(ConsoleMacros) ++
      Help.fromObject(this) :+
      (
        Help.Item(
          "help",
          None,
          Help.Summary(
            "Help with console commands; type help(\"<command>\") for detailed help for <command>"
          ),
          Help.Description(""),
          Help.Topic(Help.defaultTopLevelTopic),
        ),
      ) :+
      (Help.Item(
        "exit",
        None,
        Help.Summary("Leave the console"),
        Help.Description(""),
        Help.Topic(Help.defaultTopLevelTopic),
      ))

  lazy val participants: NodeReferences[
    ParticipantReference,
    RemoteParticipantReference,
    LocalParticipantReference,
  ] =
    NodeReferences(
      environment.config.participantsByString.keys.map(createParticipantReference).toSeq,
      environment.config.remoteParticipantsByString.keys
        .map(createRemoteParticipantReference)
        .toSeq,
    )

  lazy val sequencers: NodeReferences[
    SequencerReference,
    RemoteSequencerReference,
    LocalSequencerReference,
  ] =
    NodeReferences(
      environment.config.sequencersByString.keys.map(createSequencerReference).toSeq,
      environment.config.remoteSequencersByString.keys.map(createRemoteSequencerReference).toSeq,
    )

  lazy val mediators
      : NodeReferences[MediatorReference, RemoteMediatorReference, LocalMediatorReference] =
    NodeReferences(
      environment.config.mediatorsByString.keys.map(createMediatorReference).toSeq,
      environment.config.remoteMediatorsByString.keys.map(createRemoteMediatorReference).toSeq,
    )

  // the scala compiler / wartremover gets confused here if I use ++ directly
  def mergeLocalInstances(
      locals: Seq[LocalInstanceReference]*
  ): Seq[LocalInstanceReference] =
    locals.flatten

  def mergeRemoteInstances(remotes: Seq[InstanceReference]*): Seq[InstanceReference] =
    remotes.flatten

  lazy val nodes: NodeReferences[
    InstanceReference,
    InstanceReference,
    LocalInstanceReference,
  ] = {
    NodeReferences(
      mergeLocalInstances(
        participants.local,
        sequencers.local,
        mediators.local,
      ),
      mergeRemoteInstances(
        participants.remote,
        sequencers.remote,
        mediators.remote,
      ),
    )
  }

  protected def helpText(typeName: String, name: String) =
    s"Manage $typeName '${name}'; type '${name} help' or '${name} help" + "(\"<methodName>\")' for more help"

  protected val topicNodeReferences = "Node References"
  protected val topicGenericNodeReferences = "Generic Node References"
  protected val genericNodeReferencesDoc = " (.all, .local, .remote)"

  /** Assemble top level values with their identifier name, value binding, and help description.
    */
  protected def topLevelValues: Seq[TopLevelValue[_]] = {
    val nodeTopic = Seq(topicNodeReferences)
    val localParticipantBinds: Seq[TopLevelValue[_]] =
      participants.local.map(p =>
        TopLevelValue(p.name, helpText("participant", p.name), p, nodeTopic)
      )
    val remoteParticipantBinds: Seq[TopLevelValue[_]] =
      participants.remote.map(p =>
        TopLevelValue(p.name, helpText("remote participant", p.name), p, nodeTopic)
      )
    val localMediatorBinds: Seq[TopLevelValue[_]] =
      mediators.local.map(d =>
        TopLevelValue(d.name, helpText("local mediator", d.name), d, nodeTopic)
      )
    val remoteMediatorBinds: Seq[TopLevelValue[_]] =
      mediators.remote.map(d =>
        TopLevelValue(d.name, helpText("remote mediator", d.name), d, nodeTopic)
      )
    val localSequencerBinds: Seq[TopLevelValue[_]] =
      sequencers.local.map(d =>
        TopLevelValue(d.name, helpText("local sequencer", d.name), d, nodeTopic)
      )
    val remoteSequencerBinds: Seq[TopLevelValue[_]] =
      sequencers.remote.map(d =>
        TopLevelValue(d.name, helpText("remote sequencer", d.name), d, nodeTopic)
      )
    val clockBinds: Option[TopLevelValue[_]] =
      environment.simClock.map(cl =>
        TopLevelValue("clock", "Simulated time", new SimClockCommand(cl))
      )
    val referencesTopic = Seq(topicGenericNodeReferences)
    localParticipantBinds ++ remoteParticipantBinds ++
      localSequencerBinds ++ remoteSequencerBinds ++ localMediatorBinds ++ remoteMediatorBinds ++ clockBinds.toList :+
      TopLevelValue(
        "participants",
        "All participant nodes" + genericNodeReferencesDoc,
        participants,
        referencesTopic,
      ) :+
      TopLevelValue(
        "mediators",
        "All mediator nodes" + genericNodeReferencesDoc,
        mediators,
        referencesTopic,
      ) :+
      TopLevelValue(
        "sequencers",
        "All sequencer nodes" + genericNodeReferencesDoc,
        sequencers,
        referencesTopic,
      ) :+
      TopLevelValue("nodes", "All nodes" + genericNodeReferencesDoc, nodes, referencesTopic)
  }

  /** Bindings for ammonite
    * Add a reference to this instance to resolve implicit references within the console
    */
  lazy val bindings: Either[RuntimeException, IndexedSeq[Bind[_]]] = {
    import cats.syntax.traverse.*
    for {
      bindsWithoutSelfAlias <- topLevelValues.traverse(_.asBind)
      binds = bindsWithoutSelfAlias :+ selfAlias()
      _ <- validateNameUniqueness(binds)
    } yield binds.toIndexedSeq
  }

  private def validateNameUniqueness(binds: Seq[Bind[_]]) = {
    val nonUniqueNames =
      binds.map(_.name).groupBy(identity).collect {
        case (name, occurrences) if occurrences.sizeIs > 1 =>
          s"$name (${occurrences.size} occurrences)"
      }
    EitherUtil.condUnitE(
      nonUniqueNames.isEmpty,
      new IllegalStateException(
        s"""Node names must be unique and must differ from reserved keywords. Please revisit node names in your config file.
         |Offending names: ${nonUniqueNames.mkString("(", ", ", ")")}""".stripMargin
      ),
    )
  }

  private def createParticipantReference(name: String): LocalParticipantReference =
    new LocalParticipantReference(this, name)
  private def createRemoteParticipantReference(name: String): RemoteParticipantReference =
    new RemoteParticipantReference(this, name)

  private def createSequencerReference(name: String): LocalSequencerReference =
    new LocalSequencerReference(this, name)

  private def createRemoteSequencerReference(name: String): RemoteSequencerReference =
    new RemoteSequencerReference(this, name)

  private def createMediatorReference(name: String): LocalMediatorReference =
    new LocalMediatorReference(this, name)

  private def createRemoteMediatorReference(name: String): RemoteMediatorReference =
    new RemoteMediatorReference(this, name)

  /** So we can we make this available
    */
  protected def selfAlias(): Bind[_] = Bind(ConsoleEnvironmentBinding.BindingName, this)

  override def onClosed(): Unit = {
    Lifecycle.close(
      grpcAdminCommandRunner,
      grpcLedgerCommandRunner,
      grpcDomainCommandRunner,
      environment,
    )(logger)
  }

  def closeChannels(): Unit = {
    grpcAdminCommandRunner.closeChannels()
    grpcLedgerCommandRunner.closeChannels()
    grpcDomainCommandRunner.closeChannels()
  }

  def startAll(): Unit = runE(environment.startAll())

  def stopAll(): Unit = runE(environment.stopAll())

}

/** Expose a Canton [[environment.Environment]] in a way that's easy to deal with from a REPL.
  */
object ConsoleEnvironment {

  trait Implicits {

    import scala.language.implicitConversions

    implicit def toInstanceReferenceExtensions(
        instances: Seq[LocalInstanceReference]
    ): LocalInstancesExtensions[LocalInstanceReference] =
      new LocalInstancesExtensions.Impl(instances)

    /** Implicit maps an LfPartyId to a PartyId */
    implicit def toPartId(lfPartyId: LfPartyId): PartyId = PartyId.tryFromLfParty(lfPartyId)

    /** Extensions for many participant references
      */
    implicit def toParticipantReferencesExtensions(participants: Seq[ParticipantReference])(implicit
        consoleEnvironment: ConsoleEnvironment
    ): ParticipantReferencesExtensions =
      new ParticipantReferencesExtensions(participants)

    implicit def toLocalParticipantReferencesExtensions(
        participants: Seq[LocalParticipantReference]
    )(implicit
        consoleEnvironment: ConsoleEnvironment
    ): LocalParticipantReferencesExtensions =
      new LocalParticipantReferencesExtensions(participants)

    /** Implicitly map strings to DomainAlias, Fingerprint and Identifier
      */
    implicit def toDomainAlias(alias: String): DomainAlias = DomainAlias.tryCreate(alias)
    implicit def toDomainAliases(aliases: Seq[String]): Seq[DomainAlias] =
      aliases.map(DomainAlias.tryCreate)

    implicit def toInstanceName(name: String): InstanceName = InstanceName.tryCreate(name)

    implicit def toGrpcSequencerConnection(connection: String): SequencerConnection =
      GrpcSequencerConnection.tryCreate(connection)

    implicit def toSequencerConnections(connection: String): SequencerConnections =
      SequencerConnections.single(GrpcSequencerConnection.tryCreate(connection))

    implicit def toGSequencerConnection(
        ref: SequencerReference
    ): SequencerConnection =
      ref.sequencerConnection

    implicit def toGSequencerConnections(
        ref: SequencerReference
    ): SequencerConnections =
      SequencerConnections.single(ref.sequencerConnection)

    implicit def toFingerprint(fp: String): Fingerprint = Fingerprint.tryCreate(fp)

    /** Implicitly map ParticipantReferences to the ParticipantId
      */
    implicit def toParticipantId(reference: ParticipantReference): ParticipantId = reference.id

    /** Implicitly map an `Int` to a `NonNegativeInt`.
      * @throws java.lang.IllegalArgumentException if `n` is negative
      */
    implicit def toNonNegativeInt(n: Int): NonNegativeInt = NonNegativeInt.tryCreate(n)

    /** Implicitly map an `Int` to a `PositiveInt`.
      * @throws java.lang.IllegalArgumentException if `n` is not positive
      */
    implicit def toPositiveInt(n: Int): PositiveInt = PositiveInt.tryCreate(n)

    /** Implicitly map a Double to a `PositiveDouble`
      * @throws java.lang.IllegalArgumentException if `n` is not positive
      */
    implicit def toPositiveDouble(n: Double): PositiveDouble = PositiveDouble.tryCreate(n)

    /** Implicitly map a `CantonTimestamp` to a `LedgerCreateTime`
      */
    implicit def toLedgerCreateTime(ts: CantonTimestamp): SerializableContract.LedgerCreateTime =
      SerializableContract.LedgerCreateTime(ts)

    /** Implicitly convert a duration to a [[com.digitalasset.canton.config.NonNegativeDuration]]
      * @throws java.lang.IllegalArgumentException if `duration` is negative
      */
    implicit def durationToNonNegativeDuration(duration: SDuration): NonNegativeDuration =
      NonNegativeDuration.tryFromDuration(duration)

    /** Implicitly convert a duration to a [[com.digitalasset.canton.config.NonNegativeFiniteDuration]]
      * @throws java.lang.IllegalArgumentException if `duration` is negative or infinite
      */
    implicit def durationToNonNegativeFiniteDuration(
        duration: SDuration
    ): NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.tryFromDuration(duration)

    /** Implicitly convert a duration to a [[com.digitalasset.canton.config.PositiveDurationSeconds]]
      *
      * @throws java.lang.IllegalArgumentException if `duration` is not positive or not rounded to the second.
      */
    implicit def durationToPositiveDurationRoundedSeconds(
        duration: SDuration
    ): PositiveDurationSeconds =
      PositiveDurationSeconds.tryFromDuration(duration)
  }

  object Implicits extends Implicits

}

class SimClockCommand(clock: SimClock) {

  @Help.Description("Get current time")
  def now: Instant = clock.now.toInstant

  @Help.Description("Advance time to given time-point")
  def advanceTo(timestamp: Instant): Unit = TraceContext.withNewTraceContext {
    implicit traceContext =>
      clock.advanceTo(CantonTimestamp.assertFromInstant(timestamp))
  }

  @Help.Description("Advance time by given time-period")
  def advance(duration: JDuration): Unit = TraceContext.withNewTraceContext {
    implicit traceContext =>
      clock.advance(duration)
  }

  @Help.Summary("Reset simulation clock")
  def reset(): Unit = clock.reset()

}
