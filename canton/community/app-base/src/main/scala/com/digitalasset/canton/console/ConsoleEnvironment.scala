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
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeCommon
import com.digitalasset.canton.protocol.SerializableContract
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnection,
  SequencerConnections,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{Identifier, ParticipantId, PartyId}
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

final case class NodeReferences[A, R <: A, L <: A](local: Seq[L], remote: Seq[R]) {
  val all: Seq[A] = local ++ remote
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
  protected def startupOrderPrecedence(instance: LocalInstanceReferenceCommon): Int

  /** The order that local nodes would ideally be started in. */
  final val startupOrdering: Ordering[LocalInstanceReferenceCommon] =
    (x: LocalInstanceReferenceCommon, y: LocalInstanceReferenceCommon) =>
      startupOrderPrecedence(x) compare startupOrderPrecedence(y)

  /** allows for injecting a custom admin command runner during tests */
  protected def createAdminCommandRunner: ConsoleEnvironment => ConsoleGrpcAdminCommandRunner

  protected override val loggerFactory: NamedLoggerFactory = environment.loggerFactory

  private val commandTimeoutReference: AtomicReference[ConsoleCommandTimeout] =
    new AtomicReference[ConsoleCommandTimeout](environment.config.parameters.timeouts.console)

  private val featureSetReference: AtomicReference[HelperItems] =
    new AtomicReference[HelperItems](HelperItems(environment.config.features.featureFlags))

  private case class HelperItems(scope: Set[FeatureFlag]) {
    lazy val participantHelperItems = {
      // due to the use of reflection to grab the help-items, i need to write the following, repetitive stuff explicitly
      val subItems =
        if (participantsX.local.nonEmpty)
          participantsX.local.headOption.toList.flatMap(p =>
            Help.getItems(p, baseTopic = Seq("$participant"), scope = scope)
          )
        else if (participantsX.remote.nonEmpty)
          participantsX.remote.headOption.toList.flatMap(p =>
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
  lazy val grpcAdminCommandRunner: ConsoleGrpcAdminCommandRunner = createAdminCommandRunner(this)

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
    consoleOutput.info(Help.format(featureSetReference.get().filteredHelpItems: _*))
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

  lazy val participantsX: NodeReferences[
    ParticipantReferenceX,
    RemoteParticipantReferenceX,
    LocalParticipantReferenceX,
  ] =
    NodeReferences(
      environment.config.participantsByString.keys.map(createParticipantReferenceX).toSeq,
      environment.config.remoteParticipantsByString.keys
        .map(createRemoteParticipantReferenceX)
        .toSeq,
    )

  lazy val sequencersX: NodeReferences[
    SequencerNodeReferenceX,
    RemoteSequencerNodeReferenceX,
    LocalSequencerNodeReferenceX,
  ] =
    NodeReferences(
      environment.config.sequencersByString.keys.map(createSequencerReferenceX).toSeq,
      environment.config.remoteSequencersByString.keys.map(createRemoteSequencerReferenceX).toSeq,
    )

  lazy val mediatorsX
      : NodeReferences[MediatorReferenceX, RemoteMediatorReferenceX, LocalMediatorReferenceX] =
    NodeReferences(
      environment.config.mediatorsByString.keys.map(createMediatorReferenceX).toSeq,
      environment.config.remoteMediatorsByString.keys.map(createRemoteMediatorReferenceX).toSeq,
    )

  // the scala compiler / wartremover gets confused here if I use ++ directly
  def mergeLocalInstances(
      locals: Seq[LocalInstanceReferenceCommon]*
  ): Seq[LocalInstanceReferenceCommon] =
    locals.flatten
  def mergeLocalInstancesX(
      locals: Seq[LocalInstanceReferenceX]*
  ): Seq[LocalInstanceReferenceX] =
    locals.flatten
  def mergeRemoteInstances(remotes: Seq[InstanceReferenceCommon]*): Seq[InstanceReferenceCommon] =
    remotes.flatten

  lazy val nodes: NodeReferences[
    InstanceReferenceCommon,
    InstanceReferenceCommon,
    LocalInstanceReferenceCommon,
  ] = {
    NodeReferences(
      mergeLocalInstances(
        participantsX.local,
        sequencersX.local,
        mediatorsX.local,
      ),
      mergeRemoteInstances(
        participantsX.remote,
        sequencersX.remote,
        mediatorsX.remote,
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
    val localParticipantXBinds: Seq[TopLevelValue[_]] =
      participantsX.local.map(p =>
        TopLevelValue(p.name, helpText("participant x", p.name), p, nodeTopic)
      )
    val remoteParticipantXBinds: Seq[TopLevelValue[_]] =
      participantsX.remote.map(p =>
        TopLevelValue(p.name, helpText("remote participant x", p.name), p, nodeTopic)
      )
    val localMediatorXBinds: Seq[TopLevelValue[_]] =
      mediatorsX.local.map(d =>
        TopLevelValue(d.name, helpText("local mediator-x", d.name), d, nodeTopic)
      )
    val remoteMediatorXBinds: Seq[TopLevelValue[_]] =
      mediatorsX.remote.map(d =>
        TopLevelValue(d.name, helpText("remote mediator-x", d.name), d, nodeTopic)
      )
    val localSequencerXBinds: Seq[TopLevelValue[_]] =
      sequencersX.local.map(d =>
        TopLevelValue(d.name, helpText("local sequencer-x", d.name), d, nodeTopic)
      )
    val remoteSequencerXBinds: Seq[TopLevelValue[_]] =
      sequencersX.remote.map(d =>
        TopLevelValue(d.name, helpText("remote sequencer-x", d.name), d, nodeTopic)
      )
    val clockBinds: Option[TopLevelValue[_]] =
      environment.simClock.map(cl =>
        TopLevelValue("clock", "Simulated time", new SimClockCommand(cl))
      )
    val referencesTopic = Seq(topicGenericNodeReferences)
    localParticipantXBinds ++ remoteParticipantXBinds ++
      localSequencerXBinds ++ remoteSequencerXBinds ++ localMediatorXBinds ++ remoteMediatorXBinds ++ clockBinds.toList :+
      TopLevelValue(
        "participantsX",
        "All participant x nodes" + genericNodeReferencesDoc,
        participantsX,
        referencesTopic,
      ) :+
      TopLevelValue(
        "mediatorsX",
        "All mediator-x nodes" + genericNodeReferencesDoc,
        mediatorsX,
        referencesTopic,
      ) :+
      TopLevelValue(
        "sequencersX",
        "All sequencer-x nodes" + genericNodeReferencesDoc,
        sequencersX,
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

  private def createParticipantReferenceX(name: String): LocalParticipantReferenceX =
    new LocalParticipantReferenceX(this, name)
  private def createRemoteParticipantReferenceX(name: String): RemoteParticipantReferenceX =
    new RemoteParticipantReferenceX(this, name)

  private def createSequencerReferenceX(name: String): LocalSequencerNodeReferenceX =
    new LocalSequencerNodeReferenceX(this, name)

  private def createRemoteSequencerReferenceX(name: String): RemoteSequencerNodeReferenceX =
    new RemoteSequencerNodeReferenceX(this, name)

  private def createMediatorReferenceX(name: String): LocalMediatorReferenceX =
    new LocalMediatorReferenceX(this, name)

  private def createRemoteMediatorReferenceX(name: String): RemoteMediatorReferenceX =
    new RemoteMediatorReferenceX(this, name)

  /** So we can we make this available
    */
  protected def selfAlias(): Bind[_] = Bind(ConsoleEnvironmentBinding.BindingName, this)

  override def onClosed(): Unit = {
    Lifecycle.close(grpcAdminCommandRunner, environment)(logger)
  }

  def closeChannels(): Unit = {
    grpcAdminCommandRunner.closeChannels()
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
        instances: Seq[LocalInstanceReferenceCommon]
    ): LocalInstancesExtensions[LocalInstanceReferenceCommon] =
      new LocalInstancesExtensions.Impl(instances)

    /** Implicit maps an LfPartyId to a PartyId */
    implicit def toPartId(lfPartyId: LfPartyId): PartyId = PartyId.tryFromLfParty(lfPartyId)

    /** Extensions for many participant references
      */
    implicit def toParticipantReferencesExtensions(participants: Seq[ParticipantReferenceCommon])(
        implicit consoleEnvironment: ConsoleEnvironment
    ): ParticipantReferencesExtensions =
      new ParticipantReferencesExtensions(participants)

    implicit def toLocalParticipantReferencesExtensions[ParticipantNodeT <: ParticipantNodeCommon](
        participants: Seq[LocalParticipantReferenceCommon[ParticipantNodeT]]
    )(implicit
        consoleEnvironment: ConsoleEnvironment
    ): LocalParticipantReferencesExtensions[ParticipantNodeT, LocalParticipantReferenceCommon[
      ParticipantNodeT
    ]] =
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
        ref: InstanceReferenceWithSequencerConnection
    ): SequencerConnection =
      ref.sequencerConnection

    implicit def toGSequencerConnections(
        ref: InstanceReferenceWithSequencerConnection
    ): SequencerConnections =
      SequencerConnections.single(ref.sequencerConnection)

    implicit def toIdentifier(id: String): Identifier = Identifier.tryCreate(id)
    implicit def toFingerprint(fp: String): Fingerprint = Fingerprint.tryCreate(fp)

    /** Implicitly map ParticipantReferences to the ParticipantId
      */
    implicit def toParticipantIdX(reference: ParticipantReferenceX): ParticipantId = reference.id

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
