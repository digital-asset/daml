// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import better.files.File
import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import ch.qos.logback.classic.Level
import com.daml.ledger.api.v2.commands.{Command, CreateCommand, ExerciseCommand}
import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.value.Value.Sum
import com.daml.ledger.api.v2.value.{
  Identifier as IdentifierV1,
  List as ListV1,
  Optional,
  Record,
  RecordField,
  Value,
}
import com.daml.ledger.javaapi.data.{DisclosedContract, Identifier}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers.WrappedCreatedEvent
import com.digitalasset.canton.admin.api.client.data
import com.digitalasset.canton.admin.api.client.data.{
  ListPartiesResult,
  MediatorStatus,
  NodeStatus,
  SequencerStatus,
  TemplateId,
}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.ConsoleEnvironment.Implicits.*
import com.digitalasset.canton.console.commands.PruningSchedulerAdministration
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{
  NamedLoggerFactory,
  NamedLogging,
  NodeLoggingUtil,
  TracedLogger,
}
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection
import com.digitalasset.canton.participant.config.BaseParticipantConfig
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.{
  SequencerConnectionPoolDelays,
  SequencerConnectionValidation,
  SequencerConnections,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{SequencerAlias, SynchronizerAlias, config}
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax.*

import java.io.File as JFile
import java.time.Instant
import scala.annotation.unused
import scala.concurrent.duration.*
import scala.sys.process.ProcessLogger

trait ConsoleMacros extends NamedLogging with NoTracing {

  import scala.reflect.runtime.universe.*

  @Help.Summary("Console utilities")
  @Help.Group("Utilities")
  object utils extends Helpful {

    @Help.Summary("A process logger that forwards process logs to the canton logs")
    def cantonProcessLogger(tracedLogger: TracedLogger = logger): ProcessLogger =
      new ProcessLogger {
        override def out(s: => String): Unit = tracedLogger.debug(s)
        override def err(s: => String): Unit = tracedLogger.error(s)
        override def buffer[T](f: => T): T = f
      }

    @Help.Summary("Reflective inspection of object arguments, handy to inspect case class objects")
    @Help.Description(
      "Return the list field names of the given object. Helpful function when inspecting the return result."
    )
    def object_args[T: TypeTag](@unused obj: T): List[String] = type_args[T]

    @Help.Summary("Reflective inspection of type arguments, handy to inspect case class types")
    @Help.Description(
      "Return the list of field names of the given type. Helpful function when creating new objects for requests."
    )
    def type_args[T: TypeTag]: List[String] =
      typeOf[T].members.collect {
        case m: MethodSymbol if m.isCaseAccessor => s"${m.name}:${m.returnType}"
      }.toList

    @Help.Summary("Wait for a condition to become true, using default timeouts")
    @Help.Description("""
        |Wait until condition becomes true, with a timeout taken from the parameters.timeouts.console.bounded
        |configuration parameter.""")
    final def retry_until_true(
        condition: => Boolean
    )(implicit
        env: ConsoleEnvironment
    ): Unit = retry_until_true(env.commandTimeouts.bounded)(
      condition,
      s"Condition never became true within ${env.commandTimeouts.bounded.unwrap}",
    )

    @Help.Summary("Wait for a condition to become true")
    @Help.Description("""Wait `timeout` duration until `condition` becomes true.
        | Retry evaluating `condition` with an exponentially increasing back-off up to `maxWaitPeriod` duration between retries.
        |""")
    @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.While"))
    final def retry_until_true(
        timeout: NonNegativeDuration,
        maxWaitPeriod: NonNegativeDuration = 10.seconds,
    )(
        condition: => Boolean,
        failure: => String = s"Condition never became true within $timeout",
    ): Unit = {
      val deadline = timeout.asFiniteApproximation.fromNow
      var isCompleted = condition
      var waitMillis = 1L
      while (!isCompleted) {
        val timeLeft = deadline.timeLeft
        if (timeLeft > Duration.Zero) {
          val remaining = (timeLeft min (waitMillis.millis)) max 1.millis
          Threading.sleep(remaining.toMillis)
          // capped exponentially back off
          waitMillis = (waitMillis * 2) min maxWaitPeriod.duration.toMillis
          isCompleted = condition
        } else {
          throw new IllegalStateException(failure)
        }
      }
    }

    @Help.Summary("Wait until all topology changes have been effected on all accessible nodes")
    def synchronize_topology(
        timeoutO: Option[NonNegativeDuration] = None
    )(implicit env: ConsoleEnvironment): Unit =
      ConsoleMacros.utils.retry_until_true(timeoutO.getOrElse(env.commandTimeouts.bounded)) {
        env.nodes.all.forall(_.topology.synchronisation.is_idle())
      }

    private object GenerateDamlScriptParticipantsConf {

      private val filename = "participant-config.json"

      case class LedgerApi(host: String, port: Int)

      // Keys in the exported JSON should have snake_case
      case class Participants(
          default_participant: Option[LedgerApi],
          participants: Map[String, LedgerApi],
          party_participants: Map[String, String],
      )

      implicit val ledgerApiEncoder: Encoder[LedgerApi] = deriveEncoder[LedgerApi]
      implicit val participantsEncoder: Encoder[Participants] = deriveEncoder[Participants]

      private def partyIdToParticipants(
          useParticipantAlias: Boolean,
          uidToAlias: Map[ParticipantId, String],
      )(implicit env: ConsoleEnvironment): Map[String, String] = {
        def participantReference(p: ParticipantId) = if (useParticipantAlias)
          uidToAlias.getOrElse(p, p.uid.toProtoPrimitive)
        else p.uid.toProtoPrimitive

        def partyIdToParticipant(p: ListPartiesResult) = p.participants.headOption.map {
          participantSynchronizers =>
            (p.party.filterString, participantReference(participantSynchronizers.participant))
        }

        val partyAndParticipants =
          env.participants.all.flatMap(_.parties.list().flatMap(partyIdToParticipant(_).toList))
        val allPartiesSingleParticipant =
          partyAndParticipants.groupBy { case (partyId, _) => partyId }.forall {
            case (_, participants) => participants.sizeCompare(1) <= 0
          }

        if (!allPartiesSingleParticipant)
          logger.info(
            "Some parties are hosted on more than one participant. " +
              "For such parties, only one participant will be exported to the generated config file."
          )

        partyAndParticipants.toMap
      }

      def apply(
          file: Option[String] = None,
          useParticipantAlias: Boolean = true,
          defaultParticipant: Option[ParticipantReference] = None,
      )(implicit env: ConsoleEnvironment): JFile = {

        def toLedgerApi(participantConfig: BaseParticipantConfig) =
          LedgerApi(
            participantConfig.clientLedgerApi.address,
            participantConfig.clientLedgerApi.port.unwrap,
          )

        def participantValue(p: ParticipantReference): String =
          if (useParticipantAlias) p.name else p.uid.toProtoPrimitive

        val allParticipants = env.participants.all
        val participantsData =
          allParticipants.map(p => (participantValue(p), toLedgerApi(p.config))).toMap
        val uidToAlias = allParticipants.map(p => (p.id, p.name)).toMap

        val default_participant =
          defaultParticipant.map(participantReference => toLedgerApi(participantReference.config))

        val participantJson = Participants(
          default_participant,
          participantsData,
          partyIdToParticipants(useParticipantAlias, uidToAlias),
        ).asJson.spaces2

        val targetFile = file.map(File(_)).getOrElse(File(filename))
        targetFile.overwrite(participantJson).appendLine()

        targetFile.toJava
      }
    }

    @Help.Summary("Create a participants config for Daml script")
    @Help.Description(
      """The generated config can be passed to `daml script` via the `participant-config` parameter.
        |More information about the file format can be found in the `documentation <https://docs.daml.com/daml-script/index.html#using-daml-script-in-distributed-topologies>`_:
        |It takes three arguments:
        |- file (default to "participant-config.json")
        |- useParticipantAlias (default to true): participant aliases are used instead of UIDs
        |- defaultParticipant (default to None): adds a default participant if provided
        |"""
    )
    def generate_daml_script_participants_conf(
        file: Option[String] = None,
        useParticipantAlias: Boolean = true,
        defaultParticipant: Option[ParticipantReference] = None,
    )(implicit env: ConsoleEnvironment): JFile =
      GenerateDamlScriptParticipantsConf(
        file,
        useParticipantAlias,
        defaultParticipant,
      )

    @Help.Summary(
      "Register `AutoCloseable` object to be shutdown if Canton is shut down",
      FeatureFlag.Testing,
    )
    def auto_close(closeable: AutoCloseable)(implicit environment: ConsoleEnvironment): Unit =
      FeatureFlagFilter.check(noTracingLogger, environment)(FeatureFlag.Testing)(
        environment.environment.addUserCloseable(closeable)
      )

    @Help.Summary("Writes several Protobuf messages to a file.")
    def write_to_file(data: Seq[scalapb.GeneratedMessage], fileName: String): Unit =
      File(fileName).outputStream.foreach { os =>
        data.foreach(_.writeDelimitedTo(os))
      }

    @Help.Summary("Reads several Protobuf messages from a file.")
    @Help.Description("Fails with an exception, if the file can't be read or parsed.")
    def read_all_messages_from_file[A <: scalapb.GeneratedMessage](
        fileName: String
    )(implicit companion: scalapb.GeneratedMessageCompanion[A]): Seq[A] =
      File(fileName).inputStream
        .apply { is =>
          Seq.unfold(()) { _ =>
            companion.parseDelimitedFrom(is).map(_ -> ())
          }
        }

    @Help.Summary("Writes a Protobuf message to a file.")
    def write_to_file(data: scalapb.GeneratedMessage, fileName: String): Unit =
      write_to_file(Seq(data), fileName)

    @Help.Summary("Reads a single Protobuf message from a file.")
    @Help.Description("Fails with an exception, if the file can't be read or parsed.")
    def read_first_message_from_file[A <: scalapb.GeneratedMessage](
        fileName: String
    )(implicit companion: scalapb.GeneratedMessageCompanion[A]): A =
      File(fileName).inputStream
        .apply(companion.parseDelimitedFrom)
        .getOrElse(
          throw new IllegalArgumentException(
            s"Unable to read ${companion.getClass.getSimpleName} from $fileName."
          )
        )

    @Help.Summary("Writes a ByteString to a file.")
    def write_to_file(data: ByteString, fileName: String): Unit =
      BinaryFileUtil.writeByteStringToFile(fileName, data)

    @Help.Summary("Reads a ByteString from a file.")
    @Help.Description("Fails with an exception, if the file can't be read.")
    def read_byte_string_from_file(fileName: String)(implicit env: ConsoleEnvironment): ByteString =
      env.runE(BinaryFileUtil.readByteStringFromFile(fileName))

  }

  @Help.Summary("Canton development and testing utilities")
  @Help.Group("Ledger Api Utils")
  object ledger_api_utils extends Helpful {

    private def buildIdentifier(packageId: String, module: String, template: String): IdentifierV1 =
      IdentifierV1(
        packageId = packageId,
        moduleName = module,
        entityName = template,
      )

    private def productToLedgerApiRecord(product: Product): Sum.Record = Value.Sum.Record(
      Record(fields =
        product.productIterator
          .map(mapToLedgerApiValue)
          .map(v => RecordField(value = Some(v)))
          .toSeq
      )
    )

    private def mapToLedgerApiValue(value: Any): Value = {

      // assuming that String.toString = id, we'll just map any Map to a string map without casting
      def safeMapCast(map: Map[?, ?]): Map[String, Any] = map.map { case (key, value) =>
        (key.toString, value)
      }

      val x: Value.Sum = value match {
        case x: Int => Value.Sum.Int64(x.toLong)
        case x: Long => Value.Sum.Int64(x)
        case x: PartyId => Value.Sum.Party(x.toLf)
        case x: Float => Value.Sum.Numeric(s"$x")
        case x: Double => Value.Sum.Numeric(s"$x")
        case x: String => Value.Sum.Text(x)
        case x: Boolean => Value.Sum.Bool(x)
        case x: Seq[Any] => Value.Sum.List(value = ListV1(x.map(mapToLedgerApiValue)))
        case x: LfContractId => Value.Sum.ContractId(x.coid)
        case x: Instant => Value.Sum.Timestamp(x.toEpochMilli * 1000L)
        case x: Option[Any] => Value.Sum.Optional(Optional(value = x.map(mapToLedgerApiValue)))
        case x: Value.Sum => x
        case x: Map[?, ?] => Value.Sum.Record(buildArguments(safeMapCast(x)))
        case x: (Any, Any) => productToLedgerApiRecord(x)
        case x: (Any, Any, Any) => productToLedgerApiRecord(x)
        case _ =>
          throw new UnsupportedOperationException(
            s"value type not yet implemented: ${value.getClass}"
          )
      }
      Value(x)
    }

    private def mapToRecordField(item: (String, Any)): RecordField =
      RecordField(
        label = item._1,
        value = Some(mapToLedgerApiValue(item._2)),
      )

    private def buildArguments(map: Map[String, Any]): Record =
      Record(
        fields = map.map(mapToRecordField).toSeq
      )

    @Help.Summary("Build create command")
    def create(
        packageId: String,
        module: String,
        template: String,
        arguments: Map[String, Any],
    ): Command =
      Command.defaultInstance.withCreate(
        CreateCommand(
          templateId = Some(buildIdentifier(packageId, module, template)),
          createArguments = Some(buildArguments(arguments)),
        )
      )

    @Help.Summary("Build exercise command")
    def exercise(
        packageId: String,
        module: String,
        template: String,
        choice: String,
        arguments: Map[String, Any],
        contractId: String,
    ): Command =
      Command.defaultInstance.withExercise(
        ExerciseCommand(
          templateId = Some(buildIdentifier(packageId, module, template)),
          choice = choice,
          choiceArgument = Some(Value(Value.Sum.Record(buildArguments(arguments)))),
          contractId = contractId,
        )
      )

    @Help.Summary("Build exercise command from CreatedEvent")
    def exercise(choice: String, arguments: Map[String, Any], event: CreatedEvent): Command = {
      def getOrThrow(desc: String, opt: Option[String]): String =
        opt.getOrElse(
          throw new IllegalArgumentException(s"Corrupt created event $event without $desc")
        )

      exercise(
        getOrThrow(
          "packageId",
          event.templateId
            .map(_.packageId),
        ),
        getOrThrow("moduleName", event.templateId.map(_.moduleName)),
        getOrThrow("template", event.templateId.map(_.entityName)),
        choice,
        arguments,
        event.contractId,
      )
    }

    def fetchContractsAsDisclosed(
        participant: LocalParticipantReference,
        readerParty: PartyId,
        templateId: Identifier,
    ): Map[String /* ContractId */, DisclosedContract] =
      participant.ledger_api.state.acs
        .active_contracts_of_party(
          party = readerParty,
          filterTemplates = Seq(TemplateId.fromJavaIdentifier(templateId)),
          includeCreatedEventBlob = true,
        )
        .map { activeContract =>
          val createdEvent = activeContract.getCreatedEvent
          createdEvent.contractId -> JavaDecodeUtil
            .toDisclosedContract(
              activeContract.synchronizerId,
              WrappedCreatedEvent(createdEvent).toJava,
            )
        }
        .toMap
  }

  @Help.Summary("Logging related commands")
  @Help.Group("Logging")
  object logging extends Helpful {

    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    @Help.Summary("Dynamically change log level (TRACE, DEBUG, INFO, WARN, ERROR, OFF, null)")
    def set_level(loggerName: String = "com.digitalasset.canton", level: String): Unit =
      NodeLoggingUtil.setLevel(loggerName, level)

    @Help.Summary("Determine current logging level")
    def get_level(loggerName: String = "com.digitalasset.canton"): Option[Level] =
      Option(NodeLoggingUtil.getLogger(loggerName).getLevel)

    @Help.Summary("Returns the last errors (trace-id -> error event) that have been logged locally")
    def last_errors(): Map[String, String] =
      NodeLoggingUtil.lastErrors().getOrElse {
        logger.error(s"Log appender for last errors not found/configured")
        throw new CommandFailure()
      }

    @Help.Summary("Returns log events for an error with the same trace-id")
    def last_error_trace(traceId: String): Seq[String] =
      NodeLoggingUtil.lastErrorTrace(traceId).getOrElse {
        logger.error(s"No events found for last error trace-id $traceId")
        throw new CommandFailure()
      }
  }

  @Help.Summary("Configure behaviour of console")
  @Help.Group("Console")
  object console extends Helpful {

    @Help.Summary("Yields the timeout for running console commands")
    @Help.Description(
      "Yields the timeout for running console commands. " +
        "When the timeout has elapsed, the console stops waiting for the command result. " +
        "The command will continue running in the background."
    )
    def command_timeout(implicit env: ConsoleEnvironment): NonNegativeDuration =
      env.commandTimeouts.bounded

    @Help.Summary("Sets the timeout for running console commands.")
    @Help.Description(
      "Sets the timeout for running console commands. " +
        "When the timeout has elapsed, the console stops waiting for the command result. " +
        "The command will continue running in the background. " +
        "The new timeout must be positive."
    )
    def set_command_timeout(newTimeout: NonNegativeDuration)(implicit
        env: ConsoleEnvironment
    ): Unit =
      env.setCommandTimeout(newTimeout)

    // this command is intentionally not documented as part of the help system
    def disable_features(flag: FeatureFlag)(implicit env: ConsoleEnvironment): Unit =
      env.updateFeatureSet(flag, include = false)

    // this command is intentionally not documented as part of the help system
    def enable_features(flag: FeatureFlag)(implicit env: ConsoleEnvironment): Unit =
      env.updateFeatureSet(flag, include = true)
  }

  @Help.Summary("Functions to bootstrap/setup decentralized namespaces or full synchronizers")
  @Help.Group("Bootstrap")
  object bootstrap extends Helpful {

    @Help.Summary("Bootstraps a decentralized namespace for the provided owners")
    @Help.Description(
      """Returns the decentralized namespace, the fully authorized transaction of its definition, as well
        |as all root certificates of the owners. This allows other nodes to import and
        |fully validate the decentralized namespace definition.
        |After this call has finished successfully, all of the owners have stored the co-owners' identity topology
        |transactions as well as the fully authorized decentralized namespace definition in the specified topology store."""
    )
    def decentralized_namespace(
        owners: Seq[InstanceReference],
        threshold: PositiveInt,
        store: TopologyStoreId = TopologyStoreId.Authorized,
    ): (Namespace, Seq[GenericSignedTopologyTransaction]) = {
      val ownersNE = NonEmpty
        .from(owners)
        .getOrElse(
          throw new IllegalArgumentException(
            "There must be at least 1 owner for a decentralizedNamespace."
          )
        )
      val expectedDNS = DecentralizedNamespaceDefinition.computeNamespace(
        owners.map(_.namespace).toSet
      )
      val proposedOrExisting = ownersNE
        .map { owner =>
          val existingDnsO =
            owner.topology.transactions
              .find_latest_by_mapping_hash[DecentralizedNamespaceDefinition](
                DecentralizedNamespaceDefinition.uniqueKey(expectedDNS),
                store = store,
                includeProposals = true,
              )
              .map(_.transaction)

          existingDnsO.getOrElse(
            owner.topology.decentralized_namespaces.propose_new(
              owners.map(_.namespace).toSet,
              threshold,
              store = store,
            )
          )
        }

      // require that all proposed or previously existing transactions have the same hash,
      // otherwise there is no chance for success
      if (proposedOrExisting.distinctBy(_.hash).sizeIs != 1) {
        throw new IllegalStateException(
          s"Proposed or previously existing transactions disagree on the founding of the synchronizer's decentralized namespace:\n$proposedOrExisting"
        )
      }

      // merging the signatures here is an "optimization" so that later we only upload a single
      // decentralizedNamespace transaction, instead of a transaction per owner.
      val decentralizedNamespaceDefinition =
        proposedOrExisting.reduceLeft[SignedTopologyTransaction[
          TopologyChangeOp,
          DecentralizedNamespaceDefinition,
        ]]((txA, txB) => txA.addSignatures(txB.signatures))

      val ownerNSDs = owners.flatMap(_.topology.transactions.identity_transactions())
      val foundingTransactions = ownerNSDs :+ decentralizedNamespaceDefinition

      owners.foreach(
        _.topology.transactions
          .load(foundingTransactions, store = store, ForceFlag.AlienMember)
      )

      (decentralizedNamespaceDefinition.mapping.namespace, foundingTransactions)
    }

    /** Checks that either all given sequencers and mediators already have been initialized for the
      * expected synchronizer (returns Right(Some(_)), or none have been initialized at all (returns
      * Right(None)). In any other case (nodes have already been initialized for another
      * synchronizer or some nodes have been initialized but others not), this method returns a
      * Left(error).
      */
    private def in_synchronizer(
        sequencers: NonEmpty[Seq[SequencerReference]],
        mediators: NonEmpty[Seq[MediatorReference]],
    )(synchronizerId: PhysicalSynchronizerId): Either[String, Option[PhysicalSynchronizerId]] = {
      def isNotInitializedOrSuccessWithSynchronizer(
          instance: InstanceReference
      ): Either[String, Boolean /* isInitializedWithSynchronizer */ ] =
        instance.health.status match {
          case nonFailure if nonFailure.isActive.contains(false) =>
            Left(s"${instance.id.member} is currently not active")
          case NodeStatus.Success(status: SequencerStatus) =>
            // sequencer is already fully initialized for this synchronizer
            Either.cond(
              status.synchronizerId == synchronizerId,
              true,
              s"${instance.id.member} has already been initialized for synchronizer ${status.synchronizerId} instead of $synchronizerId.",
            )
          case NodeStatus.Success(status: MediatorStatus) =>
            // mediator is already fully initialized for this synchronizer
            Either.cond(
              status.synchronizerId == synchronizerId,
              true,
              s"${instance.id.member} has already been initialized for synchronizer ${status.synchronizerId} instead of $synchronizerId",
            )
          case NodeStatus.NotInitialized(true, _) =>
            // the node is not yet initialized for this synchronizer
            Right(false)
          case NodeStatus.Failure(msg) =>
            Left(s"Error obtaining status for ${instance.id.member}: $msg")
          case otherwise =>
            // Unexpected status response. All cases should be covered by the patterns above
            Left(s"Unexpected status: $otherwise")
        }

      val alreadyFullyInitialized =
        (sequencers ++ mediators).forgetNE.toSeq
          .traverse(isNotInitializedOrSuccessWithSynchronizer(_))
      alreadyFullyInitialized
        .map(nodesOnSynchronizerOrNotInitialized =>
          // any false in the result means, that some nodes haven't been initialized yet
          if (nodesOnSynchronizerOrNotInitialized.forall(identity)) Some(synchronizerId)
          else None
        )
    }

    private def check_synchronizer_bootstrap_status(
        name: String,
        owners: Seq[InstanceReference],
        sequencers: Seq[SequencerReference],
        mediators: Seq[MediatorReference],
        staticSynchronizerParameters: data.StaticSynchronizerParameters,
    ): Either[String, Option[PhysicalSynchronizerId]] =
      for {
        neOwners <- NonEmpty
          .from(owners.distinct)
          .toRight("you need at least one synchronizer owner")
        neSequencers <- NonEmpty
          .from(sequencers.distinct)
          .toRight("you need at least one sequencer")
        neMediators <- NonEmpty.from(mediators.distinct).toRight("you need at least one mediator")
        nodes = neOwners ++ neSequencers ++ neMediators
        _ = Either.cond(nodes.forall(_.health.is_running()), (), "all nodes must be running")
        ns =
          DecentralizedNamespaceDefinition.computeNamespace(
            owners.map(_.namespace).toSet
          )
        expectedId = PhysicalSynchronizerId(
          SynchronizerId(UniqueIdentifier.tryCreate(name, ns.toProtoPrimitive)),
          staticSynchronizerParameters.toInternal,
        )
        actualIdIfAllNodesAreInitialized <- in_synchronizer(neSequencers, neMediators)(expectedId)
      } yield actualIdIfAllNodesAreInitialized

    private def run_bootstrap(
        synchronizerName: String,
        staticSynchronizerParameters: data.StaticSynchronizerParameters,
        synchronizerOwners: Seq[InstanceReference],
        synchronizerThreshold: PositiveInt,
        sequencers: Seq[SequencerReference],
        mediatorsToSequencers: Map[
          MediatorReference,
          (Seq[SequencerReference], PositiveInt, NonNegativeInt),
        ],
        mediatorRequestAmplification: SubmissionRequestAmplification,
        mediatorThreshold: PositiveInt,
    )(implicit consoleEnvironment: ConsoleEnvironment): PhysicalSynchronizerId = {
      val synchronizerNamespace =
        DecentralizedNamespaceDefinition.computeNamespace(synchronizerOwners.map(_.namespace).toSet)
      val synchronizerId = PhysicalSynchronizerId(
        SynchronizerId(
          UniqueIdentifier.tryCreate(synchronizerName, synchronizerNamespace)
        ),
        staticSynchronizerParameters.toInternal,
      )

      val tempStoreForBootstrap = synchronizerOwners
        .map(
          _.topology.stores.create_temporary_topology_store(
            s"$synchronizerName-setup",
            staticSynchronizerParameters.protocolVersion,
          )
        )
        .headOption
        .getOrElse(consoleEnvironment.raiseError("No synchronizer owners specified."))

      val mediators = mediatorsToSequencers.keys.toSeq

      val identityTransactions =
        (sequencers ++ mediators ++ synchronizerOwners).flatMap(
          _.topology.transactions.identity_transactions()
        )

      synchronizerOwners.foreach(
        _.topology.transactions.load(
          identityTransactions,
          store = tempStoreForBootstrap,
          ForceFlag.AlienMember,
        )
      )

      val (_, foundingTxs) =
        bootstrap.decentralized_namespace(
          synchronizerOwners,
          synchronizerThreshold,
          store = tempStoreForBootstrap,
        )

      val synchronizerGenesisTxs = synchronizerOwners.flatMap(
        _.topology.synchronizer_bootstrap.generate_genesis_topology(
          synchronizerId,
          synchronizerOwners.map(_.id.member),
          sequencers.map(_.id),
          mediators.map(_.id),
          store = tempStoreForBootstrap,
          mediatorThreshold,
        )
      )

      val initialTopologyState = (identityTransactions ++ foundingTxs ++ synchronizerGenesisTxs)
        .mapFilter(_.selectOp[TopologyChangeOp.Replace])
        .distinct

      val merged =
        SignedTopologyTransactions.compact(initialTopologyState).map(_.updateIsProposal(false))

      val out = ByteString.newOutput()
      merged.foreach { stored =>
        val tx = StoredTopologyTransaction(
          sequenced = SequencedTime(SignedTopologyTransaction.InitialTopologySequencingTime),
          validFrom = EffectiveTime(SignedTopologyTransaction.InitialTopologySequencingTime),
          validUntil = None,
          transaction = stored,
          rejectionReason = None,
        )
        tx.writeDelimitedTo(staticSynchronizerParameters.protocolVersion, out)
          .left
          .foreach(error =>
            consoleEnvironment.raiseError(s"The synchronizer cannot be bootstrapped: $error")
          )
      }

      val storedTopologySnapshot = out.toByteString

      sequencers
        .filterNot(_.health.initialized())
        .foreach(x =>
          x.setup
            .assign_from_genesis_stateV2(storedTopologySnapshot, staticSynchronizerParameters)
            .discard
        )

      mediatorsToSequencers
        .filter(!_._1.health.initialized())
        .foreach {
          case (mediator, (mediatorSequencers, sequencerTrustThreshold, sequencerLivenessMargin)) =>
            mediator.setup.assign(
              synchronizerId,
              SequencerConnections.tryMany(
                mediatorSequencers
                  .map(s => s.sequencerConnection.withAlias(SequencerAlias.tryCreate(s.name))),
                sequencerTrustThreshold,
                sequencerLivenessMargin,
                mediatorRequestAmplification,
                SequencerConnectionPoolDelays.default,
              ),
              // if we run bootstrap ourselves, we should have been able to reach the nodes
              // so we don't want the bootstrapping to fail spuriously here in the middle of
              // the setup
              SequencerConnectionValidation.Disabled,
            )
        }

      synchronizerOwners.foreach(
        _.topology.stores.drop_temporary_topology_store(tempStoreForBootstrap)
      )

      synchronizerId
    }

    @Help.Summary("Bootstraps a local synchronizer using default arguments")
    @Help.Description(
      "This is a convenience method for bootstrapping a local synchronizer." +
        "The synchronizer will include all sequencers and mediators that are currently running." +
        "It will be owned by the sequencers, while the mediator threshold will be set to require" +
        "all mediators to confirm."
    )
    def synchronizer_local(
        synchronizerName: String = "local"
    )(implicit consoleEnvironment: ConsoleEnvironment): SynchronizerId = {
      def checkEnoughAndNotInitialized[T <: InstanceReference](
          name: String,
          items: Seq[T],
      ): Seq[T] = {
        val distinct = items
          .filter(x => x.health.is_running() && x.health.active && x.health.has_identity())
          .groupBy(_.id)
          .flatMap { case (_, v) =>
            v.headOption.toList
          }
          .toList
        if (distinct.isEmpty) {
          consoleEnvironment.raiseError(s"No ${name}s available to bootstrap a local synchronizer.")
        }
        distinct.find(_.health.initialized()).foreach { ref =>
          consoleEnvironment.raiseError(
            s"$name ${ref.id} has already been initialized"
          )
        }
        distinct
      }
      val distinctSequencers =
        checkEnoughAndNotInitialized("sequencer", consoleEnvironment.sequencers.all)
      val distinctMediators =
        checkEnoughAndNotInitialized("mediator", consoleEnvironment.mediators.all)
      synchronizer(
        synchronizerName,
        distinctSequencers,
        distinctMediators,
        synchronizerOwners = distinctSequencers,
        synchronizerThreshold = PositiveInt.tryCreate(distinctSequencers.length),
        staticSynchronizerParameters =
          data.StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer),
        mediatorThreshold = PositiveInt.tryCreate(distinctMediators.size),
      ).logical
    }

    @Help.Summary(
      "Bootstraps a new synchronizer."
    )
    @Help.Description(
      """Bootstraps a new synchronizer with the given static synchronizer parameters and members.
        |Any participants as synchronizer owners must still manually connect to the synchronizer afterwards.
        """
    )
    def synchronizer(
        synchronizerName: String,
        sequencers: Seq[SequencerReference],
        mediators: Seq[MediatorReference],
        synchronizerOwners: Seq[InstanceReference],
        synchronizerThreshold: PositiveInt,
        staticSynchronizerParameters: data.StaticSynchronizerParameters,
        mediatorRequestAmplification: SubmissionRequestAmplification =
          SubmissionRequestAmplification.NoAmplification,
        mediatorThreshold: PositiveInt = PositiveInt.one,
    )(implicit consoleEnvironment: ConsoleEnvironment): PhysicalSynchronizerId =
      synchronizer(
        synchronizerName,
        sequencers,
        mediators.map(_ -> (sequencers, PositiveInt.one, NonNegativeInt.zero)).toMap,
        synchronizerOwners,
        synchronizerThreshold,
        staticSynchronizerParameters,
        mediatorRequestAmplification,
        mediatorThreshold,
      )

    @Help.Summary(
      "Bootstraps a new synchronizer."
    )
    @Help.Description(
      """Bootstraps a new synchronizer with the given static synchronizer parameters and members.
        |Any participants as synchronizer owners must still manually connect to the synchronizer afterwards.
        |
        |Parameters:
        |  mediatorsToSequencers: map of mediator reference to a tuple of a sequence of sequencer references,
        |                         the sequencer trust threshold and the liveness margin for the given mediator.
        """
    )
    def synchronizer(
        synchronizerName: String,
        sequencers: Seq[SequencerReference],
        mediatorsToSequencers: Map[
          MediatorReference,
          (Seq[SequencerReference], PositiveInt, NonNegativeInt),
        ],
        synchronizerOwners: Seq[InstanceReference],
        synchronizerThreshold: PositiveInt,
        staticSynchronizerParameters: data.StaticSynchronizerParameters,
        mediatorRequestAmplification: SubmissionRequestAmplification,
        mediatorThreshold: PositiveInt,
    )(implicit consoleEnvironment: ConsoleEnvironment): PhysicalSynchronizerId = {
      // skip over HA sequencers
      val uniqueSequencers =
        sequencers.groupBy(_.id).flatMap(_._2.headOption.toList).toList
      val synchronizerOwnersOrDefault =
        if (synchronizerOwners.isEmpty) sequencers else synchronizerOwners
      val mediators = mediatorsToSequencers.keys.toSeq

      check_synchronizer_bootstrap_status(
        synchronizerName,
        synchronizerOwnersOrDefault,
        uniqueSequencers,
        mediators,
        staticSynchronizerParameters,
      ) match {
        case Right(Some(synchronizerId)) =>
          logger.info(
            s"Synchronizer $synchronizerName has already been bootstrapped with ID $synchronizerId"
          )
          synchronizerId
        case Right(None) =>
          run_bootstrap(
            synchronizerName,
            staticSynchronizerParameters,
            synchronizerOwnersOrDefault,
            synchronizerThreshold,
            uniqueSequencers,
            mediatorsToSequencers,
            mediatorRequestAmplification,
            mediatorThreshold,
          )
        case Left(error) =>
          consoleEnvironment.raiseError(s"The synchronizer cannot be bootstrapped: $error")
      }
    }

    @Help.Summary(
      "Onboards a new Sequencer node."
    )
    @Help.Description(
      "Onboards a new Sequencer node using an existing node from the network."
    )
    def onboard_new_sequencer(
        synchronizerId: SynchronizerId,
        newSequencer: SequencerReference,
        existingSequencer: SequencerReference,
        synchronizerOwners: Set[InstanceReference],
        customCommandTimeout: Option[config.NonNegativeDuration] = None,
        isBftSequencer: Boolean = false,
    )(implicit consoleEnvironment: ConsoleEnvironment): Unit = {
      import consoleEnvironment.*

      val commandTimeout = customCommandTimeout.getOrElse(commandTimeouts.bounded)

      def synchronizeTopologyAfterAddingSequencer(
          newSequencerId: SequencerId,
          existingSequencer: SequencerReference,
      ): Unit =
        ConsoleMacros.utils.retry_until_true(commandTimeout) {
          existingSequencer.topology.sequencers
            .list(existingSequencer.synchronizer_id)
            .headOption
            .map(_.item.allSequencers.forgetNE)
            .getOrElse(Seq.empty)
            .contains(newSequencerId)
        }

      def synchronizeTopologyAfterAddingBftSequencer(
          newSequencerId: SequencerId,
          existingSequencer: SequencerReference,
      ): Unit =
        ConsoleMacros.utils.retry_until_true(commandTimeout) {
          existingSequencer.bft.get_ordering_topology().sequencerIds.contains(newSequencerId)
        }

      val synchronizerOwnersNE = NonEmpty
        .from(synchronizerOwners)
        .getOrElse(raiseError("synchronizerOwners must not be empty"))

      // extract onboarding sequencer's identity transactions
      val onboardingSequencerIdentity =
        newSequencer.topology.transactions.identity_transactions()

      // upload onboarding sequencer's identity transactions
      existingSequencer.topology.transactions
        .load(onboardingSequencerIdentity, synchronizerId, ForceFlag.AlienMember)

      logger.info("Uploaded a new sequencer identity")

      // fetch the latest SequencerSynchronizerState mapping
      val seqState1 = existingSequencer.topology.sequencers
        .list(store = synchronizerId)
        .headOption
        .getOrElse(raiseError("No sequencer state found"))
        .item

      // propose the SequencerSynchronizerState that adds the new sequencer
      synchronizerOwnersNE
        .foreach(
          _.topology.sequencers
            .propose(
              synchronizerId,
              threshold = seqState1.threshold,
              active = seqState1.active :+ newSequencer.id,
            )
            .discard
        )

      logger.info("Proposed a sequencer synchronizer state with the new sequencer")

      // wait for SequencerSynchronizerState to be observed by the sequencer
      ConsoleMacros.utils.retry_until_true(commandTimeout) {
        val sequencerStates =
          existingSequencer.topology.sequencers.list(store = synchronizerId)

        val sequencerState =
          sequencerStates.headOption.getOrElse(
            raiseError("SequencerSynchronizerState should not empty")
          )
        sequencerState.item.active.contains(newSequencer.id)
      }
      logger.info("New sequencer synchronizer state has been observed")

      if (isBftSequencer) {
        synchronizeTopologyAfterAddingBftSequencer(newSequencer.id, existingSequencer)
        logger.info("The new sequencer is part of the ordering topology")
      } else {
        synchronizeTopologyAfterAddingSequencer(newSequencer.id, existingSequencer)
      }

      // now we can establish the sequencer snapshot
      val onboardingState =
        existingSequencer.setup.onboarding_state_for_sequencerV2(newSequencer.id)

      // finally, initialize "newSequencer"
      newSequencer.setup.assign_from_onboarding_stateV2(onboardingState).discard
    }
  }

  @Help.Summary("Repair utilities")
  @Help.Group("Repair")
  lazy val repair = new RepairMacros(loggerFactory)

  @Help.Summary("Convenience functions to configure behavior of pruning on all nodes at once")
  @Help.Group("Pruning")
  object pruning extends Helpful {

    @Help.Summary(
      "Sets the pruning schedule on all participants, mediators, and database sequencers in the environment"
    )
    def set_schedule(
        cron: String,
        maxDuration: config.PositiveDurationSeconds,
        retention: config.PositiveDurationSeconds,
    )(implicit env: ConsoleEnvironment): Unit =
      allPrunableNodes.foreach { case (name, prunable) =>
        prunable.set_schedule(cron, maxDuration, retention)
        logger.info(s"Enabled pruning of node $name at $cron")
      }

    @Help.Summary(
      "Deactivates scheduled pruning on all participants, mediators, and database sequencers in the environment"
    )
    def clear_schedule()(implicit env: ConsoleEnvironment): Unit =
      allPrunableNodes.foreach { case (name, prunable) =>
        prunable.clear_schedule()
        logger.info(s"Disabled pruning of node $name")
      }

    // Helper to find all HA-active nodes
    private def allPrunableNodes(implicit
        env: ConsoleEnvironment
    ): Map[String, PruningSchedulerAdministration[?]] =
      (env.participants.all.collect { case p if p.health.active => p.name -> p.pruning }
        ++ env.sequencers.all.collect {
          case s
              if s.health.status.successOption.exists(_.admin.acceptsAdminChanges) &&
                // TODO(#15987): Remove the Try when block sequencers support scheduled pruning
                util.Try(s.pruning.get_schedule().discard).isSuccess =>
            s.name -> s.pruning
        }
        ++ env.mediators.all.collect { case m if m.health.active => m.name -> m.pruning }).toMap

  }
}

object ConsoleMacros extends ConsoleMacros with NamedLogging {
  val loggerFactory = NamedLoggerFactory.root
}

object DebuggingHelpers extends LazyLogging {

  def get_active_contracts(
      ref: LocalParticipantReference,
      limit: PositiveInt = PositiveInt.tryCreate(1000000),
  ): (Map[String, String], Map[String, TemplateId]) =
    get_active_contracts_helper(
      ref,
      alias => ref.testing.pcs_search(alias, activeSet = true, limit = limit),
    )

  def get_active_contracts_from_internal_db_state(
      ref: ParticipantReference,
      state: SyncStateInspection,
      limit: PositiveInt = PositiveInt.tryCreate(1000000),
  ): (Map[String, String], Map[String, TemplateId]) =
    get_active_contracts_helper(
      ref,
      alias =>
        TraceContext.withNewTraceContext("get_active_contracts")(implicit traceContext =>
          state.findContracts(alias, None, None, None, limit.value)
        ),
    )

  private def get_active_contracts_helper(
      ref: ParticipantReference,
      lookup: SynchronizerAlias => Seq[(Boolean, ContractInstance)],
  ): (Map[String, String], Map[String, TemplateId]) = {
    val syncAcs = ref.synchronizers
      .list_connected()
      .map(_.synchronizerAlias)
      .flatMap(lookup)
      .collect {
        case (active, sc) if active =>
          (sc.contractId.coid, sc.templateId.qualifiedName.toString())
      }
      .toMap
    val lapiAcs =
      ref.ledger_api.state.acs.of_all().map(ev => (ev.event.contractId, ev.templateId)).toMap
    (syncAcs, lapiAcs)
  }

  def diff_active_contracts(ref: LocalParticipantReference, limit: Int = 1000000): Unit = {
    val (syncAcs, lapiAcs) = get_active_contracts(ref, limit)
    if (syncAcs.sizeCompare(lapiAcs) != 0) {
      logger.error(s"Sync ACS differs ${syncAcs.size} from Ledger API ACS ${lapiAcs.size} in size")
    }

    val lapiSet = lapiAcs.keySet
    val syncSet = syncAcs.keySet

    def compare[V](
        explain: String,
        lft: Set[String],
        rght: Set[String],
        payload: Map[String, V],
    ) = {
      val delta = lft.diff(rght)
      delta.foreach { key =>
        logger.info(s"$explain $key ${payload.getOrElse(key, sys.error("should be there"))}")
      }
    }

    compare("Active in LAPI but not in SYNC", lapiSet, syncSet, lapiAcs)
    compare("Active in SYNC but not in LAPI", syncSet, lapiSet, syncAcs)

  }

  def active_contracts_by_template(
      ref: LocalParticipantReference,
      limit: Int = 1000000,
  ): (Map[String, Int], Map[TemplateId, Int]) = {
    val (syncAcs, lapiAcs) = get_active_contracts(ref, limit)
    val groupedSync = syncAcs.toSeq
      .map { x =>
        x.swap
      }
      .groupBy(_._1)
      .map(x => (x._1, x._2.length))
    val groupedLapi = lapiAcs.toSeq
      .map { x =>
        x.swap
      }
      .groupBy(_._1)
      .map(x => (x._1, x._2.length))
    (groupedSync, groupedLapi)
  }

}
