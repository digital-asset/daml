// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import better.files.File
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, Logger}
import ch.qos.logback.core.spi.AppenderAttachable
import ch.qos.logback.core.{Appender, FileAppender}
import com.daml.ledger.api.v1.commands.{Command, CreateCommand, ExerciseCommand}
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.value.Value.Sum
import com.daml.ledger.api.v1.value.{
  Identifier as IdentifierV1,
  List as ListV1,
  Optional,
  Record,
  RecordField,
  Value,
}
import com.daml.lf.value.Value.ContractId
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps.*
import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers.ContractData
import com.digitalasset.canton.admin.api.client.data
import com.digitalasset.canton.admin.api.client.data.{ListPartiesResult, TemplateId}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.ConsoleEnvironment.Implicits.*
import com.digitalasset.canton.crypto.{CryptoPureApi, Salt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.admin.data.{
  MediatorNodeStatus,
  NodeStatus,
  SequencerNodeStatus,
}
import com.digitalasset.canton.logging.{LastErrorsAppender, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection
import com.digitalasset.canton.participant.admin.repair.RepairService
import com.digitalasset.canton.participant.config.{AuthServiceConfig, BaseParticipantConfig}
import com.digitalasset.canton.participant.ledger.api.JwtTokenUtilities
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.{
  GenericSignedTopologyTransactionX,
  PositiveSignedTopologyTransactionX,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.{BinaryFileUtil, EitherUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DomainAlias, SequencerAlias}
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax.*

import java.io.File as JFile
import java.time.Instant
import scala.annotation.nowarn
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

trait ConsoleMacros extends NamedLogging with NoTracing {
  import scala.reflect.runtime.universe.*

  @Help.Summary("Console utilities")
  @Help.Group("Utilities")
  object utils extends Helpful {

    @Help.Summary("Reflective inspection of object arguments, handy to inspect case class objects")
    @Help.Description(
      "Return the list field names of the given object. Helpful function when inspecting the return result."
    )
    def object_args[T: TypeTag](obj: T): List[String] = type_args[T]

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
    )(implicit env: ConsoleEnvironment): Unit = {
      ConsoleMacros.utils.retry_until_true(timeoutO.getOrElse(env.commandTimeouts.bounded)) {
        env.nodes.all.forall(_.topology.synchronisation.is_idle())
      }
    }

    @nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
    private object GenerateDamlScriptParticipantsConf {
      import ConsoleEnvironment.Implicits.*

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
          participantDomains =>
            (p.party.filterString, participantReference(participantDomains.participant))
        }

        val partyAndParticipants =
          env.participantsX.all.flatMap(_.parties.list().flatMap(partyIdToParticipant(_).toList))
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
          defaultParticipantX: Option[ParticipantReferenceX] = None,
      )(implicit env: ConsoleEnvironment): JFile = {

        def toLedgerApi(participantConfig: BaseParticipantConfig) =
          LedgerApi(
            participantConfig.clientLedgerApi.address,
            participantConfig.clientLedgerApi.port.unwrap,
          )

        def participantValue(p: ParticipantReferenceX): String =
          if (useParticipantAlias) p.name else p.uid.toProtoPrimitive

        val allParticipants = env.participantsX.all
        val participantsData =
          allParticipants.map(p => (participantValue(p), toLedgerApi(p.config))).toMap
        val uidToAlias = allParticipants.map(p => (p.id, p.name)).toMap

        val default_participant =
          defaultParticipantX.map(participantReference => toLedgerApi(participantReference.config))

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
        |- defaultParticipantX (default to None): adds a default participant if provided
        |"""
    )
    def generate_daml_script_participants_conf(
        file: Option[String] = None,
        useParticipantAlias: Boolean = true,
        defaultParticipantX: Option[ParticipantReferenceX] = None,
    )(implicit env: ConsoleEnvironment): JFile =
      GenerateDamlScriptParticipantsConf(
        file,
        useParticipantAlias,
        defaultParticipantX,
      )

    // TODO(i7387): add check that flag is set
    @Help.Summary(
      "Register `AutoCloseable` object to be shutdown if Canton is shut down",
      FeatureFlag.Testing,
    )
    def auto_close(closeable: AutoCloseable)(implicit environment: ConsoleEnvironment): Unit = {
      environment.environment.addUserCloseable(closeable)
    }

    @Help.Summary("Convert contract data to a contract instance.")
    @Help.Description(
      """The `utils.contract_data_to_instance` bridges the gap between `participant.ledger_api.acs` commands that
        |return various pieces of "contract data" and the `participant.repair.add` command used to add "contract instances"
        |as part of repair workflows. Such workflows (for example migrating contracts from other Daml ledgers to Canton
        |participants) typically consist of extracting contract data using `participant.ledger_api.acs` commands,
        |modifying the contract data, and then converting the `contractData` using this function before finally
        |adding the resulting contract instances to Canton participants via `participant.repair.add`.
        |Obtain the `contractData` by invoking `.toContractData` on the `WrappedCreatedEvent` returned by the
        |corresponding `participant.ledger_api.acs.of_party` or `of_all` call. The `ledgerTime` parameter should be
        |chosen to be a time meaningful to the domain on which you plan to subsequently invoke `participant.repair.add`
        |on and will be retained alongside the contract instance by the `participant.repair.add` invocation."""
    )
    def contract_data_to_instance(contractData: ContractData, ledgerTime: Instant)(implicit
        env: ConsoleEnvironment
    ): SerializableContract =
      TraceContext.withNewTraceContext { implicit traceContext =>
        env.runE(
          RepairService.ContractConverter.contractDataToInstance(
            contractData.templateId.toIdentifier,
            contractData.createArguments,
            contractData.signatories,
            contractData.observers,
            contractData.inheritedContractId,
            contractData.ledgerCreateTime.map(_.toInstant).getOrElse(ledgerTime),
            contractData.contractSalt,
          )
        )
      }

    @Help.Summary("Convert a contract instance to contract data.")
    @Help.Description(
      """The `utils.contract_instance_to_data` converts a Canton "contract instance" to "contract data", a format more
        |amenable to inspection and modification as part of repair workflows. This function consumes the output of
        |the `participant.testing` commands and can thus be employed in workflows geared at verifying the contents of
        |contracts for diagnostic purposes and in environments in which the "features.enable-testing-commands"
        |configuration can be (at least temporarily) enabled."""
    )
    def contract_instance_to_data(
        contract: SerializableContract
    )(implicit env: ConsoleEnvironment): ContractData =
      env.runE(
        RepairService.ContractConverter.contractInstanceToData(contract).map {
          case (
                templateId,
                createArguments,
                signatories,
                observers,
                contractId,
                contractSaltO,
                ledgerCreateTime,
              ) =>
            ContractData(
              TemplateId.fromIdentifier(templateId),
              createArguments,
              signatories,
              observers,
              contractId,
              contractSaltO,
              Some(ledgerCreateTime.ts.underlying),
            )
        }
      )

    @Help.Summary("Recompute authenticated contract ids.")
    @Help.Description(
      """The `utils.recompute_contract_ids` regenerates "contract ids" of multiple contracts after their contents have
        |changed. Starting from protocol version 4, Canton uses the so called authenticated contract ids which depend
        |on the details of the associated contracts. When aspects of a contract such as the parties involved change as
        |part of repair or export/import procedure, the corresponding contract id must be recomputed."""
    )
    def recompute_contract_ids(
        participant: LocalParticipantReferenceX,
        acs: Seq[SerializableContract],
        protocolVersion: ProtocolVersion,
    ): (Seq[SerializableContract], Map[LfContractId, LfContractId]) = {
      val contractIdMappings = mutable.Map.empty[LfContractId, LfContractId]
      // We assume ACS events are in order
      val remappedCIds = acs.map { contract =>
        // Update the referenced contract ids
        val contractInstanceWithUpdatedContractIdReferences =
          SerializableRawContractInstance
            .create(
              contract.rawContractInstance.contractInstance.map(_.mapCid(contractIdMappings)),
              AgreementText.empty, // Empty is fine, because the agreement text is not used when generating the raw serializable contract hash
            )
            .valueOr(err =>
              throw new RuntimeException(
                s"Could not create serializable raw contract instance: $err"
              )
            )

        val LfContractId.V1(discriminator, _) = contract.contractId
        val contractSalt = contract.contractSalt.getOrElse(
          throw new IllegalArgumentException("Missing contract salt")
        )
        val pureCrypto = participant.underlying
          .map(_.cryptoPureApi)
          .getOrElse(sys.error("where is my crypto?"))

        // Compute the new contract id
        val newContractId =
          generate_contract_id(
            cryptoPureApi = pureCrypto,
            rawContract = contractInstanceWithUpdatedContractIdReferences,
            createdAt = contract.ledgerCreateTime.ts,
            discriminator = discriminator,
            contractSalt = contractSalt,
            metadata = contract.metadata,
          )

        // Update the contract id mappings with the current contract's id
        contractIdMappings += contract.contractId -> newContractId

        // Update the contract with the new contract id and recomputed instance
        contract
          .copy(
            contractId = newContractId,
            rawContractInstance = contractInstanceWithUpdatedContractIdReferences,
          )
      }

      remappedCIds -> Map.from(contractIdMappings)
    }

    @Help.Summary("Generate authenticated contract id.")
    @Help.Description(
      """The `utils.generate_contract_id` generates "contract id" of a contract. Starting from protocol version 4,
        |Canton uses the so called authenticated contract ids which depend on the details of the associated contracts.
        |When aspects of a contract such as the parties involved change as part of repair or export/import procedure,
        |the corresponding contract id must be recomputed. This function can be used as a tool to generate an id for
        |an arbitrary contract content"""
    )
    def generate_contract_id(
        cryptoPureApi: CryptoPureApi,
        rawContract: SerializableRawContractInstance,
        createdAt: CantonTimestamp,
        discriminator: LfHash,
        contractSalt: Salt,
        metadata: ContractMetadata,
    ): ContractId.V1 = {
      val unicumGenerator = new UnicumGenerator(cryptoPureApi)
      val cantonContractIdVersion = AuthenticatedContractIdVersionV2
      val unicum = unicumGenerator
        .recomputeUnicum(
          contractSalt,
          LedgerCreateTime(createdAt),
          metadata,
          rawContract,
          cantonContractIdVersion,
        )
        .valueOr(err => throw new RuntimeException(err))
      cantonContractIdVersion.fromDiscriminator(discriminator, unicum)
    }

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

  @Help.Summary("Canton development and testing utilities", FeatureFlag.Testing)
  @Help.Group("Ledger Api Testing")
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
      def safeMapCast(map: Map[_, _]): Map[String, Any] = map.map { case (key, value) =>
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
        case x: Map[_, _] => Value.Sum.Record(buildArguments(safeMapCast(x)))
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

    @Help.Summary("Build create command", FeatureFlag.Testing)
    def create(
        packageId: String,
        module: String,
        template: String,
        arguments: Map[String, Any],
    ): Command =
      Command().withCreate(
        CreateCommand(
          templateId = Some(buildIdentifier(packageId, module, template)),
          createArguments = Some(buildArguments(arguments)),
        )
      )

    @Help.Summary("Build exercise command", FeatureFlag.Testing)
    def exercise(
        packageId: String,
        module: String,
        template: String,
        choice: String,
        arguments: Map[String, Any],
        contractId: String,
    ): Command =
      Command().withExercise(
        ExerciseCommand(
          templateId = Some(buildIdentifier(packageId, module, template)),
          choice = choice,
          choiceArgument = Some(Value(Value.Sum.Record(buildArguments(arguments)))),
          contractId = contractId,
        )
      )

    @Help.Summary("Build exercise command from CreatedEvent", FeatureFlag.Testing)
    def exercise(choice: String, arguments: Map[String, Any], event: CreatedEvent): Command = {
      def getOrThrow(desc: String, opt: Option[String]): String =
        opt.getOrElse(
          throw new IllegalArgumentException(s"Corrupt created event ${event} without ${desc}")
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

    // intentionally not publicly documented
    object jwt {
      def generate_unsafe_token_for_participant(
          participant: LocalParticipantReferenceX,
          admin: Boolean,
          applicationId: String,
      ): Map[PartyId, String] = {
        val secret = participant.config.ledgerApi.authServices
          .collectFirst { case AuthServiceConfig.UnsafeJwtHmac256(secret, _, _) =>
            secret.unwrap
          }
          .getOrElse("notasecret")

        participant.parties
          .hosted()
          .map(_.party)
          .map(x =>
            (
              x,
              generate_unsafe_jwt256_token(
                secret = secret,
                admin = admin,
                readAs = List(x.toLf),
                actAs = List(x.toLf),
                ledgerId = Some(participant.id.uid.id.unwrap),
                applicationId = Some(applicationId),
              ),
            )
          )
          .toMap
      }

      def generate_unsafe_jwt256_token(
          secret: String,
          admin: Boolean,
          readAs: List[String],
          actAs: List[String],
          ledgerId: Option[String],
          applicationId: Option[String],
      ): String = JwtTokenUtilities.buildUnsafeToken(
        secret = secret,
        admin = admin,
        readAs = readAs,
        actAs = actAs,
        ledgerId = ledgerId,
        applicationId = applicationId,
      )
    }
  }

  @Help.Summary("Logging related commands")
  @Help.Group("Logging")
  object logging extends Helpful {

    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    @Help.Summary("Dynamically change log level (TRACE, DEBUG, INFO, WARN, ERROR, OFF, null)")
    def set_level(loggerName: String = "com.digitalasset.canton", level: String): Unit = {
      if (Seq("com.digitalasset.canton", "com.daml").exists(loggerName.startsWith))
        System.setProperty("LOG_LEVEL_CANTON", level)

      val logger = getLogger(loggerName)
      if (level == "null")
        logger.setLevel(null)
      else
        logger.setLevel(Level.valueOf(level))
    }

    @Help.Summary("Determine current logging level")
    def get_level(loggerName: String = "com.digitalasset.canton"): Option[Level] =
      Option(getLogger(loggerName).getLevel)

    private def getLogger(loggerName: String): Logger = {
      import org.slf4j.LoggerFactory
      @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
      val logger: Logger = LoggerFactory.getLogger(loggerName).asInstanceOf[Logger]
      logger
    }

    private def getAppenders(logger: Logger): List[Appender[ILoggingEvent]] = {
      def go(currentAppender: Appender[ILoggingEvent]): List[Appender[ILoggingEvent]] = {
        currentAppender match {
          case attachable: AppenderAttachable[ILoggingEvent @unchecked] =>
            attachable.iteratorForAppenders().asScala.toList.flatMap(go)
          case appender: Appender[ILoggingEvent] => List(appender)
        }
      }

      logger.iteratorForAppenders().asScala.toList.flatMap(go)
    }

    private lazy val rootLogger = getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)

    private lazy val allAppenders = getAppenders(rootLogger)

    private lazy val lastErrorsAppender: LastErrorsAppender = {
      findAppender("LAST_ERRORS") match {
        case Some(lastErrorsAppender: LastErrorsAppender) =>
          lastErrorsAppender
        case _ =>
          logger.error(s"Log appender for last errors not found/configured")
          throw new CommandFailure()
      }
    }

    private def findAppender(appenderName: String): Option[Appender[ILoggingEvent]] =
      Option(rootLogger.getAppender(appenderName))
        .orElse(allAppenders.find(_.getName == appenderName))

    private def renderError(errorEvent: ILoggingEvent): String = {
      findAppender("FILE") match {
        case Some(appender: FileAppender[ILoggingEvent]) =>
          ByteString.copyFrom(appender.getEncoder.encode(errorEvent)).toStringUtf8
        case _ => errorEvent.getFormattedMessage
      }
    }

    @Help.Summary("Returns the last errors (trace-id -> error event) that have been logged locally")
    def last_errors(): Map[String, String] =
      lastErrorsAppender.lastErrors.fmap(renderError)

    @Help.Summary("Returns log events for an error with the same trace-id")
    def last_error_trace(traceId: String): Seq[String] = {
      lastErrorsAppender.lastErrorTrace(traceId) match {
        case Some(events) => events.map(renderError)
        case None =>
          logger.error(s"No events found for last error trace-id $traceId")
          throw new CommandFailure()
      }
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
    def disable_features(flag: FeatureFlag)(implicit env: ConsoleEnvironment): Unit = {
      env.updateFeatureSet(flag, include = false)
    }

    // this command is intentionally not documented as part of the help system
    def enable_features(flag: FeatureFlag)(implicit env: ConsoleEnvironment): Unit = {
      env.updateFeatureSet(flag, include = true)
    }
  }

  @Help.Summary("Functions to bootstrap/setup decentralized namespaces or full domains")
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
        owners: Seq[InstanceReferenceX],
        store: String = AuthorizedStore.filterName,
    ): (Namespace, Seq[GenericSignedTopologyTransactionX]) = {
      val ownersNE = NonEmpty
        .from(owners)
        .getOrElse(
          throw new IllegalArgumentException(
            "There must be at least 1 owner for a decentralizedNamespace."
          )
        )
      val expectedDNS = DecentralizedNamespaceDefinitionX.computeNamespace(
        owners.map(_.id.member.uid.namespace).toSet
      )
      val proposedOrExisting = ownersNE
        .map { owner =>
          val existingDnsO =
            owner.topology.transactions
              .findLatestByMappingHash[DecentralizedNamespaceDefinitionX](
                DecentralizedNamespaceDefinitionX.uniqueKey(expectedDNS),
                filterStore = AuthorizedStore.filterName,
                includeProposals = true,
              )
              .map(_.transaction)

          existingDnsO.getOrElse(
            owner.topology.decentralized_namespaces.propose(
              owners.map(_.id.member.uid.namespace).toSet,
              PositiveInt.tryCreate(1.max(owners.size - 1)),
              store = store,
            )
          )
        }

      // require that all proposed or previously existing transactions have the same hash,
      // otherwise there is no chance for success
      if (proposedOrExisting.distinctBy(_.transaction.hash).sizeIs != 1) {
        throw new IllegalStateException(
          s"Proposed or previously existing transactions disagree on the founding of the domain's decentralized namespace:\n$proposedOrExisting"
        )
      }

      // merging the signatures here is an "optimization" so that later we only upload a single
      // decentralizedNamespace transaction, instead of a transaction per owner.
      val decentralizedNamespaceDefinition =
        proposedOrExisting.reduceLeft[SignedTopologyTransactionX[
          TopologyChangeOpX,
          DecentralizedNamespaceDefinitionX,
        ]]((txA, txB) => txA.addSignatures(txB.signatures.forgetNE.toSeq))

      val ownerNSDs = owners.flatMap(_.topology.transactions.identity_transactions())
      val foundingTransactions = ownerNSDs :+ decentralizedNamespaceDefinition

      owners.foreach(_.topology.transactions.load(foundingTransactions, store = store))

      (decentralizedNamespaceDefinition.transaction.mapping.namespace, foundingTransactions)
    }

    private def expected_namespace(
        owners: Seq[InstanceReferenceX]
    ): Either[String, Option[Namespace]] = {
      val expectedNamespace =
        DecentralizedNamespaceDefinitionX.computeNamespace(
          owners.map(_.id.member.uid.namespace).toSet
        )
      val recordedNamespaces =
        owners.map(
          _.topology.transactions
            .findLatestByMappingHash[DecentralizedNamespaceDefinitionX](
              DecentralizedNamespaceDefinitionX.uniqueKey(expectedNamespace),
              filterStore = AuthorizedStore.filterName,
              includeProposals = true,
            )
            .map(_.mapping.namespace)
        )

      recordedNamespaces.distinct match {
        case Seq(namespaceO) => Right(namespaceO)
        case otherwise =>
          Left(s"the domain owners do not agree on the decentralizedNamespace: $otherwise")
      }
    }

    private def in_domain(
        sequencers: NonEmpty[Seq[SequencerNodeReferenceX]],
        mediators: NonEmpty[Seq[MediatorReferenceX]],
    )(domainId: DomainId): Either[String, Option[DomainId]] = {
      def isNotInitializedOrSuccessWithDomain(
          instance: InstanceReferenceX
      ): Either[String, Boolean /* isInitializedWithDomain */ ] = {
        instance.health.status match {
          case nonFailure if nonFailure.isActive.contains(false) =>
            Left(s"${instance.id.member} is currently not active")
          case NodeStatus.Success(status: SequencerNodeStatus) =>
            // sequencer is already fully initialized for this domain
            Either.cond(
              status.domainId == domainId,
              true,
              s"${instance.id.member} has already been initialized for domain ${status.domainId} instead of $domainId.",
            )
          case NodeStatus.Success(status: MediatorNodeStatus) =>
            // mediator is already fully initialized for this domain
            Either.cond(
              status.domainId == domainId,
              true,
              s"${instance.id.member} has already been initialized for domain ${status.domainId} instead of $domainId",
            )
          case NodeStatus.NotInitialized(true) =>
            // the node is not yet initialized for this domain
            Right(false)
          case NodeStatus.Failure(msg) =>
            Left(s"Error obtaining status for ${instance.id.member}: $msg")
          case otherwise =>
            // Unexpected status response. All cases should be covered by the patterns above
            Left(s"Unexpected status: $otherwise")
        }
      }

      val alreadyFullyInitialized =
        (sequencers ++ mediators).forgetNE.toSeq.traverse(isNotInitializedOrSuccessWithDomain(_))
      alreadyFullyInitialized
        .map(nodesOnDomainOrNotInitialized =>
          // any false in the result means, that some nodes haven't been initialized yet
          if (nodesOnDomainOrNotInitialized.forall(identity)) Some(domainId)
          else None
        )
    }

    private def no_domain(nodes: NonEmpty[Seq[InstanceReferenceX]]): Either[String, Unit] =
      EitherUtil.condUnitE(
        !nodes.exists(_.health.initialized()),
        "the domain has not yet been bootstrapped but some sequencers or mediators are already part of one",
      )

    private def check_domain_bootstrap_status(
        name: String,
        owners: Seq[InstanceReferenceX],
        sequencers: Seq[SequencerNodeReferenceX],
        mediators: Seq[MediatorReferenceX],
    ): Either[String, Option[DomainId]] =
      for {
        neOwners <- NonEmpty.from(owners.distinct).toRight("you need at least one domain owner")
        neSequencers <- NonEmpty
          .from(sequencers.distinct)
          .toRight("you need at least one sequencer")
        neMediators <- NonEmpty.from(mediators.distinct).toRight("you need at least one mediator")
        nodes = neOwners ++ neSequencers ++ neMediators
        _ = EitherUtil.condUnitE(nodes.forall(_.health.running()), "all nodes must be running")
        ns <- expected_namespace(neOwners)
        expectedId = ns.map(ns => DomainId(UniqueIdentifier.tryCreate(name, ns.toProtoPrimitive)))
        actualIdIfAllNodesAreInitialized <- expectedId.fold(
          no_domain(neSequencers ++ neMediators).map[Option[DomainId]](_ => None)
        )(in_domain(neSequencers, neMediators))
      } yield actualIdIfAllNodesAreInitialized

    private def run_bootstrap(
        domainName: String,
        staticDomainParameters: data.StaticDomainParameters,
        domainOwners: Seq[InstanceReferenceX],
        sequencers: Seq[SequencerNodeReferenceX],
        mediators: Seq[MediatorReferenceX],
    ): DomainId = {
      val (decentralizedNamespace, foundingTxs) =
        bootstrap.decentralized_namespace(domainOwners, store = AuthorizedStore.filterName)

      val domainId = DomainId(
        UniqueIdentifier.tryCreate(domainName, decentralizedNamespace.toProtoPrimitive)
      )

      val seqMedIdentityTxs =
        (sequencers ++ mediators).flatMap(_.topology.transactions.identity_transactions())
      domainOwners.foreach(
        _.topology.transactions.load(seqMedIdentityTxs, store = AuthorizedStore.filterName)
      )

      val domainGenesisTxs = domainOwners.flatMap(
        _.topology.domain_bootstrap.generate_genesis_topology(
          domainId,
          domainOwners.map(_.id.member),
          sequencers.map(_.id),
          mediators.map(_.id),
        )
      )

      val initialTopologyState = (foundingTxs ++ seqMedIdentityTxs ++ domainGenesisTxs)
        .mapFilter(_.selectOp[TopologyChangeOpX.Replace])

      // TODO(#12390) replace this merge / active with proper tooling and checks that things are really fully authorized
      val orderingMap =
        Seq(
          NamespaceDelegationX.code,
          OwnerToKeyMappingX.code,
          DecentralizedNamespaceDefinitionX.code,
        ).zipWithIndex.toMap
          .withDefaultValue(5)

      val merged = initialTopologyState
        .groupBy1(_.transaction.hash)
        .values
        .map(
          // combine signatures of transactions with the same hash
          _.reduceLeft[PositiveSignedTopologyTransactionX] { (a, b) =>
            a.addSignatures(b.signatures.toSeq)
          }.copy(isProposal = false)
        )
        .toSeq
        .sortBy(tx => orderingMap(tx.transaction.mapping.code))

      sequencers
        .filterNot(_.health.initialized())
        .foreach(x => x.setup.assign_from_beginning(merged, staticDomainParameters).discard)

      mediators
        .filter(!_.health.initialized())
        .foreach(
          _.setup
            .assign(
              domainId,
              staticDomainParameters,
              SequencerConnections.tryMany(
                sequencers
                  .map(s => s.sequencerConnection.withAlias(SequencerAlias.tryCreate(s.name))),
                PositiveInt.tryCreate(1),
              ),
            )
        )

      domainId
    }

    @Help.Summary(
      """Bootstraps a new domain with the given static domain parameters and members. Any participants as domain owners
        |must still manually connect to the domain afterwards."""
    )
    def domain(
        domainName: String,
        sequencers: Seq[SequencerNodeReferenceX],
        mediators: Seq[MediatorReferenceX],
        domainOwners: Seq[InstanceReferenceX] = Seq.empty,
        staticDomainParameters: data.StaticDomainParameters =
          data.StaticDomainParameters.defaultsWithoutKMS,
    ): DomainId = {
      val domainOwnersOrDefault = if (domainOwners.isEmpty) sequencers else domainOwners
      check_domain_bootstrap_status(
        domainName,
        domainOwnersOrDefault,
        sequencers,
        mediators,
      ) match {
        case Right(Some(domainId)) =>
          logger.info(s"Domain $domainName has already been bootstrapped with ID $domainId")
          domainId
        case Right(None) =>
          run_bootstrap(
            domainName,
            staticDomainParameters,
            domainOwnersOrDefault,
            sequencers,
            mediators,
          )
        case Left(error) =>
          val message = s"The domain cannot be bootstrapped: $error"
          logger.error(message)
          sys.error(message)
      }
    }
  }

}

object ConsoleMacros extends ConsoleMacros with NamedLogging {
  val loggerFactory = NamedLoggerFactory.root
}

object DebuggingHelpers extends LazyLogging {

  def get_active_contracts(
      ref: LocalParticipantReferenceCommon[?],
      limit: PositiveInt = PositiveInt.tryCreate(1000000),
  ): (Map[String, String], Map[String, TemplateId]) =
    get_active_contracts_helper(
      ref,
      alias => ref.testing.pcs_search(alias, activeSet = true, limit = limit),
    )

  def get_active_contracts_from_internal_db_state(
      ref: ParticipantReferenceCommon,
      state: SyncStateInspection,
      limit: PositiveInt = PositiveInt.tryCreate(1000000),
  ): (Map[String, String], Map[String, TemplateId]) =
    get_active_contracts_helper(
      ref,
      alias =>
        TraceContext.withNewTraceContext(implicit traceContext =>
          state.findContracts(alias, None, None, None, limit.value)
        ),
    )

  private def get_active_contracts_helper(
      ref: ParticipantReferenceCommon,
      lookup: DomainAlias => Seq[(Boolean, SerializableContract)],
  ): (Map[String, String], Map[String, TemplateId]) = {
    val syncAcs = ref.domains
      .list_connected()
      .map(_.domainAlias)
      .flatMap(lookup)
      .collect {
        case (active, sc) if active =>
          (sc.contractId.coid, sc.contractInstance.unversioned.template.qualifiedName.toString())
      }
      .toMap
    val lapiAcs =
      ref.ledger_api_v2.state.acs.of_all().map(ev => (ev.event.contractId, ev.templateId)).toMap
    (syncAcs, lapiAcs)
  }

  def diff_active_contracts(ref: LocalParticipantReferenceCommon[?], limit: Int = 1000000): Unit = {
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
        logger.info(s"${explain} ${key} ${payload.getOrElse(key, sys.error("should be there"))}")
      }
    }

    compare("Active in LAPI but not in SYNC", lapiSet, syncSet, lapiAcs)
    compare("Active in SYNC but not in LAPI", syncSet, lapiSet, syncAcs)

  }

  def active_contracts_by_template(
      ref: LocalParticipantReferenceCommon[?],
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
