// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.interactive.interactive_submission_service.DamlTransaction.Node.VersionedNode
import com.daml.ledger.api.v2.interactive.interactive_submission_service.Metadata
import com.daml.ledger.api.v2.interactive.interactive_submission_service.Metadata.ProcessedDisclosedContract.Contract
import com.daml.ledger.api.v2.interactive.transaction.v1.interactive_submission_data as isdv1
import com.daml.ledger.api.v2.interactive.{
  interactive_submission_common_data as iscd,
  interactive_submission_service as iss,
}
import com.daml.ledger.api.v2.value as lapiValue
import com.digitalasset.canton.data.ProcessedDisclosedContract
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.ExecuteRequest
import com.digitalasset.canton.ledger.api.validation.StricterValueValidator
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo.ExternallySignedSubmission
import com.digitalasset.canton.ledger.participant.state.{SubmitterInfo, Update}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.execution.CommandExecutionResult
import com.digitalasset.canton.platform.apiserver.services.command.interactive.PreparedTransactionCodec.*
import com.digitalasset.canton.platform.apiserver.services.command.interactive.PreparedTransactionDecoder.DeserializationResult
import com.digitalasset.canton.protocol.{LfNode, LfNodeId}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf
import com.digitalasset.daml.lf.data.Ref.TypeConName
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.NodeId
import com.digitalasset.daml.lf.value.Value
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import io.scalaland.chimney.dsl.TransformerConfiguration.UpdateFlag
import io.scalaland.chimney.dsl.{TransformedNamesComparison, TransformerConfiguration}
import io.scalaland.chimney.inlined.*
import io.scalaland.chimney.internal.runtime.TransformerFlags
import io.scalaland.chimney.partial.Result
import io.scalaland.chimney.syntax.*
import io.scalaland.chimney.{PartialTransformer, Transformer}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object PreparedTransactionDecoder {
  final case class DeserializationResult(
      commandExecutionResult: CommandExecutionResult,
      transactionUUID: UUID,
      mediatorGroup: Int,
  )
}

/** Class to decode a PreparedTransaction to an LF Transaction and its metadata.
  * Uses chimney to define Transformers and PartialTransformer for all conversions.
  */
final class PreparedTransactionDecoder(override val loggerFactory: NamedLoggerFactory)
    extends NamedLogging {

  // General config, applies to all transformers defined in this scope
  private implicit val transformerConfig
      : UpdateFlag[TransformerFlags.Enable[TransformerFlags.FieldNameComparison[
        TransformedNamesComparison.StrictEquality.type
      ], TransformerFlags.Default]] =
    TransformerConfiguration.default
      // Needed to avoid confusions in the proto generated classes method names with `get` prefix
      .enableCustomFieldNameComparison(TransformedNamesComparison.StrictEquality)

  /*
   * Decoders from LAPI values to LF values
   */
  private implicit def identifierTransformer(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): PartialTransformer[lapiValue.Identifier, lf.data.Ref.Identifier] =
    PartialTransformer { src =>
      StricterValueValidator
        .validateIdentifier(src)
        .leftMap(_.getMessage)
        .toResult
    }

  private implicit def valueTransformer(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): PartialTransformer[lapiValue.Value, lf.value.Value] =
    PartialTransformer { src =>
      StricterValueValidator
        .validateValue(src)
        .leftMap(_.getMessage)
        .toResult
    }

  /*
   * Generic collection decoders
   */
  private implicit def immArrayTransformer[A, B](implicit
      aToB: PartialTransformer[A, B]
  ): PartialTransformer[Seq[A], ImmArray[B]] =
    PartialTransformer { src =>
      src.toList
        .traverse(_.transformIntoPartial[B])
        .map(ImmArray.from)
    }

  /*
   * Straightforward decoders for simple proto values
   */
  private implicit val languageVersionTransformer
      : PartialTransformer[String, lf.language.LanguageVersion] =
    PartialTransformer {
      case "dev" =>
        Result.fromValue(lf.language.LanguageVersion.v2_dev)
      case src =>
        lf.language.LanguageVersion.fromString(src).toResult
    }

  private implicit val contractIdTransformer
      : PartialTransformer[String, lf.value.Value.ContractId] =
    PartialTransformer { src =>
      for {
        hexString <- lf.data.Ref.IdString.HexString.fromString(src).toResult
        bytes = lf.data.Ref.HexString.decode(hexString)
        contractId <- lf.value.Value.ContractId.fromBytes(bytes).toResult
      } yield contractId
    }
  private implicit val uuidTransformer: PartialTransformer[String, UUID] =
    PartialTransformer(src => Try(UUID.fromString(src)).toEither.leftMap(_.getMessage).toResult)

  private implicit val hashTransformer: PartialTransformer[ByteString, lf.crypto.Hash] =
    PartialTransformer { src =>
      lf.crypto.Hash.fromBytes(Bytes.fromByteString(src)).toResult
    }

  private implicit val commandIdTransformer: PartialTransformer[String, lf.data.Ref.CommandId] =
    PartialTransformer(src => lf.data.Ref.CommandId.fromString(src).toResult)

  private implicit val nodeSeedTransformer
      : PartialTransformer[iss.DamlTransaction.NodeSeed, (lf.transaction.NodeId, lf.crypto.Hash)] =
    PartialTransformer { src =>
      src.seed
        .transformIntoPartial[lf.crypto.Hash]
        .map(lf.transaction.NodeId(src.nodeId) -> _)
    }

  private implicit val bytesTransformer: Transformer[ByteString, Bytes] = (src: ByteString) =>
    Bytes.fromByteString(src)

  private implicit val timestampTransformer: PartialTransformer[Long, lf.data.Time.Timestamp] =
    PartialTransformer(src => lf.data.Time.Timestamp.fromLong(src).toResult)

  private implicit val packageNameTransformer: PartialTransformer[String, lf.data.Ref.PackageName] =
    PartialTransformer(src => lf.data.Ref.PackageName.fromString(src).toResult)

  private implicit val nameTransformer: PartialTransformer[String, lf.data.Ref.Name] =
    PartialTransformer(src => lf.data.Ref.Name.fromString(src).toResult)

  private implicit val partyTransformer: PartialTransformer[String, lf.data.Ref.Party] =
    PartialTransformer(src => lf.data.Ref.Party.fromString(src).toResult)

  private implicit val mediatorGroupIndexDecoder: PartialTransformer[Int, MediatorGroupIndex] =
    PartialTransformer(src => MediatorGroupIndex.create(src).leftMap(_.message).toResult)

  private implicit val nodeIdTransformer: PartialTransformer[String, lf.transaction.NodeId] =
    PartialTransformer { src =>
      src.toIntOption
        .toRight("Node Id is not a valid integer")
        .map(lf.transaction.NodeId.apply)
        .toResult
    }

  /*
   * Global key decoders
   */
  private implicit def globalKeyTransformer(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): PartialTransformer[iscd.GlobalKey, lf.transaction.GlobalKey] = {
    // GlobalKey default constructor is private, so create a constructor function from the companion builder
    // and pass that to chimney so it can construct the instance
    def globalKeyConstructor(
        templateId: TypeConName,
        key: Value,
        packageName: Ref.PackageName,
    ): Result[lf.transaction.GlobalKey] =
      lf.transaction.GlobalKey.build(templateId, key, packageName).leftMap(_.msg).toResult

    PartialTransformer
      .define[iscd.GlobalKey, lf.transaction.GlobalKey]
      .withConstructorPartial(globalKeyConstructor _)
      .buildTransformer
  }

  /*
   * Node Transformers
   * These transformers decode proto nodes to LF nodes. Each proto version can map to several LF version, which is why
   * the specific LF version is embedded in each proto node as a field, and set on the LF node during decoding.
   */

  /*
   * V1 Transformers
   */
  object v1 {
    // Make the create transformer visible to the package because some objects require explicitly decoding to a create node
    private[interactive] implicit def createNodeTransformer(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): PartialTransformer[isdv1.Create, lf.transaction.Node.Create] = Transformer
      .definePartial[isdv1.Create, lf.transaction.Node.Create]
      .withFieldRenamed(_.contractId, _.coid)
      .withFieldComputedPartial(
        _.arg,
        _.argument
          .traverse(_.transformIntoPartial[lf.value.Value])
          .flatMap(_.toRight("Missing argument value").toResult),
      )
      .withFieldConst(_.agreementText, "") // Agreement text will be removed
      .withFieldComputedPartial(_.version, _.lfVersion.transformIntoPartial[LanguageVersion])
      // Fields not supported in V1
      .withFieldConst(_.keyOpt, None)
      .withFieldConst(_.packageVersion, None)
      .buildTransformer

    private implicit def fetchTransformer(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): PartialTransformer[isdv1.Fetch, lf.transaction.Node.Fetch] = Transformer
      .definePartial[isdv1.Fetch, lf.transaction.Node.Fetch]
      .withFieldRenamed(_.contractId, _.coid)
      .withFieldComputedPartial(_.version, _.lfVersion.transformIntoPartial[LanguageVersion])
      // Not supported in V1
      .withFieldConst(_.keyOpt, None)
      .withFieldConst(_.byKey, false)
      .withFieldConst(_.interfaceId, None)
      .buildTransformer

    private implicit def exerciseTransformer(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): PartialTransformer[isdv1.Exercise, lf.transaction.Node.Exercise] =
      Transformer
        .definePartial[isdv1.Exercise, lf.transaction.Node.Exercise]
        .withFieldRenamed(_.contractId, _.targetCoid)
        .withFieldComputedPartial(
          _.choiceObservers,
          _.choiceObservers.traverse(_.transformIntoPartial[lf.data.Ref.Party]).map(_.toSet),
        )
        .withFieldComputedPartial(_.version, _.lfVersion.transformIntoPartial[LanguageVersion])
        // Fields not supported in V1
        .withFieldConst(_.keyOpt, None)
        .withFieldConst(_.byKey, false)
        .withFieldConst(_.choiceAuthorizers, None)
        .buildTransformer

    private implicit val rollbackTransformer
        : PartialTransformer[isdv1.Rollback, lf.transaction.Node.Rollback] =
      PartialTransformer.derive[isdv1.Rollback, lf.transaction.Node.Rollback]

    private[interactive] def nodeTransformer(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): PartialTransformer[isdv1.Node, lf.transaction.Node] = PartialTransformer {
      case isdv1.Node(create: isdv1.Node.NodeType.Create) =>
        create.value.transformIntoPartial[lf.transaction.Node.Create]
      case isdv1.Node(fetch: isdv1.Node.NodeType.Fetch) =>
        fetch.value.transformIntoPartial[lf.transaction.Node.Fetch]
      case isdv1.Node(exercise: isdv1.Node.NodeType.Exercise) =>
        exercise.value.transformIntoPartial[lf.transaction.Node.Exercise]
      case isdv1.Node(rollback: isdv1.Node.NodeType.Rollback) =>
        rollback.value.transformIntoPartial[lf.transaction.Node.Rollback]
      case isdv1.Node(isdv1.Node.NodeType.Empty) =>
        Result.fromErrorString("Cannot decode empty transaction node")
    }
  }

  // Version agnostic decoder from proto node to LF node
  private implicit def nodeTransformer(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): PartialTransformer[iss.DamlTransaction.Node, (lf.transaction.NodeId, lf.transaction.Node)] =
    PartialTransformer { node =>
      val versionedNode = node.versionedNode

      val decodedNodeResult = versionedNode match {
        case VersionedNode.V1(v1Node) => v1.nodeTransformer.transform(v1Node)
        case VersionedNode.Empty => Result.fromErrorString("Cannot decode empty versioned node")
      }

      for {
        nodeId <- node.nodeId.transformIntoPartial[NodeId]
        decodedNode <- decodedNodeResult
      } yield nodeId -> decodedNode
    }

  // Transaction decoder
  @VisibleForTesting
  private[interactive] implicit def transactionTransformer(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): PartialTransformer[iss.DamlTransaction, lf.transaction.VersionedTransaction] =
    PartialTransformer { src =>
      def lfVersionedConstructor(
          version: LanguageVersion,
          nodes: Map[LfNodeId, LfNode],
          roots: ImmArray[LfNodeId],
      ): lf.transaction.VersionedTransaction = lf.transaction.VersionedTransaction(
        version,
        nodes,
        roots,
      )

      src
        .intoPartial[lf.transaction.VersionedTransaction]
        .withFieldComputedPartial(
          _.nodes,
          _.nodes
            .traverse(_.transformIntoPartial[(lf.transaction.NodeId, lf.transaction.Node)])
            .map(_.toMap),
        )
        .withConstructor(lfVersionedConstructor _)
        .transform
    }

  // Global key mapping decoder
  private implicit def globalKeyMappingsTransformer(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): PartialTransformer[Seq[
    Metadata.GlobalKeyMappingEntry
  ], Map[lf.transaction.GlobalKey, Option[lf.value.Value.ContractId]]] =
    PartialTransformer { result =>
      result
        .traverse { case Metadata.GlobalKeyMappingEntry(keyOpt, valueOpt) =>
          for {
            convertedKey <- keyOpt
              .traverse(_.transformIntoPartial[lf.transaction.GlobalKey])
              .flatMap(_.toRight("Missing global key in key mappings").toResult)
            convertedValue <- valueOpt.traverse(_.transformIntoPartial[lf.value.Value])
            contractId <- convertedValue.traverse {
              case Value.ValueContractId(value) => Result.fromValue(value)
              case _ =>
                Result.fromErrorString(
                  s"Value with key $convertedValue in global key mapping was not a contract id"
                )
            }
          } yield convertedKey -> contractId
        }
        .map(_.toMap)
    }

  // Disclosed contract decoder
  private implicit def processedDisclosedContractTransformer(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): PartialTransformer[iss.Metadata.ProcessedDisclosedContract, ProcessedDisclosedContract] =
    PartialTransformer { src =>
      src
        .intoPartial[ProcessedDisclosedContract]
        .withFieldComputedPartial(
          _.create,
          contract =>
            // Here again we use the correct transformer version
            contract.contract match {
              case Contract.V1(value) => v1.createNodeTransformer.transform(value)
              case Contract.Empty =>
                Result.fromErrorString("Cannot decode empty disclosed contract")
            },
        )
        .transform
    }

  private def requireField[A](optA: Option[A], field: String)(implicit
      errorLoggingContext: ContextualizedErrorLogger
  ): Future[A] =
    Future.fromTry(
      optA.toRight(RequestValidationErrors.MissingField.Reject(field).asGrpcError).toTry
    )
  def extractLedgerEffectiveTime(executeRequest: ExecuteRequest)(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ContextualizedErrorLogger,
  ): Future[Option[Time.Timestamp]] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext
    for {
      metadataProto <- requireField(executeRequest.preparedTransaction.metadata, "metadata")
      letE = metadataProto.ledgerEffectiveTime.traverse(Time.Timestamp.fromLong)
      let <- letE.toResult.toFutureWithLoggedFailures(
        "Failed to deserialize transaction meta",
        logger,
      )
    } yield let
  }

  /** Decodes a prepared transaction back into a CommandExecutionResult that can be submitted.
    */
  def makeCommandExecutionResult(
      executeRequest: ExecuteRequest,
      ledgerEffectiveTime: Time.Timestamp,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ContextualizedErrorLogger,
  ): Future[DeserializationResult] = {
    implicit val traceContext = loggingContext.traceContext

    for {
      metadataProto <- requireField(executeRequest.preparedTransaction.metadata, "metadata")
      submitterInfoProto <- requireField(metadataProto.submitterInfo, "submitter_info")
      transactionUUID <- metadataProto.transactionUuid
        .transformIntoPartial[UUID]
        .toFutureWithLoggedFailures("Failed to deserialize transaction UUID", logger)
      submitterInfo <- submitterInfoProto
        .intoPartial[SubmitterInfo]
        // Read as is unused for the execution as the transaction has already been run through Daml engine at this point
        .withFieldConst(_.readAs, List.empty)
        .withFieldConst(_.submissionId, Some(executeRequest.submissionId))
        .withFieldConst(_.applicationId, executeRequest.applicationId)
        .withFieldConstPartial(
          _.commandId,
          submitterInfoProto.commandId.transformIntoPartial[lf.data.Ref.CommandId],
        )
        .withFieldConst(_.deduplicationPeriod, executeRequest.deduplicationPeriod)
        .withFieldConstPartial(
          _.externallySignedSubmission,
          for {
            mediatorGroup <- metadataProto.mediatorGroup.transformIntoPartial[MediatorGroupIndex]
          } yield Some(
            ExternallySignedSubmission(
              executeRequest.serializationVersion,
              executeRequest.signatures,
              transactionUUID = transactionUUID,
              mediatorGroup = mediatorGroup,
              usesLedgerEffectiveTime = metadataProto.ledgerEffectiveTime.isDefined,
            )
          ),
        )
        .transform
        .toFutureWithLoggedFailures("Failed to deserialize submitter info", logger)
      domainId <- Future.fromTry(
        DomainId
          .fromProtoPrimitive(metadataProto.domainId, "domain_id")
          .leftMap(_.message)
          .leftMap(RequestValidationErrors.InvalidArgument.Reject(_).asGrpcError)
          .toTry
      )
      transactionProto <- requireField(
        executeRequest.preparedTransaction.transaction,
        "transaction",
      )
      transactionMeta <-
        metadataProto
          .intoPartial[state.TransactionMeta]
          // Unused field
          .withFieldConst(_.optUsedPackages, None)
          // Submission seed is irrelevant at this point, as we already have the individual node seeds, which are signed
          // and shipped to the participants
          .withFieldConst(_.submissionSeed, Update.noOpSeed)
          .withFieldConstPartial(
            _.optNodeSeeds,
            transactionProto.nodeSeeds
              .transformIntoPartial[ImmArray[(NodeId, lf.crypto.Hash)]]
              .map(Some(_)),
          )
          // Unused field
          .withFieldConst(_.optByKeyNodes, None)
          // Workflow ID is not supported for interactive submissions
          .withFieldConst(_.workflowId, None)
          .withFieldConst(
            _.ledgerEffectiveTime,
            ledgerEffectiveTime,
          )
          .transform
          .toFutureWithLoggedFailures("Failed to deserialize transaction meta", logger)
      transaction <- transactionProto
        .transformIntoPartial[lf.transaction.VersionedTransaction]
        .toFutureWithLoggedFailures("Failed to deserialize transaction", logger)
      globalKeyMapping <- metadataProto.globalKeyMapping
        .transformIntoPartial[Map[lf.transaction.GlobalKey, Option[lf.value.Value.ContractId]]]
        .toFutureWithLoggedFailures("Failed to deserialize global key mapping", logger)
      processedDisclosedContracts <- metadataProto.disclosedEvents
        .transformIntoPartial[ImmArray[com.digitalasset.canton.data.ProcessedDisclosedContract]]
        .toFutureWithLoggedFailures("Failed to deserialize disclosed contracts", logger)
    } yield {
      val commandExecutionResult = CommandExecutionResult(
        submitterInfo = submitterInfo,
        optDomainId = Some(domainId),
        transactionMeta = transactionMeta,
        transaction = lf.transaction.SubmittedTransaction(transaction),
        dependsOnLedgerTime = metadataProto.ledgerEffectiveTime.isDefined,
        // Unused
        interpretationTimeNanos = 0L,
        globalKeyMapping = globalKeyMapping,
        processedDisclosedContracts = processedDisclosedContracts,
      )

      DeserializationResult(commandExecutionResult, transactionUUID, metadataProto.mediatorGroup)
    }
  }
}
