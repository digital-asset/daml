// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive

import cats.syntax.traverse.*
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.interactive.interactive_submission_service.DamlTransaction.Node.VersionedNode
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{DamlTransaction, Metadata}
import com.daml.ledger.api.v2.interactive.transaction.v1.interactive_submission_data as isdv1
import com.daml.ledger.api.v2.interactive.{
  interactive_submission_common_data as iscd,
  interactive_submission_service as iss,
}
import com.daml.ledger.api.v2.value as lapiValue
import com.digitalasset.canton.data.ProcessedDisclosedContract
import com.digitalasset.canton.ledger.api.util.LfEngineToApi
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.execution.CommandExecutionResult
import com.digitalasset.canton.platform.apiserver.services.command.interactive.PreparedTransactionCodec.*
import com.digitalasset.canton.platform.store.dao.events.LfValueTranslation
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.{Bytes, ImmArray}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.{GlobalKey, Node, NodeId, TransactionVersion}
import com.digitalasset.daml.lf.value.Value
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import io.scalaland.chimney.dsl.*
import io.scalaland.chimney.partial.Result
import io.scalaland.chimney.{PartialTransformer, Transformer}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/** Class to encode an LF Transaction and its metadata to a PreparedTransaction.
  * Uses chimney to define Transformers and PartialTransformer for all conversions.
  */
final class PreparedTransactionEncoder(
    override val loggerFactory: NamedLoggerFactory,
    lfValueTranslation: LfValueTranslation,
) extends NamedLogging {

  /** Defines the mapping between LF version and Encoding versions.
    * An encoding version can be used for several LF Versions.
    */
  private val nodeTransformers = Map(
    LanguageVersion.v2_1 -> v1.nodeTransformer,
    LanguageVersion.v2_dev -> v1.nodeTransformer,
  )

  private def getEncoderForVersion(
      version: LanguageVersion
  ): Result[PartialTransformer[lf.transaction.Node, iss.DamlTransaction.Node.VersionedNode]] =
    nodeTransformers
      .get(version)
      .toRight(s"Expected a transformer for version $version")
      .toResult

  /*
   * Value encoding. For LF Values and identifiers we encode using the LAPI proto.
   * The downside is we can't easily version this if LF values change significantly,
   * however that should be very rare as LF values need to be stable to guarantee compatibility.
   */
  private implicit val identifierTransformer
      : Transformer[lf.data.Ref.Identifier, lapiValue.Identifier] = (src: lf.data.Ref.Identifier) =>
    LfEngineToApi.toApiIdentifier(src)

  private implicit val valueTransformer: PartialTransformer[lf.value.Value, lapiValue.Value] =
    PartialTransformer { lfValue =>
      LfEngineToApi
        .lfValueToApiValue(verbose = true, value0 = lfValue)
        .toResult
    }

  /*
   * Generic collections
   */
  private implicit def immArrayToSeqTransformer[A, B](implicit
      aToB: Transformer[A, B]
  ): Transformer[ImmArray[A], Seq[B]] = _.map(_.transformInto[B]).toSeq

  private implicit def optImmArrayToSeqTransformer[A, B](implicit
      aToB: Transformer[A, B]
  ): Transformer[Option[ImmArray[A]], Seq[B]] =
    _.map(_.transformInto[Seq[B]]).getOrElse(Seq.empty)

  private implicit def optSetToSeq[A, B](implicit
      aToB: Transformer[A, B]
  ): Transformer[Option[Set[A]], Seq[B]] =
    _.toList.flatMap(_.map(_.transformInto[B]))

  /*
   * Straightforward encoders for simple LF classes
   */
  private implicit val contractIdTransformer: Transformer[lf.value.Value.ContractId, String] =
    _.toBytes.toHexString

  private implicit val hashTransformer: Transformer[lf.crypto.Hash, ByteString] =
    _.bytes.toByteString

  private implicit val partyVersionTransformer: Transformer[lf.data.Ref.Party, String] =
    Transformer.derive

  private implicit val languageVersionTransformer
      : Transformer[lf.language.LanguageVersion, String] =
    TransactionVersion.toProtoValue(_)

  private implicit val packageIdTransformer: Transformer[lf.data.Ref.PackageId, String] = identity

  private implicit val nodeIdTransformer: Transformer[lf.transaction.NodeId, String] =
    _.index.toString

  private implicit val bytesTransformer: Transformer[Bytes, ByteString] = _.toByteString

  private implicit val timestampTransformer: Transformer[lf.data.Time.Timestamp, Long] = _.micros

  private implicit val domainIdTransformer: Transformer[DomainId, String] = _.toProtoPrimitive

  private implicit val nodeIdHashTransformer: Transformer[
    (lf.transaction.NodeId, lf.crypto.Hash),
    iss.DamlTransaction.NodeSeed,
  ] = { case (nodeId, hash) =>
    iss.DamlTransaction.NodeSeed(nodeId.index, hash.transformInto[ByteString])
  }

  /*
   * Node Transformers
   * LF Nodes are versioned individually. A proto version serializes one or more several LF versions.
   * When a new LF version introduces changes that need either the serialization or the hashing to change,
   * we introduce a new Proto version.
   */

  /*
   * V1 Transformers
   */
  object v1 {
    private implicit val createNodeTransformer
        : PartialTransformer[lf.transaction.Node.Create, isdv1.Create] = Transformer
      .definePartial[lf.transaction.Node.Create, isdv1.Create]
      .withFieldRenamed(_.coid, _.contractId)
      .withFieldRenamed(_.arg, _.argument)
      .buildTransformer

    private implicit val exerciseTransformer
        : PartialTransformer[lf.transaction.Node.Exercise, isdv1.Exercise] = Transformer
      .definePartial[lf.transaction.Node.Exercise, isdv1.Exercise]
      .withFieldRenamed(_.targetCoid, _.contractId)
      .withFieldComputed(_.choiceObservers, _.choiceObservers.toSeq)
      .buildTransformer

    private implicit val fetchTransformer
        : PartialTransformer[lf.transaction.Node.Fetch, isdv1.Fetch] = Transformer
      .definePartial[lf.transaction.Node.Fetch, isdv1.Fetch]
      .withFieldRenamed(_.coid, _.contractId)
      .buildTransformer

    private[interactive] val nodeTransformer
        : PartialTransformer[lf.transaction.Node, iss.DamlTransaction.Node.VersionedNode] =
      PartialTransformer[lf.transaction.Node, iss.DamlTransaction.Node.VersionedNode] { lfNode =>
        val nodeType = lfNode match {
          case create: lf.transaction.Node.Create =>
            create
              .transformIntoPartial[isdv1.Create]
              .map(isdv1.Node.NodeType.Create.apply)
          case exercise: lf.transaction.Node.Exercise =>
            exercise
              .transformIntoPartial[isdv1.Exercise]
              .map(isdv1.Node.NodeType.Exercise.apply)
          case fetch: lf.transaction.Node.Fetch =>
            fetch
              .transformIntoPartial[isdv1.Fetch]
              .map(isdv1.Node.NodeType.Fetch.apply)
          case rollback: lf.transaction.Node.Rollback =>
            rollback
              .transformIntoPartial[isdv1.Rollback]
              .map(isdv1.Node.NodeType.Rollback.apply)
          case _: lf.transaction.Node.LookupByKey =>
            Result.fromErrorString("Lookup By Key nodes are not supporting in V1 Hashing Scheme")
        }

        nodeType
          .map(isdv1.Node(_))
          .map(iss.DamlTransaction.Node.VersionedNode.V1.apply)
      }
  }

  // Top level transformer of an lf node to a proto node
  // The version is automatically selected based on the LF -> Proto mapping defined at the beginning of this class
  private def nodeTransformer(
      nodeId: lf.transaction.NodeId
  ): PartialTransformer[lf.transaction.Node, iss.DamlTransaction.Node] = PartialTransformer {
    lfNode =>
      val transformerResult = lfNode match {
        // Rollback nodes are not versioned so lfNode.optVersion will be empty
        // Just pick the transformer for the default version as it doesn't matter here
        case _: Node.Rollback => getEncoderForVersion(LanguageVersion.default)
        case _ =>
          lfNode.optVersion
            .toRight("Expected a node version but was empty")
            .toResult
            .flatMap(getEncoderForVersion)
      }

      for {
        transformer <- transformerResult
        versionedNode <- transformer.transform(lfNode)
      } yield iss.DamlTransaction.Node(
        nodeId = nodeId.transformInto[String],
        lfVersion = lfNode.optVersion.map(_.transformInto[String]),
        versionedNode = versionedNode,
      )
  }

  // Transformer for a full transaction
  private def transactionTransformer(
      nodeSeeds: Option[ImmArray[(NodeId, crypto.Hash)]]
  ): PartialTransformer[lf.transaction.VersionedTransaction, iss.DamlTransaction] = Transformer
    .definePartial[lf.transaction.VersionedTransaction, iss.DamlTransaction]
    .withFieldComputed(_.roots, _.roots.map(_.transformInto[String]).toSeq)
    .withFieldComputedPartial(
      _.nodes,
      _.nodes.toList.traverse { case (nodeId, node) =>
        node.transformIntoPartial[iss.DamlTransaction.Node](nodeTransformer(nodeId))
      },
    )
    .withFieldConst(_.nodeSeeds, nodeSeeds.transformInto[Seq[iss.DamlTransaction.NodeSeed]])
    .buildTransformer

  // Transformer for global key mappings
  private implicit val commandExecutionResultGlobalKeyMappingTransformer
      : PartialTransformer[Map[GlobalKey, Option[Value.ContractId]], Seq[
        Metadata.GlobalKeyMappingEntry
      ]] = PartialTransformer {
    _.toList.traverse { case (key, maybeContractId) =>
      for {
        convertedKey <- key.transformIntoPartial[iscd.GlobalKey]
        convertedValue <- maybeContractId
          .map[lf.value.Value](lf.value.Value.ValueContractId.apply)
          .traverse(_.transformIntoPartial[lapiValue.Value])
      } yield iss.Metadata.GlobalKeyMappingEntry(
        key = Some(convertedKey),
        value = convertedValue,
      )
    }
  }

  private implicit val processedDisclosedContractTransformer
      : PartialTransformer[ProcessedDisclosedContract, Metadata.ProcessedDisclosedContract] =
    Transformer
      .definePartial[ProcessedDisclosedContract, Metadata.ProcessedDisclosedContract]
      .withFieldComputedPartial(
        _.contract,
        { contract =>
          val lfCreate = contract.create

          // Encode the disclosed contract with the matching version
          getEncoderForVersion(lfCreate.version)
            .flatMap(_.transform(lfCreate))
            .flatMap {
              // The encoding should have produced a create node, anything else is an error
              case VersionedNode.V1(isdv1.Node(create: isdv1.Node.NodeType.Create)) =>
                Result.fromValue(iss.Metadata.ProcessedDisclosedContract.Contract.V1(create.value))
              case _ =>
                Result.fromErrorString("Failed to encode disclosed contract to create contract")
            }
        },
      )
      .withFieldComputed(_.version, _.create.version.transformInto[String])
      .buildTransformer

  // Transformer for the transaction metadata
  private def resultToMetadataTransformer(
      domainId: DomainId,
      transactionUUID: UUID,
      mediatorGroup: Int,
  ): PartialTransformer[CommandExecutionResult, iss.Metadata] =
    Transformer
      .definePartial[CommandExecutionResult, iss.Metadata]
      .withFieldComputed(
        _.submissionSeed,
        _.transactionMeta.submissionSeed.transformInto[ByteString],
      )
      .withFieldComputed(_.submissionTime, _.transactionMeta.submissionTime.transformInto[Long])
      .withFieldComputed(
        _.usedPackages,
        _.transactionMeta.optUsedPackages.transformInto[Seq[String]],
      )
      .withFieldComputed(
        _.ledgerEffectiveTime,
        r =>
          Option.when(r.dependsOnLedgerTime)(
            r.transactionMeta.ledgerEffectiveTime.transformInto[Long]
          ),
      )
      .withFieldComputedPartial(
        _.disclosedEvents,
        _.processedDisclosedContracts.toList.traverse(
          _.transformIntoPartial[iss.Metadata.ProcessedDisclosedContract]
        ),
      )
      .withFieldComputed(_.byKeyNodes, _.transactionMeta.optByKeyNodes.transformInto[Seq[String]])
      .withFieldConst(_.domainId, domainId.transformInto[String])
      .withFieldConst(_.transactionUuid, transactionUUID.toString)
      .withFieldConst(_.mediatorGroup, mediatorGroup)
      .buildTransformer

  @VisibleForTesting
  private[interactive] def serializeTransaction(
      transaction: lf.transaction.VersionedTransaction,
      nodeSeeds: Option[ImmArray[(NodeId, crypto.Hash)]],
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ContextualizedErrorLogger,
  ): Future[DamlTransaction] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext
    implicit val implicitTransactionTransformer
        : PartialTransformer[lf.transaction.VersionedTransaction, iss.DamlTransaction] =
      transactionTransformer(nodeSeeds)
    for {
      // Enrich the transaction to get value fields in the resulting proto
      enrichedTransaction <- lfValueTranslation
        .enrichVersionedTransaction(transaction)
        .recover { case err =>
          // If we fail to enrich it just fallback to the un-enriched transaction
          logger.debug(s"Failed to enrich versioned transaction", err)
          transaction
        }
      // Convert the LF transaction to the interactive submission proto, this is where all the implicits above
      // kick in.
      protoTransaction <- enrichedTransaction
        .transformIntoPartial[iss.DamlTransaction]
        .toFutureWithLoggedFailures("Failed to serialize prepared transaction", logger)
    } yield protoTransaction
  }

  def serializeCommandExecutionResult(
      result: CommandExecutionResult,
      domainId: DomainId,
      transactionUUID: UUID,
      mediatorGroup: Int,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ContextualizedErrorLogger,
  ): Future[iss.PreparedTransaction] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext
    implicit val metadataTransformer: PartialTransformer[CommandExecutionResult, Metadata] =
      resultToMetadataTransformer(domainId, transactionUUID, mediatorGroup)
    val versionedTransaction = lf.transaction.VersionedTransaction(
      result.transaction.version,
      result.transaction.nodes,
      result.transaction.roots,
    )
    for {
      serializedTransaction <- serializeTransaction(
        versionedTransaction,
        result.transactionMeta.optNodeSeeds,
      )
      metadata <- result
        .transformIntoPartial[iss.Metadata]
        .toFutureWithLoggedFailures("Failed to serialize metadata", logger)
    } yield {
      iss.PreparedTransaction(
        transaction = Some(serializedTransaction),
        metadata = Some(metadata),
      )
    }
  }
}
