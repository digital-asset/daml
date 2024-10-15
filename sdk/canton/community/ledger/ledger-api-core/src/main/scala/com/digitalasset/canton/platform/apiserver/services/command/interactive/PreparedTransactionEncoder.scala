// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive

import cats.syntax.traverse.*
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.interactive_submission_data.{DamlTransaction, Metadata}
import com.daml.ledger.api.v2.{interactive_submission_data as isd, value as lapiValue}
import com.digitalasset.canton.data.ProcessedDisclosedContract
import com.digitalasset.canton.ledger.api.util.LfEngineToApi
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.execution.CommandExecutionResult
import com.digitalasset.canton.platform.apiserver.services.command.interactive.PreparedTransactionCodec.*
import com.digitalasset.canton.platform.store.dao.events.LfValueTranslation
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf
import com.digitalasset.daml.lf.data.{Bytes, ImmArray}
import com.digitalasset.daml.lf.transaction.{GlobalKey, TransactionVersion}
import com.digitalasset.daml.lf.value.Value
import com.google.protobuf.ByteString
import io.scalaland.chimney.dsl.*
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

  private implicit val identifierTransformer
      : Transformer[lf.data.Ref.Identifier, lapiValue.Identifier] = (src: lf.data.Ref.Identifier) =>
    LfEngineToApi.toApiIdentifier(src)

  // LF-Value to LAPI-Value
  private implicit val valueTransformer: PartialTransformer[lf.value.Value, lapiValue.Value] =
    (lfValue: lf.value.Value, _failFast: Boolean) => {
      LfEngineToApi
        .lfValueToApiValue(verbose = true, value0 = lfValue)
        .toResult
    }

  private implicit val contractIdTransformer: Transformer[lf.value.Value.ContractId, String] =
    _.toBytes.toHexString

  private implicit val hashTransformer: Transformer[lf.crypto.Hash, ByteString] =
    _.bytes.toByteString

  private implicit val packageVersionTransformer: Transformer[lf.data.Ref.PackageVersion, String] =
    _.toString()

  private implicit val partyVersionTransformer: Transformer[lf.data.Ref.Party, String] =
    Transformer.derive

  private implicit val transactionVersionTransformer
      : Transformer[lf.language.LanguageVersion, String] =
    TransactionVersion.toProtoValue(_)

  private implicit def immArrayToSeqTransformer[A, B](implicit
      aToB: Transformer[A, B]
  ): Transformer[ImmArray[A], Seq[B]] =
    _.map(_.transformInto[B]).toSeq

  private implicit def optImmArrayToSeqTransformer[A, B](implicit
      aToB: Transformer[A, B]
  ): Transformer[Option[ImmArray[A]], Seq[B]] =
    _.map(_.transformInto[Seq[B]]).getOrElse(Seq.empty)

  private implicit val packageIdTransformer: Transformer[lf.data.Ref.PackageId, String] = _.toString

  private implicit def optSetToSeq[A, B](implicit
      aToB: Transformer[A, B]
  ): Transformer[Option[Set[A]], Seq[B]] =
    _.toList.flatMap(_.map(_.transformInto[B]))

  private implicit val nodeIdTransformer: Transformer[lf.transaction.NodeId, String] =
    _.index.toString

  private implicit val bytesTransformer: Transformer[Bytes, ByteString] = _.toByteString

  private implicit val timestampTransformer: Transformer[lf.data.Time.Timestamp, Long] = _.micros

  private implicit val domainIdTransformer: Transformer[DomainId, String] = _.toProtoPrimitive

  private implicit val nodeIdHashTransformer: Transformer[
    (lf.transaction.NodeId, lf.crypto.Hash),
    isd.Metadata.NodeSeed,
  ] = { case (nodeId, hash) =>
    isd.Metadata.NodeSeed(nodeId.index, hash.transformInto[ByteString])
  }

  private implicit val globalKeyTransformer
      : PartialTransformer[lf.transaction.GlobalKeyWithMaintainers, isd.GlobalKeyWithMaintainers] =
    PartialTransformer
      .define[lf.transaction.GlobalKeyWithMaintainers, isd.GlobalKeyWithMaintainers]
      .withFieldRenamed(_.globalKey, _.key)
      .buildTransformer

  private implicit val createNodeTransformer
      : PartialTransformer[lf.transaction.Node.Create, isd.Create] = Transformer
    .definePartial[lf.transaction.Node.Create, isd.Create]
    .withFieldRenamed(_.coid, _.contractId)
    .withFieldRenamed(_.keyOpt, _.globalKeyWithMaintainers)
    .withFieldRenamed(_.arg, _.argument)
    .buildTransformer

  private implicit val exerciseFetchTransformer
      : PartialTransformer[lf.transaction.Node.Exercise, isd.Node.Fetch] = Transformer
    .definePartial[lf.transaction.Node.Exercise, isd.Node.Fetch]
    .withFieldRenamed(_.targetCoid, _.contractId)
    .withFieldRenamed(_.keyOpt, _.keyWithMaintainers)
    .buildTransformer

  private implicit val exerciseTransformer
      : PartialTransformer[lf.transaction.Node.Exercise, isd.Node.Exercise] = Transformer
    .definePartial[lf.transaction.Node.Exercise, isd.Node.Exercise]
    .withFieldComputedPartial(_.fetch, _.transformIntoPartial[isd.Node.Fetch].map(Some(_)))
    .withFieldComputed(_.choiceAuthorizers, _.choiceAuthorizers.transformInto[Seq[String]])
    .withFieldComputed(_.choiceObservers, _.choiceObservers.toSeq)
    .buildTransformer

  private implicit val fetchTransformer
      : PartialTransformer[lf.transaction.Node.Fetch, isd.Node.Fetch] = Transformer
    .definePartial[lf.transaction.Node.Fetch, isd.Node.Fetch]
    .withFieldRenamed(_.coid, _.contractId)
    .withFieldRenamed(_.keyOpt, _.keyWithMaintainers)
    .buildTransformer

  private implicit val lookupTransformer
      : PartialTransformer[lf.transaction.Node.LookupByKey, isd.Node.LookupByKey] = Transformer
    .definePartial[lf.transaction.Node.LookupByKey, isd.Node.LookupByKey]
    .withFieldRenamed(_.result, _.contractId)
    .buildTransformer

  private implicit val nodetoNodeTypeTransformer
      : PartialTransformer[lf.transaction.Node, isd.Node.NodeType] =
    PartialTransformer[lf.transaction.Node, isd.Node.NodeType] {
      case create: lf.transaction.Node.Create =>
        create
          .transformIntoPartial[isd.Create]
          .map(isd.Node.NodeType.Create.apply)
      case exercise: lf.transaction.Node.Exercise =>
        exercise
          .transformIntoPartial[isd.Node.Exercise]
          .map(isd.Node.NodeType.Exercise.apply)
      case fetch: lf.transaction.Node.Fetch =>
        fetch
          .transformIntoPartial[isd.Node.Fetch]
          .map(isd.Node.NodeType.Fetch.apply)
      case lookupByKey: lf.transaction.Node.LookupByKey =>
        lookupByKey
          .transformIntoPartial[isd.Node.LookupByKey]
          .map(isd.Node.NodeType.LookupByKey.apply)
      case rollback: lf.transaction.Node.Rollback =>
        rollback
          .transformIntoPartial[isd.Node.Rollback]
          .map(isd.Node.NodeType.Rollback.apply)
    }

  private def nodeTransformer(
      nodeId: lf.transaction.NodeId
  ): PartialTransformer[lf.transaction.Node, isd.Node] = PartialTransformer
    .define[lf.transaction.Node, isd.Node]
    .withFieldComputedPartial(_.nodeType, _.transformIntoPartial[isd.Node.NodeType])
    .withFieldRenamed(_.optVersion, _.version)
    .withFieldComputed(_.nodeId, _ => nodeId.transformInto[String])
    .buildTransformer

  private implicit val transactionTransformer
      : PartialTransformer[lf.transaction.VersionedTransaction, isd.DamlTransaction] = Transformer
    .definePartial[lf.transaction.VersionedTransaction, isd.DamlTransaction]
    .withFieldComputed(_.version, x => TransactionVersion.toProtoValue(x.version))
    .withFieldComputed(_.roots, _.roots.map(_.transformInto[String]).toSeq)
    .withFieldComputedPartial(
      _.nodes,
      _.nodes.toList.traverse { case (nodeId, node) =>
        node.transformIntoPartial[isd.Node](nodeTransformer(nodeId))
      },
    )
    .buildTransformer

  private implicit val commandExecutionResultGlobalKeyMappingTransformer
      : PartialTransformer[Map[GlobalKey, Option[Value.ContractId]], Seq[
        Metadata.GlobalKeyMappingEntry
      ]] =
    PartialTransformer[Map[GlobalKey, Option[Value.ContractId]], Seq[
      isd.Metadata.GlobalKeyMappingEntry
    ]] {
      _.toList.traverse { case (key, maybeContractId) =>
        for {
          convertedKey <- key.transformIntoPartial[isd.GlobalKey]
          convertedValue <- maybeContractId
            .map[lf.value.Value](lf.value.Value.ValueContractId.apply)
            .traverse(_.transformIntoPartial[lapiValue.Value])
        } yield isd.Metadata.GlobalKeyMappingEntry(
          key = Some(convertedKey),
          value = convertedValue,
        )
      }
    }

  private implicit val processedDisclosedContractTransformer
      : PartialTransformer[ProcessedDisclosedContract, Metadata.ProcessedDisclosedContract] =
    Transformer
      .definePartial[ProcessedDisclosedContract, Metadata.ProcessedDisclosedContract]
      .withFieldRenamed(_.create, _.contract)
      .buildTransformer

  private implicit val resultToMetadataTransformer
      : PartialTransformer[CommandExecutionResult, isd.Metadata] =
    Transformer
      .definePartial[CommandExecutionResult, isd.Metadata]
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
        _.nodeSeeds,
        _.transactionMeta.optNodeSeeds.transformInto[Seq[isd.Metadata.NodeSeed]],
      )
      .withFieldComputed(_.workflowId, _.transactionMeta.workflowId)
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
          _.transformIntoPartial[isd.Metadata.ProcessedDisclosedContract]
        ),
      )
      // TODO(i20688) The 3 fields below should be picked by running through domain router
      .withFieldRenamed(_.optDomainId, _.domainId)
      .withFieldConst(_.transactionId, UUID.randomUUID().toString)
      .withFieldConst(_.mediatorGroup, 0)
      .buildTransformer

  private def serializeTransaction(
      transaction: lf.transaction.VersionedTransaction
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ContextualizedErrorLogger,
  ): Future[DamlTransaction] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext
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
        .transformIntoPartial[isd.DamlTransaction]
        .toFutureWithLoggedFailures("prepared transaction", logger)
    } yield protoTransaction
  }

  def serializeCommandExecutionResult(
      result: CommandExecutionResult
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ContextualizedErrorLogger,
  ): Future[isd.PreparedTransaction] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext
    val versionedTransaction = lf.transaction.VersionedTransaction(
      result.transaction.version,
      result.transaction.nodes,
      result.transaction.roots,
    )
    for {
      serializedTransaction <- serializeTransaction(versionedTransaction)
      metadata <- result
        .transformIntoPartial[isd.Metadata]
        .toFutureWithLoggedFailures("metadata", logger)
    } yield {
      isd.PreparedTransaction(
        transaction = Some(serializedTransaction),
        metadata = Some(metadata),
      )
    }
  }
}
