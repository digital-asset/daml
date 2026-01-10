// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.ProtoDeserializationError.{
  FieldNotSet,
  ProtoDeserializationFailure,
  ValueConversionError,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId as AdminTopologyStoreId
import com.digitalasset.canton.topology.admin.v30
import com.digitalasset.canton.topology.admin.v30.*
import com.digitalasset.canton.topology.admin.v30.AuthorizeRequest.{Proposal, Type}
import com.digitalasset.canton.topology.store.TopologyStoreId.TemporaryStore
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, GrpcStreamingUtils}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.digitalasset.canton.{ProtoDeserializationError, config}
import com.google.protobuf.ByteString
import com.google.protobuf.duration.Duration
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future}

/** @param managers
  *   A sequence of topology managers. The type [[com.digitalasset.canton.crypto.BaseCrypto]] is
  *   used to accommodate various implementations of
  *   [[com.digitalasset.canton.topology.TopologyManager]], including
  *   [[com.digitalasset.canton.topology.LocalTopologyManager]] and
  *   [[com.digitalasset.canton.topology.SynchronizerTopologyManager]], which depend on different
  *   crypto types.
  */
class GrpcTopologyManagerWriteService(
    managers: => Seq[TopologyManager[TopologyStoreId, BaseCrypto]],
    physicalSynchronizerIdLookup: PSIdLookup,
    temporaryStoreRegistry: TemporaryStoreRegistry,
    nodeParameters: CantonNodeParameters,
    override val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends v30.TopologyManagerWriteServiceGrpc.TopologyManagerWriteService
    with NamedLogging {

  override def authorize(requests: v30.AuthorizeRequest): Future[v30.AuthorizeResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.AuthorizeRequest(
      requestType,
      mustFullyAuthorize,
      forceChanges,
      signedBy,
      store,
      waitToBecomeEffective,
    ) = requests

    def authorizeFromHash(txHash: TxHash) = for {
      manager <- targetManagerET(store)
      signingKeys <-
        EitherT
          .fromEither[FutureUnlessShutdown](
            signedBy.traverse(Fingerprint.fromProtoPrimitive)
          )
          .leftMap(ProtoDeserializationFailure.Wrap(_))
      forceFlags <- EitherT
        .fromEither[FutureUnlessShutdown](
          ForceFlags
            .fromProtoV30(forceChanges)
            .leftMap(ProtoDeserializationFailure.Wrap(_): RpcError)
        )
      signedTopoTx <-
        manager
          .accept(
            txHash,
            signingKeys,
            forceChanges = forceFlags,
            expectFullAuthorization = mustFullyAuthorize,
          )
          .leftWiden[RpcError]
    } yield signedTopoTx

    val result = requestType match {
      case Type.Empty =>
        EitherT.leftT[FutureUnlessShutdown, GenericSignedTopologyTransaction][RpcError](
          ProtoDeserializationFailure.Wrap(FieldNotSet("AuthorizeRequest.type"))
        )

      case Type.TransactionHash(value) =>
        for {
          txHash <- EitherT
            .fromEither[FutureUnlessShutdown](Hash.fromHexString(value).map(TxHash.apply))
            .leftMap(err => ProtoDeserializationFailure.Wrap(err.toProtoDeserializationError))
          signedTopoTx <- authorizeFromHash(txHash)
        } yield signedTopoTx

      case Type.Proposal(Proposal(op, serial, mapping)) =>
        val validatedMappingE = for {
          // we treat serial=0 as "serial was not set". negative values should be rejected by parsePositiveInt
          serial <- Option
            .when(serial != 0)(serial)
            .traverse(ProtoConverter.parsePositiveInt("serial", _))
          op <- ProtoConverter.parseEnum(TopologyChangeOp.fromProtoV30, "operation", op)
          mapping <- ProtoConverter.required("AuthorizeRequest.mapping", mapping)
          signingKeys <- signedBy.traverse(Fingerprint.fromProtoPrimitive)
          forceFlags <- ForceFlags.fromProtoV30(forceChanges)
          validatedMapping <- TopologyMapping.fromProtoV30(mapping)
        } yield {
          (op, serial, validatedMapping, signingKeys, forceFlags)
        }

        for {
          mapping <- EitherT
            .fromEither[FutureUnlessShutdown](validatedMappingE)
            .leftMap(ProtoDeserializationFailure.Wrap(_))
          waitToBecomeEffectiveO <- EitherT
            .fromEither[FutureUnlessShutdown](
              waitToBecomeEffective
                .traverse(
                  config.NonNegativeFiniteDuration.fromProtoPrimitive("wait_to_become_effective")
                )
                .leftMap(ProtoDeserializationFailure.Wrap(_))
            )
          (op, serial, validatedMapping, signingKeys, forceChanges) = mapping

          // TODO(#28972) Remove this check once LSU is stable
          _ <- EitherT.fromEither[FutureUnlessShutdown](ensureLSUPreviewEnabled(validatedMapping))

          manager <- targetManagerET(store)
          signedTopoTx <- manager
            .proposeAndAuthorize(
              op,
              validatedMapping,
              serial,
              signingKeys,
              manager.managerVersion.serialization,
              expectFullAuthorization = mustFullyAuthorize,
              forceChanges = forceChanges,
              waitToBecomeEffective = waitToBecomeEffectiveO,
            )
            .leftWiden[RpcError]
        } yield signedTopoTx
    }
    CantonGrpcUtil.mapErrNewEUS(result.map(tx => v30.AuthorizeResponse(Some(tx.toProtoV30))))
  }

  // LSU announcement require preview features enabled
  private def ensureLSUPreviewEnabled(
      mapping: TopologyMapping
  )(implicit traceContext: TraceContext): Either[RpcError, Unit] = mapping
    .select[SynchronizerUpgradeAnnouncement]
    .fold(().asRight[RpcError])(_ =>
      Either
        .cond(
          nodeParameters.enablePreviewFeatures,
          (),
          TopologyManagerError.PreviewFeature.Error(operation = "synchronizer upgrade announcement"),
        )
        .leftWiden[RpcError]
    )

  override def signTransactions(
      requestP: v30.SignTransactionsRequest
  ): Future[v30.SignTransactionsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val requestE = for {
      signedTxs <-
        requestP.transactions
          .traverse(tx =>
            SignedTopologyTransaction.fromProtoV30(ProtocolVersionValidation.NoValidation, tx)
          )
      signingKeys <-
        requestP.signedBy.traverse(Fingerprint.fromProtoPrimitive)
      forceFlags <- ForceFlags.fromProtoV30(requestP.forceFlags)
    } yield (signedTxs, signingKeys, forceFlags)

    val res = for {
      request <- EitherT
        .fromEither[FutureUnlessShutdown](requestE)
        .leftMap(ProtoDeserializationFailure.Wrap(_))
      (signedTxs, signingKeys, forceFlags) = request

      targetManager <- targetManagerET(requestP.store)

      extendedTransactions <- signedTxs.parTraverse(tx =>
        targetManager
          .extendSignature(tx, signingKeys, forceFlags)
          .leftWiden[RpcError]
      )
    } yield extendedTransactions

    CantonGrpcUtil.mapErrNewEUS(res.map(txs => v30.SignTransactionsResponse(txs.map(_.toProtoV30))))
  }

  override def addTransactions(
      request: v30.AddTransactionsRequest
  ): Future[v30.AddTransactionsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val res = for {
      manager <- targetManagerET(request.store)
      protocolVersionValidation = manager.managerVersion.validation
      forceChanges <- EitherT.fromEither[FutureUnlessShutdown](
        ForceFlags
          .fromProtoV30(request.forceChanges)
          .leftMap(ProtoDeserializationFailure.Wrap(_): RpcError)
      )
      signedTxs <- EitherT.fromEither[FutureUnlessShutdown](
        request.transactions
          .traverse(tx => SignedTopologyTransaction.fromProtoV30(protocolVersionValidation, tx))
          .leftMap(ProtoDeserializationFailure.Wrap(_): RpcError)
      )
      waitToBecomeEffectiveO <- EitherT
        .fromEither[FutureUnlessShutdown](
          request.waitToBecomeEffective
            .traverse(
              config.NonNegativeFiniteDuration.fromProtoPrimitive("wait_to_become_effective")
            )
            .leftMap(ProtoDeserializationFailure.Wrap(_))
        )
      _ <- addTransactions(signedTxs, request.store, forceChanges, waitToBecomeEffectiveO)
    } yield v30.AddTransactionsResponse()
    CantonGrpcUtil.mapErrNewEUS(res)
  }

  override def importTopologySnapshot(
      responseObserver: StreamObserver[ImportTopologySnapshotResponse]
  ): StreamObserver[ImportTopologySnapshotRequest] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    GrpcStreamingUtils
      .streamFromClient[
        ImportTopologySnapshotRequest,
        ImportTopologySnapshotResponse,
        (Option[v30.StoreId], Option[Duration]),
      ](
        _.topologySnapshot,
        req => (req.store, req.waitToBecomeEffective),
        { case (topologySnapshot, (waitToBecomeEffective, store)) =>
          doImportTopologySnapshot(topologySnapshot, store, waitToBecomeEffective)
        },
        responseObserver,
      )
  }

  private def doImportTopologySnapshot(
      topologySnapshot: ByteString,
      waitToBecomeEffectiveP: Option[Duration],
      store: Option[v30.StoreId],
  )(implicit traceContext: TraceContext): Future[ImportTopologySnapshotResponse] = {
    val res: EitherT[FutureUnlessShutdown, RpcError, ImportTopologySnapshotResponse] = for {
      storedTxs <- EitherT.fromEither[FutureUnlessShutdown](
        StoredTopologyTransactions
          .fromTrustedByteString(topologySnapshot)
          .leftMap(ProtoDeserializationFailure.Wrap(_): RpcError)
      )
      signedTxs = storedTxs.result.map(_.transaction)
      waitToBecomeEffectiveO <- EitherT
        .fromEither[FutureUnlessShutdown](
          waitToBecomeEffectiveP
            .traverse(
              config.NonNegativeFiniteDuration.fromProtoPrimitive("wait_to_become_effective")
            )
            .leftMap(ProtoDeserializationFailure.Wrap(_))
        )
      _ <- addTransactions(signedTxs, store, ForceFlags.all, waitToBecomeEffectiveO)
    } yield v30.ImportTopologySnapshotResponse()
    CantonGrpcUtil.mapErrNewEUS(res)
  }

  override def importTopologySnapshotV2(
      responseObserver: StreamObserver[ImportTopologySnapshotV2Response]
  ): StreamObserver[ImportTopologySnapshotV2Request] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    GrpcStreamingUtils
      .streamFromClient[
        ImportTopologySnapshotV2Request,
        ImportTopologySnapshotV2Response,
        (Option[v30.StoreId], Option[Duration]),
      ](
        _.topologySnapshot,
        req => (req.store, req.waitToBecomeEffective),
        { case (topologySnapshot, (waitToBecomeEffective, store)) =>
          doImportTopologySnapshotV2(topologySnapshot, store, waitToBecomeEffective)
        },
        responseObserver,
      )
  }

  private def doImportTopologySnapshotV2(
      topologySnapshot: ByteString,
      waitToBecomeEffectiveP: Option[Duration],
      store: Option[v30.StoreId],
  )(implicit traceContext: TraceContext): Future[ImportTopologySnapshotV2Response] = {
    val res: EitherT[FutureUnlessShutdown, RpcError, ImportTopologySnapshotV2Response] = for {
      storedTxs <- EitherT.fromEither[FutureUnlessShutdown](
        GrpcStreamingUtils
          .parseDelimitedFromTrusted(topologySnapshot.newInput(), StoredTopologyTransaction)
          .leftMap(err =>
            ProtoDeserializationFailure.Wrap(
              ValueConversionError("topology_snapshot", err)
            ): RpcError
          )
      )
      signedTxs = storedTxs.map(_.transaction)
      waitToBecomeEffectiveO <- EitherT
        .fromEither[FutureUnlessShutdown](
          waitToBecomeEffectiveP
            .traverse(
              config.NonNegativeFiniteDuration.fromProtoPrimitive("wait_to_become_effective")
            )
            .leftMap(ProtoDeserializationFailure.Wrap(_))
        )
      _ <- addTransactions(signedTxs, store, ForceFlags.all, waitToBecomeEffectiveO)
    } yield v30.ImportTopologySnapshotV2Response()
    CantonGrpcUtil.mapErrNewEUS(res)
  }

  private def addTransactions(
      signedTxs: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
      store: Option[v30.StoreId],
      forceChanges: ForceFlags,
      waitToBecomeEffective: Option[config.NonNegativeFiniteDuration],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Unit] =
    for {
      manager <- targetManagerET(store)
      asyncResult <- manager
        .add(
          signedTxs,
          forceChanges = forceChanges,
          expectFullAuthorization = false,
        )
        .leftWiden[RpcError]
      _ = waitToBecomeEffective match {
        case Some(timeout) =>
          EitherT.rightT[FutureUnlessShutdown, TopologyManagerError](
            timeout.awaitUS(s"addTransactions-wait-for-effective")(asyncResult.unwrap)
          )
        case None => EitherTUtil.unitUS[TopologyManagerError]
      }
    } yield ()

  /** @return
    *   the appropriate [[com.digitalasset.canton.topology.TopologyManager]] corresponding to the
    *   provided store ID. The manager uses [[com.digitalasset.canton.crypto.BaseCrypto]] to
    *   abstract over different types of topology managers that rely on distinct crypto types, such
    *   as [[com.digitalasset.canton.topology.LocalTopologyManager]] or
    *   [[com.digitalasset.canton.topology.SynchronizerTopologyManager]].
    */
  private def targetManagerET(
      store: Option[v30.StoreId]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, TopologyManager[TopologyStoreId, BaseCrypto]] =
    for {
      targetStore <- EitherT.fromEither[FutureUnlessShutdown](
        ProtoConverter
          .parseRequired(
            AdminTopologyStoreId.fromProtoV30(_, "store"),
            "store",
            store,
          )
          .leftMap(ProtoDeserializationFailure.Wrap(_))
      )
      targetStoreInternal <- EitherT
        .fromEither[FutureUnlessShutdown](targetStore.toInternal(physicalSynchronizerIdLookup))
        .leftMap(TopologyManagerError.InvalidSynchronizer.Failure(_))
      manager <- EitherT
        .fromOption[FutureUnlessShutdown](
          managers.find(_.store.storeId == targetStoreInternal),
          TopologyManagerError.TopologyStoreUnknown.Failure(targetStoreInternal),
        )
        .leftWiden[RpcError]
    } yield manager

  /** RPC to generate topology transactions that can be signed
    */
  override def generateTransactions(
      request: GenerateTransactionsRequest
  ): Future[GenerateTransactionsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val resultET = request.proposals.parTraverse { proposal =>
      val v30.GenerateTransactionsRequest.Proposal(opP, serialP, mappingPO, store) = proposal
      val validatedMappingE = for {
        serial <- Option
          .when(serialP != 0)(serialP)
          .traverse(ProtoConverter.parsePositiveInt("serial", _))
        op <- ProtoConverter.parseEnum(TopologyChangeOp.fromProtoV30, "operation", opP)
        mappingP <- ProtoConverter.required("mapping", mappingPO)
        mapping <- TopologyMapping.fromProtoV30(mappingP)
      } yield (serial, op, mapping)

      for {
        serialOpMapping <- EitherT
          .fromEither[FutureUnlessShutdown](validatedMappingE)
          .leftMap(ProtoDeserializationFailure.Wrap(_))
        (serial, op, mapping) = serialOpMapping
        manager <- targetManagerET(store)
        existingTransaction <- manager
          .findExistingTransaction(mapping)
        transaction <- manager
          .build(
            op,
            mapping,
            serial,
            manager.managerVersion.serialization,
            existingTransaction,
          )
          .leftWiden[RpcError]
      } yield transaction.toByteString -> transaction.hash.hash.getCryptographicEvidence
    }

    CantonGrpcUtil.mapErrNewEUS(
      resultET.map { txAndHashes =>
        GenerateTransactionsResponse(
          txAndHashes.map { case (serializedTransaction, hash) =>
            GenerateTransactionsResponse.GeneratedTransaction(serializedTransaction, hash)
          }
        )
      }
    )
  }

  override def createTemporaryTopologyStore(
      request: CreateTemporaryTopologyStoreRequest
  ): Future[CreateTemporaryTopologyStoreResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      protocolVersion <- ProtocolVersion
        .fromProtoPrimitive(request.protocolVersion)
        .leftMap(ProtoDeserializationFailure.Wrap(_))
      storeId <- TemporaryStore
        .create(request.name)
        .leftMap(err =>
          ProtoDeserializationFailure.Wrap(ProtoDeserializationError.StringConversionError(err))
        )
      _ <- temporaryStoreRegistry.createStore(storeId, protocolVersion)
    } yield {
      CreateTemporaryTopologyStoreResponse(
        Some(AdminTopologyStoreId.Temporary(storeId.name).toProtoV30.getTemporary)
      )
    }
    CantonGrpcUtil.mapErrNew(EitherT.fromEither[Future](res))
  }

  override def dropTemporaryTopologyStore(
      request: DropTemporaryTopologyStoreRequest
  ): Future[DropTemporaryTopologyStoreResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      grpcStore <- ProtoConverter
        .parseRequired(
          AdminTopologyStoreId.fromProtoV30(_, "store_id"),
          "store_id",
          request.storeId.map(temp => StoreId(StoreId.Store.Temporary(temp))),
        )
        .leftMap(ProtoDeserializationFailure.Wrap(_))
      tempStoreId = grpcStore match {
        case AdminTopologyStoreId.Temporary(name) => TopologyStoreId.TemporaryStore(name)
        case AdminTopologyStoreId.Synchronizer(_) | AdminTopologyStoreId.Authorized =>
          ErrorUtil.invalidArgument("Cannot drop authorized store or synchronizer stores")
      }
      _ <- temporaryStoreRegistry.dropStore(tempStoreId)
    } yield DropTemporaryTopologyStoreResponse.defaultInstance

    CantonGrpcUtil.mapErrNew(EitherT.fromEither[Future](res))
  }
}
