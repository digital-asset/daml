// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, ProtoDeserializationFailure}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.protocol.v30.TopologyMapping.Mapping
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.v30
import com.digitalasset.canton.topology.admin.v30.AuthorizeRequest.{Proposal, Type}
import com.digitalasset.canton.topology.admin.v30.{
  GenerateTransactionsRequest,
  GenerateTransactionsResponse,
  ImportTopologySnapshotRequest,
}
import com.digitalasset.canton.topology.store.{StoredTopologyTransactions, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.version.ProtocolVersionValidation

import scala.concurrent.{ExecutionContext, Future}

class GrpcTopologyManagerWriteService[PureCrypto <: CryptoPureApi](
    managers: => Seq[TopologyManager[TopologyStoreId, PureCrypto]],
    crypto: Crypto,
    override val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends v30.TopologyManagerWriteServiceGrpc.TopologyManagerWriteService
    with NamedLogging {

  override def authorize(request: v30.AuthorizeRequest): Future[v30.AuthorizeResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val result = request.`type` match {
      case Type.Empty =>
        EitherT.leftT[FutureUnlessShutdown, GenericSignedTopologyTransaction][CantonError](
          ProtoDeserializationFailure.Wrap(FieldNotSet("AuthorizeRequest.type"))
        )

      case Type.TransactionHash(value) =>
        for {
          txHash <- EitherT
            .fromEither[FutureUnlessShutdown](Hash.fromHexString(value).map(TxHash.apply))
            .leftMap(err => ProtoDeserializationFailure.Wrap(err.toProtoDeserializationError))
          manager <- targetManagerET(request.store)
          signingKeys <-
            EitherT
              .fromEither[FutureUnlessShutdown](
                request.signedBy.traverse(Fingerprint.fromProtoPrimitive)
              )
              .leftMap(ProtoDeserializationFailure.Wrap(_))
          forceFlags <- EitherT
            .fromEither[FutureUnlessShutdown](
              ForceFlags
                .fromProtoV30(request.forceChanges)
                .leftMap(ProtoDeserializationFailure.Wrap(_): CantonError)
            )
          signedTopoTx <-
            manager
              .accept(
                txHash,
                signingKeys,
                forceChanges = forceFlags,
                expectFullAuthorization = request.mustFullyAuthorize,
              )
              .leftWiden[CantonError]
        } yield {
          signedTopoTx
        }

      case Type.Proposal(Proposal(op, serial, mapping)) =>
        val validatedMappingE = for {
          // we treat serial=0 as "serial was not set". negative values should be rejected by parsePositiveInt
          serial <- Option
            .when(serial != 0)(serial)
            .traverse(ProtoConverter.parsePositiveInt("serial", _))
          op <- ProtoConverter.parseEnum(TopologyChangeOp.fromProtoV30, "operation", op)
          mapping <- ProtoConverter.required("AuthorizeRequest.mapping", mapping)
          signingKeys <-
            request.signedBy.traverse(Fingerprint.fromProtoPrimitive)
          forceFlags <- ForceFlags
            .fromProtoV30(request.forceChanges)
          validatedMapping <- deserializeTopologyMapping(mapping.mapping)
        } yield {
          (op, serial, validatedMapping, signingKeys, forceFlags)
        }
        for {
          mapping <- EitherT
            .fromEither[FutureUnlessShutdown](validatedMappingE)
            .leftMap(ProtoDeserializationFailure.Wrap(_))
          (op, serial, validatedMapping, signingKeys, forceChanges) = mapping
          manager <- targetManagerET(request.store)

          signedTopoTx <- manager
            .proposeAndAuthorize(
              op,
              validatedMapping,
              serial,
              signingKeys,
              manager.managerVersion.serialization,
              expectFullAuthorization = request.mustFullyAuthorize,
              forceChanges = forceChanges,
            )
            .leftWiden[CantonError]
        } yield {
          signedTopoTx
        }
    }
    CantonGrpcUtil.mapErrNewEUS(result.map(tx => v30.AuthorizeResponse(Some(tx.toProtoV30))))
  }

  private def deserializeTopologyMapping(mapping: Mapping) =
    mapping match {
      case Mapping.Empty => FieldNotSet("mapping").asLeft
      case Mapping.DecentralizedNamespaceDefinition(mapping) =>
        DecentralizedNamespaceDefinition.fromProtoV30(mapping)
      case Mapping.NamespaceDelegation(mapping) =>
        NamespaceDelegation.fromProtoV30(mapping)
      case Mapping.IdentifierDelegation(mapping) =>
        IdentifierDelegation.fromProtoV30(mapping)
      case Mapping.DomainParametersState(mapping) =>
        DomainParametersState.fromProtoV30(mapping)
      case Mapping.SequencingDynamicParametersState(mapping) =>
        DynamicSequencingParametersState.fromProtoV30(mapping)
      case Mapping.MediatorDomainState(mapping) =>
        MediatorDomainState.fromProtoV30(mapping)
      case Mapping.SequencerDomainState(mapping) =>
        SequencerDomainState.fromProtoV30(mapping)
      case Mapping.PartyToParticipant(mapping) =>
        PartyToParticipant.fromProtoV30(mapping)
      case Mapping.DomainTrustCertificate(mapping) =>
        DomainTrustCertificate.fromProtoV30(mapping)
      case Mapping.OwnerToKeyMapping(mapping) =>
        OwnerToKeyMapping.fromProtoV30(mapping)
      case Mapping.VettedPackages(mapping) =>
        VettedPackages.fromProtoV30(mapping)
      case Mapping.ParticipantPermission(mapping) =>
        ParticipantDomainPermission.fromProtoV30(mapping)
      case Mapping.PartyHostingLimits(mapping) =>
        PartyHostingLimits.fromProtoV30(mapping)
      case Mapping.PurgeTopologyTxs(mapping) =>
        PurgeTopologyTransaction.fromProtoV30(mapping)
      case Mapping.PartyToKeyMapping(mapping) =>
        PartyToKeyMapping.fromProtoV30(mapping)
    }

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
        targetManager.extendSignature(tx, signingKeys, forceFlags).leftWiden[CantonError]
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
          .leftMap(ProtoDeserializationFailure.Wrap(_): CantonError)
      )
      signedTxs <- EitherT.fromEither[FutureUnlessShutdown](
        request.transactions
          .traverse(tx => SignedTopologyTransaction.fromProtoV30(protocolVersionValidation, tx))
          .leftMap(ProtoDeserializationFailure.Wrap(_): CantonError)
      )
      _ <- addTransactions(signedTxs, request.store, forceChanges)
    } yield v30.AddTransactionsResponse()
    CantonGrpcUtil.mapErrNewEUS(res)
  }

  override def importTopologySnapshot(
      request: ImportTopologySnapshotRequest
  ): Future[v30.ImportTopologySnapshotResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      storedTxs <- EitherT.fromEither[FutureUnlessShutdown](
        StoredTopologyTransactions
          .fromTrustedByteString(request.topologySnapshot)
          .leftMap(ProtoDeserializationFailure.Wrap(_): CantonError)
      )
      signedTxs = storedTxs.result.map(_.transaction)
      _ <- addTransactions(signedTxs, request.store, ForceFlags.all)
    } yield v30.ImportTopologySnapshotResponse()
    CantonGrpcUtil.mapErrNewEUS(res)
  }

  private def addTransactions(
      signedTxs: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
      store: String,
      forceChanges: ForceFlags,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, Unit] =
    for {
      manager <- targetManagerET(store)
      // TODO(#12390) let the caller decide whether to expect full authorization or not?
      _ <- manager
        .add(
          signedTxs,
          forceChanges = forceChanges,
          expectFullAuthorization = false,
        )
        .leftWiden[CantonError]
    } yield ()

  private def targetManagerET(
      store: String
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, TopologyManager[TopologyStoreId, PureCrypto]] =
    for {
      targetStore <- EitherT.cond[FutureUnlessShutdown](
        store.nonEmpty,
        store,
        ProtoDeserializationFailure.Wrap(FieldNotSet("store")),
      )
      manager <- EitherT
        .fromOption[FutureUnlessShutdown](
          managers.find(_.store.storeId.filterName == targetStore),
          TopologyManagerError.InternalError
            .Other(s"Target store $targetStore unknown: available stores: $managers"),
        )
        .leftWiden[CantonError]
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
        mapping <- deserializeTopologyMapping(mappingP.mapping)
      } yield (serial, op, mapping)

      for {
        serialOpMapping <- EitherT
          .fromEither[FutureUnlessShutdown](validatedMappingE)
          .leftMap(ProtoDeserializationFailure.Wrap(_))
        (serial, op, mapping) = serialOpMapping
        manager <- targetManagerET(store)
        existingTransaction <- manager
          .findExistingTransaction(mapping)
          .mapK(
            FutureUnlessShutdown.outcomeK
          )
        transaction <- manager
          .build(
            op,
            mapping,
            serial,
            manager.managerVersion.serialization,
            existingTransaction,
          )
          .mapK(FutureUnlessShutdown.outcomeK)
          .leftWiden[CantonError]
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
}
