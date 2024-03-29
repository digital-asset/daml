// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, ProtoDeserializationFailure}
import com.digitalasset.canton.crypto.{Crypto, Fingerprint, Hash}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.protocol.v30.TopologyMapping.Mapping
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.v30
import com.digitalasset.canton.topology.admin.v30.AuthorizeRequest.{Proposal, Type}
import com.digitalasset.canton.topology.admin.v30.ImportTopologySnapshotRequest
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactionsX,
  TopologyStoreId,
  TopologyStoreX,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.TxHash
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}

import scala.concurrent.{ExecutionContext, Future}

class GrpcTopologyManagerWriteService(
    managers: => Seq[TopologyManagerX[TopologyStoreId]],
    topologyStoreX: TopologyStoreX[AuthorizedStore],
    getId: => Option[UniqueIdentifier],
    crypto: Crypto,
    protocolVersion: ProtocolVersion,
    clock: Clock,
    override val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends v30.TopologyManagerWriteServiceGrpc.TopologyManagerWriteService
    with NamedLogging {

  override def authorize(request: v30.AuthorizeRequest): Future[v30.AuthorizeResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val result = request.`type` match {
      case Type.Empty =>
        EitherT.leftT[FutureUnlessShutdown, GenericSignedTopologyTransactionX][CantonError](
          ProtoDeserializationFailure.Wrap(FieldNotSet("AuthorizeRequest.type"))
        )

      case Type.TransactionHash(value) =>
        for {
          txHash <- EitherT
            .fromEither[FutureUnlessShutdown](Hash.fromHexString(value).map(TxHash))
            .leftMap(err => ProtoDeserializationFailure.Wrap(err.toProtoDeserializationError))
          manager <- targetManagerET(request.store)
          signingKeys <-
            EitherT
              .fromEither[FutureUnlessShutdown](
                request.signedBy.traverse(Fingerprint.fromProtoPrimitive)
              )
              .leftMap(ProtoDeserializationFailure.Wrap(_))
          signedTopoTx <-
            // TODO(#14067) understand when and why force needs to come in effect
            manager
              .accept(
                txHash,
                signingKeys,
                force = false,
                expectFullAuthorization = request.mustFullyAuthorize,
              )
              .leftWiden[CantonError]
        } yield {
          signedTopoTx
        }

      case Type.Proposal(Proposal(op, serial, mapping)) =>
        val validatedMappingE = for {
          // we treat serial=0 as "serial was not set". negative values should be rejected by parsePositiveInt
          serial <- Option.when(serial != 0)(serial).traverse(ProtoConverter.parsePositiveInt)
          op <- TopologyChangeOpX.fromProtoV30(op)
          mapping <- ProtoConverter.required("AuthorizeRequest.mapping", mapping)
          signingKeys <-
            request.signedBy.traverse(Fingerprint.fromProtoPrimitive)
          validatedMapping <- mapping.mapping match {
            case Mapping.DecentralizedNamespaceDefinition(mapping) =>
              DecentralizedNamespaceDefinitionX.fromProtoV30(mapping)
            case Mapping.NamespaceDelegation(mapping) =>
              NamespaceDelegationX.fromProtoV30(mapping)
            case Mapping.IdentifierDelegation(mapping) =>
              IdentifierDelegationX.fromProtoV30(mapping)
            case Mapping.DomainParametersState(mapping) =>
              DomainParametersStateX.fromProtoV30(mapping)
            case Mapping.MediatorDomainState(mapping) =>
              MediatorDomainStateX.fromProtoV30(mapping)
            case Mapping.SequencerDomainState(mapping) =>
              SequencerDomainStateX.fromProtoV30(mapping)
            case Mapping.PartyToParticipant(mapping) =>
              PartyToParticipantX.fromProtoV30(mapping)
            case Mapping.AuthorityOf(mapping) =>
              AuthorityOfX.fromProtoV30(mapping)
            case Mapping.DomainTrustCertificate(mapping) =>
              DomainTrustCertificateX.fromProtoV30(mapping)
            case Mapping.OwnerToKeyMapping(mapping) =>
              OwnerToKeyMappingX.fromProtoV30(mapping)
            case Mapping.VettedPackages(mapping) =>
              VettedPackagesX.fromProtoV30(mapping)
            case Mapping.ParticipantPermission(mapping) =>
              ParticipantDomainPermissionX.fromProtoV30(mapping)
            case Mapping.TrafficControlState(mapping) =>
              TrafficControlStateX.fromProtoV30(mapping)
            case Mapping.PartyHostingLimits(mapping) =>
              PartyHostingLimitsX.fromProtoV30(mapping)
            case Mapping.PurgeTopologyTxs(mapping) =>
              PurgeTopologyTransactionX.fromProtoV30(mapping)
            case _ =>
              // TODO(#14048): match missing cases
              ???
          }
        } yield {
          (op, serial, validatedMapping, signingKeys, request.forceChange)
        }
        for {
          mapping <- EitherT
            .fromEither[FutureUnlessShutdown](validatedMappingE)
            .leftMap(ProtoDeserializationFailure.Wrap(_))
          (op, serial, validatedMapping, signingKeys, forceChange) = mapping
          manager <- targetManagerET(request.store)
          signedTopoTx <- manager
            .proposeAndAuthorize(
              op,
              validatedMapping,
              serial,
              signingKeys,
              protocolVersion,
              expectFullAuthorization = request.mustFullyAuthorize,
              force = forceChange,
            )
            .leftWiden[CantonError]
        } yield {
          signedTopoTx
        }
    }
    CantonGrpcUtil.mapErrNewEUS(result.map(tx => v30.AuthorizeResponse(Some(tx.toProtoV30))))
  }

  override def signTransactions(
      request: v30.SignTransactionsRequest
  ): Future[v30.SignTransactionsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      signedTxs <- EitherT.fromEither[FutureUnlessShutdown](
        request.transactions
          .traverse(tx =>
            SignedTopologyTransactionX.fromProtoV30(ProtocolVersionValidation(protocolVersion), tx)
          )
          .leftMap(ProtoDeserializationFailure.Wrap(_): CantonError)
      )
      signingKeys <-
        EitherT
          .fromEither[FutureUnlessShutdown](
            request.signedBy.traverse(Fingerprint.fromProtoPrimitive)
          )
          .leftMap(ProtoDeserializationFailure.Wrap(_): CantonError)

      extendedTransactions <- signedTxs.parTraverse(tx =>
        signingKeys
          .parTraverse(key => crypto.privateCrypto.sign(tx.hash.hash, key))
          .leftMap(TopologyManagerError.InternalError.TopologySigningError(_): CantonError)
          .map(tx.addSignatures)
          .mapK(FutureUnlessShutdown.outcomeK)
      )
    } yield extendedTransactions

    CantonGrpcUtil.mapErrNewEUS(res.map(txs => v30.SignTransactionsResponse(txs.map(_.toProtoV30))))
  }

  override def addTransactions(
      request: v30.AddTransactionsRequest
  ): Future[v30.AddTransactionsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      signedTxs <- EitherT.fromEither[FutureUnlessShutdown](
        request.transactions
          .traverse(tx =>
            SignedTopologyTransactionX.fromProtoV30(ProtocolVersionValidation(protocolVersion), tx)
          )
          .leftMap(ProtoDeserializationFailure.Wrap(_): CantonError)
      )
      _ <- addTransactions(signedTxs, request.store, request.forceChange)
    } yield v30.AddTransactionsResponse()
    CantonGrpcUtil.mapErrNewEUS(res)
  }

  override def importTopologySnapshot(
      request: ImportTopologySnapshotRequest
  ): Future[v30.ImportTopologySnapshotResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      storedTxs <- EitherT.fromEither[FutureUnlessShutdown](
        StoredTopologyTransactionsX
          .fromTrustedByteString(request.topologySnapshot)
          .leftMap(ProtoDeserializationFailure.Wrap(_): CantonError)
      )
      signedTxs = storedTxs.result.map(_.transaction)
      _ <- addTransactions(signedTxs, request.store, request.forceChange)
    } yield v30.ImportTopologySnapshotResponse()
    CantonGrpcUtil.mapErrNewEUS(res)
  }

  private def addTransactions(
      signedTxs: Seq[SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]],
      store: String,
      forceChange: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, Unit] =
    for {
      manager <- targetManagerET(store)
      // TODO(#12390) let the caller decide whether to expect full authorization or not?
      _ <- manager
        .add(signedTxs, force = forceChange, expectFullAuthorization = false)
        .leftWiden[CantonError]
    } yield ()

  private def targetManagerET(
      store: String
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, TopologyManagerX[TopologyStoreId]] =
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

}
