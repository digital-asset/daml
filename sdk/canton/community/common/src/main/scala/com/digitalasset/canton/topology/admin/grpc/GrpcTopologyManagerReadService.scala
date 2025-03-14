// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.implicits.catsSyntaxEitherId
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Crypto, Fingerprint}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.wrapErrUS
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.v30.*
import com.digitalasset.canton.topology.admin.{grpc, v30 as adminProto}
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TimeQuery,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, GrpcStreamingUtils}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import io.grpc.stub.StreamObserver

import java.io.OutputStream
import scala.concurrent.{ExecutionContext, Future}

final case class BaseQuery(
    store: Option[grpc.TopologyStoreId],
    proposals: Boolean,
    timeQuery: TimeQuery,
    ops: Option[TopologyChangeOp],
    filterSigningKey: String,
    protocolVersion: Option[ProtocolVersion],
) {
  def toProtoV1: adminProto.BaseQuery =
    adminProto.BaseQuery(
      store.map(_.toProtoV30),
      proposals,
      ops.map(_.toProto).getOrElse(v30.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_UNSPECIFIED),
      timeQuery.toProtoV30,
      filterSigningKey,
      protocolVersion.map(_.toProtoPrimitive),
    )
}

object BaseQuery {
  def apply(
      store: TopologyStoreId,
      proposals: Boolean,
      timeQuery: TimeQuery,
      ops: Option[TopologyChangeOp],
      filterSigningKey: String,
      protocolVersion: Option[ProtocolVersion],
  ): BaseQuery =
    BaseQuery(
      Some(store),
      proposals,
      timeQuery,
      ops,
      filterSigningKey,
      protocolVersion,
    )

  def fromProto(value: Option[adminProto.BaseQuery]): ParsingResult[BaseQuery] =
    for {
      baseQuery <- ProtoConverter.required("base_query", value)
      proposals = baseQuery.proposals
      filterSignedKey = baseQuery.filterSignedKey
      timeQuery <- TimeQuery.fromProto(baseQuery.timeQuery, "time_query")
      operationOp <- TopologyChangeOp.fromProtoV30(baseQuery.operation)
      protocolVersion <- baseQuery.protocolVersion.traverse(ProtocolVersion.fromProtoPrimitive(_))
      store <- baseQuery.store.traverse(
        grpc.TopologyStoreId.fromProtoV30(_, "store")
      )
    } yield BaseQuery(
      store,
      proposals,
      timeQuery,
      operationOp,
      filterSignedKey,
      protocolVersion,
    )
}

class GrpcTopologyManagerReadService(
    member: Member,
    stores: => Seq[topology.store.TopologyStore[topology.store.TopologyStoreId]],
    crypto: Crypto,
    topologyClientLookup: topology.store.TopologyStoreId => Option[SynchronizerTopologyClient],
    processingTimeout: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends adminProto.TopologyManagerReadServiceGrpc.TopologyManagerReadService
    with NamedLogging {

  private case class TransactionSearchResult(
      store: TopologyStoreId,
      sequenced: SequencedTime,
      validFrom: EffectiveTime,
      validUntil: Option[EffectiveTime],
      operation: TopologyChangeOp,
      transactionHash: ByteString,
      serial: PositiveInt,
      signedBy: NonEmpty[Set[Fingerprint]],
  )

  private def collectStores(
      storeO: Option[grpc.TopologyStoreId]
  ): EitherT[FutureUnlessShutdown, CantonError, Seq[
    topology.store.TopologyStore[topology.store.TopologyStoreId]
  ]] =
    storeO match {
      case Some(store) =>
        EitherT.rightT(stores.filter(_.storeId == store.toInternal))
      case None => EitherT.rightT(stores)
    }

  private def collectSynchronizerStore(
      storeO: Option[grpc.TopologyStoreId]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, topology.store.TopologyStore[
    topology.store.TopologyStoreId
  ]] = {
    val synchronizerStores
        : Either[CantonError, topology.store.TopologyStore[topology.store.TopologyStoreId]] =
      storeO match {
        case Some(store) =>
          val targetStoreInternal = store.toInternal
          val synchronizerStores = stores.filter { s =>
            s.storeId.isSynchronizerStore && s.storeId == targetStoreInternal
          }
          synchronizerStores match {
            case Nil =>
              TopologyManagerError.TopologyStoreUnknown
                .Failure(targetStoreInternal)
                .asLeft
            case Seq(synchronizerStore) => synchronizerStore.asRight
            case _ =>
              TopologyManagerError.InvalidSynchronizer
                .MultipleSynchronizerStoresFound(targetStoreInternal)
                .asLeft
          }

        case None =>
          stores.find(_.storeId.isSynchronizerStore) match {
            case Some(synchronizerStore) => synchronizerStore.asRight
            case None =>
              TopologyManagerError.TopologyStoreUnknown.NoSynchronizerStoreAvailable().asLeft
          }
      }

    EitherT.fromEither[FutureUnlessShutdown](synchronizerStores)

  }

  private def createBaseResult(context: TransactionSearchResult): adminProto.BaseResult =
    new adminProto.BaseResult(
      store = Some(context.store.toProtoV30),
      sequenced = Some(context.sequenced.value.toProtoTimestamp),
      validFrom = Some(context.validFrom.value.toProtoTimestamp),
      validUntil = context.validUntil.map(_.value.toProtoTimestamp),
      operation = context.operation.toProto,
      transactionHash = context.transactionHash,
      serial = context.serial.unwrap,
      signedByFingerprints = context.signedBy.map(_.unwrap).toSeq,
    )

  // to avoid race conditions, we want to use the approximateTimestamp of the topology client.
  // otherwise, we might read stuff from the database that isn't yet known to the node
  private def getApproximateTimestamp(
      storeId: topology.store.TopologyStoreId
  ): Option[CantonTimestamp] =
    topologyClientLookup(storeId).map(_.approximateTimestamp)

  private def collectFromStoresByFilterString(
      baseQueryProto: Option[adminProto.BaseQuery],
      typ: TopologyMapping.Code,
      filterString: String,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, Seq[
    (TransactionSearchResult, TopologyMapping)
  ]] = {
    val (idFilter, namespaceFilter) = UniqueIdentifier.splitFilter(filterString)
    collectFromStores(
      baseQueryProto,
      typ,
      idFilter = Some(idFilter),
      namespaceFilter = namespaceFilter,
    )
  }

  /** Collects mappings of specified type from stores specified in baseQueryProto satisfying the
    * filters specified in baseQueryProto as well as separately specified filter either by a
    * namespace prefix (Left) or by a uid prefix (Right) depending on which applies to the mapping
    * type.
    */
  private def collectFromStores(
      baseQueryProto: Option[adminProto.BaseQuery],
      typ: TopologyMapping.Code,
      idFilter: Option[String],
      namespaceFilter: Option[String],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, Seq[(TransactionSearchResult, TopologyMapping)]] = {

    def fromStore(
        baseQuery: BaseQuery,
        store: topology.store.TopologyStore[topology.store.TopologyStoreId],
    ): FutureUnlessShutdown[Seq[(TransactionSearchResult, TopologyMapping)]] = {
      val storeId = store.storeId

      store
        .inspect(
          proposals = baseQuery.proposals,
          timeQuery = baseQuery.timeQuery,
          asOfExclusiveO = getApproximateTimestamp(storeId),
          op = baseQuery.ops,
          types = Seq(typ),
          idFilter = idFilter,
          namespaceFilter = namespaceFilter,
        )
        .flatMap { col =>
          col.result
            .filter(
              baseQuery.filterSigningKey.isEmpty || _.transaction.signatures
                .exists(_.signedBy.unwrap.startsWith(baseQuery.filterSigningKey))
            )
            .parTraverse { tx =>
              val resultE = for {
                // Re-create the signed topology transaction if necessary
                signedTx <- baseQuery.protocolVersion
                  .map { protocolVersion =>
                    SignedTopologyTransaction
                      .asVersion(tx.transaction, protocolVersion)(crypto)
                      .leftMap[Throwable](err =>
                        new IllegalStateException(s"Failed to convert topology transaction: $err")
                      )
                  }
                  .getOrElse {
                    // Keep the original transaction in its existing protocol version if no desired protocol version is specified
                    EitherT.rightT[FutureUnlessShutdown, Throwable](tx.transaction)
                  }

                result = TransactionSearchResult(
                  TopologyStoreId.fromInternal(storeId),
                  tx.sequenced,
                  tx.validFrom,
                  tx.validUntil,
                  signedTx.operation,
                  signedTx.transaction.hash.hash.getCryptographicEvidence,
                  signedTx.transaction.serial,
                  signedTx.signatures.map(_.signedBy),
                )
              } yield (result, tx.transaction.transaction.mapping)

              EitherTUtil.toFutureUnlessShutdown(resultE)
            }
        }
    }

    for {
      baseQuery <- wrapErrUS(BaseQuery.fromProto(baseQueryProto))
      stores <- collectStores(baseQuery.store)
      results <- EitherT.right(stores.parTraverse { store =>
        fromStore(baseQuery, store)
      })
    } yield {
      val res = results.flatten
      if (baseQuery.filterSigningKey.nonEmpty)
        res.filter(x => x._1.signedBy.exists(_.unwrap.startsWith(baseQuery.filterSigningKey)))
      else res
    }
  }

  override def listNamespaceDelegation(
      request: adminProto.ListNamespaceDelegationRequest
  ): Future[adminProto.ListNamespaceDelegationResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        NamespaceDelegation.code,
        idFilter = None,
        namespaceFilter = Some(request.filterNamespace),
      )
    } yield {
      val results = res
        .collect {
          case (result, x: NamespaceDelegation)
              if request.filterTargetKeyFingerprint.isEmpty || x.target.fingerprint.unwrap == request.filterTargetKeyFingerprint =>
            (result, x)
        }
        .map { case (context, elem) =>
          new adminProto.ListNamespaceDelegationResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListNamespaceDelegationResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listDecentralizedNamespaceDefinition(
      request: adminProto.ListDecentralizedNamespaceDefinitionRequest
  ): Future[adminProto.ListDecentralizedNamespaceDefinitionResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        DecentralizedNamespaceDefinition.code,
        idFilter = None,
        namespaceFilter = Some(request.filterNamespace),
      )
    } yield {
      val results = res
        .collect { case (result, x: DecentralizedNamespaceDefinition) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListDecentralizedNamespaceDefinitionResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListDecentralizedNamespaceDefinitionResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listIdentifierDelegation(
      request: adminProto.ListIdentifierDelegationRequest
  ): Future[adminProto.ListIdentifierDelegationResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        IdentifierDelegation.code,
        request.filterUid,
      )
    } yield {
      val results = res
        .collect {
          case (result, x: IdentifierDelegation)
              if request.filterTargetKeyFingerprint.isEmpty || x.target.fingerprint.unwrap == request.filterTargetKeyFingerprint =>
            (result, x)
        }
        .map { case (context, elem) =>
          new adminProto.ListIdentifierDelegationResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListIdentifierDelegationResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listOwnerToKeyMapping(
      request: adminProto.ListOwnerToKeyMappingRequest
  ): Future[adminProto.ListOwnerToKeyMappingResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        OwnerToKeyMapping.code,
        request.filterKeyOwnerUid,
      )
    } yield {
      val results = res
        .collect {
          // topology store indexes by uid, so need to filter out the members of the wrong type
          case (result, x: OwnerToKeyMapping)
              if x.member.filterString.startsWith(request.filterKeyOwnerUid) &&
                (request.filterKeyOwnerType.isEmpty || request.filterKeyOwnerType == x.member.code.threeLetterId.unwrap) =>
            (result, x)
        }
        .map { case (context, elem) =>
          new adminProto.ListOwnerToKeyMappingResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }
      adminProto.ListOwnerToKeyMappingResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listPartyToKeyMapping(
      request: ListPartyToKeyMappingRequest
  ): Future[ListPartyToKeyMappingResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        PartyToKeyMapping.code,
        request.filterParty,
      )
    } yield {
      val results = res
        .collect { case (result, x: PartyToKeyMapping) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListPartyToKeyMappingResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }
      adminProto.ListPartyToKeyMappingResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listSynchronizerTrustCertificate(
      request: adminProto.ListSynchronizerTrustCertificateRequest
  ): Future[adminProto.ListSynchronizerTrustCertificateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        SynchronizerTrustCertificate.code,
        request.filterUid,
      )
    } yield {
      val results = res
        .collect { case (result, x: SynchronizerTrustCertificate) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListSynchronizerTrustCertificateResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListSynchronizerTrustCertificateResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listPartyHostingLimits(
      request: ListPartyHostingLimitsRequest
  ): Future[ListPartyHostingLimitsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        PartyHostingLimits.code,
        request.filterUid,
      )
    } yield {
      val results = res
        .collect { case (result, x: PartyHostingLimits) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListPartyHostingLimitsResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListPartyHostingLimitsResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listParticipantSynchronizerPermission(
      request: adminProto.ListParticipantSynchronizerPermissionRequest
  ): Future[adminProto.ListParticipantSynchronizerPermissionResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        ParticipantSynchronizerPermission.code,
        request.filterUid,
      )
    } yield {
      val results = res
        .collect { case (result, x: ParticipantSynchronizerPermission) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListParticipantSynchronizerPermissionResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListParticipantSynchronizerPermissionResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listVettedPackages(
      request: adminProto.ListVettedPackagesRequest
  ): Future[adminProto.ListVettedPackagesResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        VettedPackages.code,
        request.filterParticipant,
      )
    } yield {
      val results = res
        .collect { case (result, x: VettedPackages) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListVettedPackagesResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListVettedPackagesResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listPartyToParticipant(
      request: adminProto.ListPartyToParticipantRequest
  ): Future[adminProto.ListPartyToParticipantResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        PartyToParticipant.code,
        request.filterParty,
      )
    } yield {
      def partyPredicate(x: PartyToParticipant) =
        x.partyId.toProtoPrimitive.startsWith(request.filterParty)
      def participantPredicate(x: PartyToParticipant) =
        request.filterParticipant.isEmpty || x.participantIds.exists(
          _.toProtoPrimitive.contains(request.filterParticipant)
        )

      val results = res
        .collect {
          case (result, x: PartyToParticipant) if partyPredicate(x) && participantPredicate(x) =>
            (result, x)
        }
        .map { case (context, elem) =>
          new adminProto.ListPartyToParticipantResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListPartyToParticipantResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listSynchronizerParametersState(
      request: adminProto.ListSynchronizerParametersStateRequest
  ): Future[adminProto.ListSynchronizerParametersStateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        SynchronizerParametersState.code,
        request.filterSynchronizerId,
      )
    } yield {
      val results = res
        .collect { case (result, x: SynchronizerParametersState) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListSynchronizerParametersStateResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.parameters.toProtoV30),
          )
        }

      adminProto.ListSynchronizerParametersStateResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listMediatorSynchronizerState(
      request: adminProto.ListMediatorSynchronizerStateRequest
  ): Future[adminProto.ListMediatorSynchronizerStateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        MediatorSynchronizerState.code,
        request.filterSynchronizerId,
      )
    } yield {
      val results = res
        .collect { case (result, x: MediatorSynchronizerState) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListMediatorSynchronizerStateResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListMediatorSynchronizerStateResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listSequencerSynchronizerState(
      request: adminProto.ListSequencerSynchronizerStateRequest
  ): Future[adminProto.ListSequencerSynchronizerStateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        SequencerSynchronizerState.code,
        request.filterSynchronizerId,
      )
    } yield {
      val results = res
        .collect { case (result, x: SequencerSynchronizerState) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListSequencerSynchronizerStateResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListSequencerSynchronizerStateResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listAvailableStores(
      request: adminProto.ListAvailableStoresRequest
  ): Future[adminProto.ListAvailableStoresResponse] = Future.successful(
    adminProto.ListAvailableStoresResponse(storeIds =
      stores.map(s => TopologyStoreId.fromInternal(s.storeId).toProtoV30)
    )
  )

  override def listAll(request: adminProto.ListAllRequest): Future[adminProto.ListAllResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      baseQuery <- wrapErrUS(BaseQuery.fromProto(request.baseQuery))
      excludeTopologyMappings <- wrapErrUS(
        request.excludeMappings.traverse(TopologyMapping.Code.fromString)
      )
      types = TopologyMapping.Code.all.diff(excludeTopologyMappings)
      storedTopologyTransactions <- listAllStoredTopologyTransactions(
        baseQuery,
        types,
        request.filterNamespace,
      )
    } yield {
      adminProto.ListAllResponse(result = Some(storedTopologyTransactions.toProtoV30))
    }
    CantonGrpcUtil.mapErrNewEUS(res)
  }

  override def exportTopologySnapshot(
      request: ExportTopologySnapshotRequest,
      responseObserver: StreamObserver[ExportTopologySnapshotResponse],
  ): Unit =
    GrpcStreamingUtils.streamToClient[ExportTopologySnapshotResponse](
      (out: OutputStream) => getTopologySnapshot(request, out),
      responseObserver,
      byteString => ExportTopologySnapshotResponse(byteString),
      processingTimeout.unbounded.duration,
    )

  private def getTopologySnapshot(
      request: ExportTopologySnapshotRequest,
      out: OutputStream,
  ): Future[Unit] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      baseQuery <- wrapErrUS(BaseQuery.fromProto(request.baseQuery))
      excludeTopologyMappings <- wrapErrUS(
        request.excludeMappings.traverse(TopologyMapping.Code.fromString)
      )
      types = TopologyMapping.Code.all.diff(excludeTopologyMappings)
      storedTopologyTransactions <- listAllStoredTopologyTransactions(
        baseQuery,
        types,
        request.filterNamespace,
      )

    } yield {
      val protocolVersion = baseQuery.protocolVersion.getOrElse(ProtocolVersion.latest)
      storedTopologyTransactions.toByteString(protocolVersion).writeTo(out)
    }
    CantonGrpcUtil.mapErrNewEUS(res)
  }

  private def listAllStoredTopologyTransactions(
      baseQuery: BaseQuery,
      topologyMappings: Seq[TopologyMapping.Code],
      filterNamespace: String,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, GenericStoredTopologyTransactions] =
    for {
      stores <- collectStores(baseQuery.store)
      results <- EitherT.right(
        stores.parTraverse { store =>
          store
            .inspect(
              proposals = baseQuery.proposals,
              timeQuery = baseQuery.timeQuery,
              asOfExclusiveO = getApproximateTimestamp(store.storeId),
              op = baseQuery.ops,
              types = topologyMappings,
              idFilter = None,
              namespaceFilter = Some(filterNamespace),
            )
        }
      ): EitherT[FutureUnlessShutdown, CantonError, Seq[GenericStoredTopologyTransactions]]
    } yield {
      val res = results.foldLeft(StoredTopologyTransactions.empty) { case (acc, elem) =>
        StoredTopologyTransactions(
          acc.result ++ elem.result.filter(
            baseQuery.filterSigningKey.isEmpty || _.transaction.signatures.exists(
              _.signedBy.unwrap.startsWith(baseQuery.filterSigningKey)
            )
          )
        )
      }
      if (logger.underlying.isDebugEnabled()) {
        logger.debug(s"All listed topology transactions: ${res.result}")
      }
      res
    }

  override def genesisState(
      request: GenesisStateRequest,
      responseObserver: StreamObserver[GenesisStateResponse],
  ): Unit =
    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => getGenesisState(request.synchronizerStore, request.timestamp, out),
      responseObserver,
      byteString => GenesisStateResponse(byteString),
      processingTimeout.unbounded.duration,
    )

  private def getGenesisState(
      filterSynchronizerStore: Option[StoreId],
      timestamp: Option[Timestamp],
      out: OutputStream,
  ): Future[Unit] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val res: EitherT[FutureUnlessShutdown, CantonError, Unit] =
      for {
        _ <- member match {
          case _: ParticipantId =>
            wrapErrUS(
              ProtoConverter
                .required("filter_synchronizer_store", filterSynchronizerStore)
            )

          case _ => EitherT.rightT[FutureUnlessShutdown, CantonError](())
        }
        topologyStoreO <- wrapErrUS(
          filterSynchronizerStore.traverse(
            grpc.TopologyStoreId.fromProtoV30(_, "filter_synchronizer_store")
          )
        )
        synchronizerTopologyStore <- collectSynchronizerStore(topologyStoreO)
        timestampO <- wrapErrUS(
          timestamp
            .traverse(CantonTimestamp.fromProtoTimestamp)
        )

        sequencedTimestamp <- timestampO match {
          case Some(value) => EitherT.rightT[FutureUnlessShutdown, CantonError](value)
          case None =>
            val sequencedTimeF = synchronizerTopologyStore
              .maxTimestamp(SequencedTime.MaxValue, includeRejected = true)
              .map {
                case Some((sequencedTime, _)) =>
                  Right(sequencedTime.value)

                case None =>
                  Left(
                    TopologyManagerError.TopologyTransactionNotFound.EmptyStore()
                  )
              }

            EitherT(sequencedTimeF)
        }

        topologySnapshot <- EitherT.right[CantonError](
          synchronizerTopologyStore.findEssentialStateAtSequencedTime(
            SequencedTime(sequencedTimestamp),
            includeRejected = false,
          )
        )
        // reset effective time and sequenced time if we are initializing the sequencer from the beginning
        genesisState: StoredTopologyTransactions[TopologyChangeOp, TopologyMapping] =
          StoredTopologyTransactions[TopologyChangeOp, TopologyMapping](
            topologySnapshot.result.map(stored =>
              StoredTopologyTransaction(
                SequencedTime(SignedTopologyTransaction.InitialTopologySequencingTime),
                EffectiveTime(SignedTopologyTransaction.InitialTopologySequencingTime),
                stored.validUntil.map(_ =>
                  EffectiveTime(SignedTopologyTransaction.InitialTopologySequencingTime)
                ),
                stored.transaction,
                stored.rejectionReason,
              )
            )
          )
      } yield {
        genesisState.toByteString(ProtocolVersion.latest).writeTo(out)
      }
    CantonGrpcUtil.mapErrNewEUS(res)
  }

  override def listPurgeTopologyTransaction(
      request: ListPurgeTopologyTransactionRequest
  ): Future[ListPurgeTopologyTransactionResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        PurgeTopologyTransaction.code,
        request.filterSynchronizerId,
      )
    } yield {
      val results = res
        .collect { case (result, x: PurgeTopologyTransaction) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListPurgeTopologyTransactionResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListPurgeTopologyTransactionResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

}
