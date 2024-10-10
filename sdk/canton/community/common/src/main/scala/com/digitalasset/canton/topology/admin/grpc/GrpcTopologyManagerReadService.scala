// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.{wrapErr, wrapErrUS}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.v30 as adminProto
import com.digitalasset.canton.topology.admin.v30.*
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TimeQuery,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{EitherTUtil, GrpcStreamingUtils, OptionUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{ProtoDeserializationError, topology}
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import io.grpc.stub.StreamObserver

import java.io.OutputStream
import scala.concurrent.{ExecutionContext, Future}

final case class BaseQuery(
    filterStore: Option[TopologyStore],
    proposals: Boolean,
    timeQuery: TimeQuery,
    ops: Option[TopologyChangeOp],
    filterSigningKey: String,
    protocolVersion: Option[ProtocolVersion],
) {
  def toProtoV1: adminProto.BaseQuery =
    adminProto.BaseQuery(
      filterStore.map(_.toProto),
      proposals,
      ops.map(_.toProto).getOrElse(v30.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_UNSPECIFIED),
      timeQuery.toProtoV30,
      filterSigningKey,
      protocolVersion.map(_.toProtoPrimitive),
    )
}

object BaseQuery {
  def apply(
      filterStore: String,
      proposals: Boolean,
      timeQuery: TimeQuery,
      ops: Option[TopologyChangeOp],
      filterSigningKey: String,
      protocolVersion: Option[ProtocolVersion],
  ): BaseQuery =
    BaseQuery(
      OptionUtil.emptyStringAsNone(filterStore).map(TopologyStore.tryFromString),
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
      filterStore <- baseQuery.filterStore.traverse(TopologyStore.fromProto(_, "filter_store"))
    } yield BaseQuery(
      filterStore,
      proposals,
      timeQuery,
      operationOp,
      filterSignedKey,
      protocolVersion,
    )
}

sealed trait TopologyStore extends Product with Serializable {
  def toProto: adminProto.Store

  def filterString: String
}

object TopologyStore {

  def tryFromString(store: String): TopologyStore =
    if (store.toLowerCase == "authorized") Authorized
    else Domain(DomainId.tryFromString(store))

  def fromProto(
      store: adminProto.Store,
      fieldName: String,
  ): Either[ProtoDeserializationError, TopologyStore] =
    store.store match {
      case adminProto.Store.Store.Empty => Left(ProtoDeserializationError.FieldNotSet(fieldName))
      case adminProto.Store.Store.Authorized(_) => Right(TopologyStore.Authorized)
      case adminProto.Store.Store.Domain(domain) =>
        DomainId.fromProtoPrimitive(domain.id, fieldName).map(TopologyStore.Domain.apply)
    }

  final case class Domain(id: DomainId) extends TopologyStore {
    override def toProto: adminProto.Store =
      adminProto.Store(
        adminProto.Store.Store.Domain(adminProto.Store.Domain(id.toProtoPrimitive))
      )

    override def filterString: String = id.toProtoPrimitive
  }

  case object Authorized extends TopologyStore {
    override def toProto: adminProto.Store =
      adminProto.Store(adminProto.Store.Store.Authorized(adminProto.Store.Authorized()))

    override def filterString: String = "Authorized"
  }
}

class GrpcTopologyManagerReadService(
    member: Member,
    stores: => Seq[topology.store.TopologyStore[TopologyStoreId]],
    crypto: Crypto,
    topologyClientLookup: TopologyStoreId => Option[DomainTopologyClient],
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
      filterStoreO: Option[TopologyStore]
  ): EitherT[Future, CantonError, Seq[topology.store.TopologyStore[TopologyStoreId]]] =
    filterStoreO match {
      case Some(filterStore) =>
        EitherT.rightT(stores.filter(_.storeId.filterName.startsWith(filterStore.filterString)))
      case None => EitherT.rightT(stores)
    }

  private def collectDomainStore(
      filterStoreO: Option[TopologyStore]
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, CantonError, topology.store.TopologyStore[TopologyStoreId]] = {
    val domainStores: Either[CantonError, store.TopologyStore[TopologyStoreId]] =
      filterStoreO match {
        case Some(filterStore) =>
          val domainStores = stores.filter { s =>
            s.storeId.isDomainStore && s.storeId.filterName.startsWith(filterStore.filterString)
          }
          domainStores match {
            case Nil =>
              TopologyManagerError.InvalidDomain.InvalidFilterStore(filterStore.filterString).asLeft
            case Seq(domainStore) => domainStore.asRight
            case _ =>
              TopologyManagerError.InvalidDomain
                .MultipleDomainStores(filterStore.filterString)
                .asLeft
          }

        case None =>
          stores.find(_.storeId.isDomainStore) match {
            case Some(domainStore) => domainStore.asRight
            case None => TopologyManagerError.InternalError.Other("No domain store found").asLeft
          }
      }

    EitherT.fromEither[Future](domainStores)

  }

  private def createBaseResult(context: TransactionSearchResult): adminProto.BaseResult = {
    val storeProto: adminProto.Store = context.store match {
      case DomainStore(domainId, _) =>
        adminProto.Store(
          adminProto.Store.Store.Domain(adminProto.Store.Domain(domainId.toProtoPrimitive))
        )
      case TopologyStoreId.AuthorizedStore =>
        adminProto.Store(
          adminProto.Store.Store.Authorized(adminProto.Store.Authorized())
        )
    }

    new adminProto.BaseResult(
      store = Some(storeProto),
      sequenced = Some(context.sequenced.value.toProtoTimestamp),
      validFrom = Some(context.validFrom.value.toProtoTimestamp),
      validUntil = context.validUntil.map(_.value.toProtoTimestamp),
      operation = context.operation.toProto,
      transactionHash = context.transactionHash,
      serial = context.serial.unwrap,
      signedByFingerprints = context.signedBy.map(_.unwrap).toSeq,
    )
  }

  // to avoid race conditions, we want to use the approximateTimestamp of the topology client.
  // otherwise, we might read stuff from the database that isn't yet known to the node
  private def getApproximateTimestamp(storeId: TopologyStoreId): Option[CantonTimestamp] =
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
      namespaceFilter = Some(namespaceFilter),
    )
  }

  /** Collects mappings of specified type from stores specified in baseQueryProto satisfying the
    * filters specified in baseQueryProto as well as separately specified filter either by
    * a namespace prefix (Left) or by a uid prefix (Right) depending on which applies to the mapping type.
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
        store: topology.store.TopologyStore[TopologyStoreId],
    ): FutureUnlessShutdown[Seq[(TransactionSearchResult, TopologyMapping)]] = {
      val storeId = store.storeId

      FutureUnlessShutdown
        .outcomeF(
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
                  storeId,
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
      stores <- collectStores(baseQuery.filterStore).mapK(FutureUnlessShutdown.outcomeK)
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

  override def listDomainTrustCertificate(
      request: adminProto.ListDomainTrustCertificateRequest
  ): Future[adminProto.ListDomainTrustCertificateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        DomainTrustCertificate.code,
        request.filterUid,
      )
    } yield {
      val results = res
        .collect { case (result, x: DomainTrustCertificate) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListDomainTrustCertificateResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListDomainTrustCertificateResponse(results = results)
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

  override def listParticipantDomainPermission(
      request: adminProto.ListParticipantDomainPermissionRequest
  ): Future[adminProto.ListParticipantDomainPermissionResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        ParticipantDomainPermission.code,
        request.filterUid,
      )
    } yield {
      val results = res
        .collect { case (result, x: ParticipantDomainPermission) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListParticipantDomainPermissionResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListParticipantDomainPermissionResponse(results = results)
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

  override def listDomainParametersState(
      request: adminProto.ListDomainParametersStateRequest
  ): Future[adminProto.ListDomainParametersStateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        DomainParametersState.code,
        request.filterDomain,
      )
    } yield {
      val results = res
        .collect { case (result, x: DomainParametersState) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListDomainParametersStateResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.parameters.toProtoV30),
          )
        }

      adminProto.ListDomainParametersStateResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listMediatorDomainState(
      request: adminProto.ListMediatorDomainStateRequest
  ): Future[adminProto.ListMediatorDomainStateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        MediatorDomainState.code,
        request.filterDomain,
      )
    } yield {
      val results = res
        .collect { case (result, x: MediatorDomainState) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListMediatorDomainStateResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListMediatorDomainStateResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listSequencerDomainState(
      request: adminProto.ListSequencerDomainStateRequest
  ): Future[adminProto.ListSequencerDomainStateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        SequencerDomainState.code,
        request.filterDomain,
      )
    } yield {
      val results = res
        .collect { case (result, x: SequencerDomainState) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListSequencerDomainStateResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListSequencerDomainStateResponse(results = results)
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listAvailableStores(
      request: adminProto.ListAvailableStoresRequest
  ): Future[adminProto.ListAvailableStoresResponse] = Future.successful(
    adminProto.ListAvailableStoresResponse(storeIds = stores.map(_.storeId.filterName))
  )

  override def listAll(request: adminProto.ListAllRequest): Future[adminProto.ListAllResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      baseQuery <- wrapErr(BaseQuery.fromProto(request.baseQuery))
      excludeTopologyMappings <- wrapErr(
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
    CantonGrpcUtil.mapErrNew(res)
  }

  override def exportTopologySnapshot(
      request: ExportTopologySnapshotRequest
  ): Future[ExportTopologySnapshotResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      baseQuery <- wrapErr(BaseQuery.fromProto(request.baseQuery))
      excludeTopologyMappings <- wrapErr(
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
      adminProto.ExportTopologySnapshotResponse(result =
        storedTopologyTransactions.toByteString(protocolVersion)
      )
    }
    CantonGrpcUtil.mapErrNew(res)
  }

  private def listAllStoredTopologyTransactions(
      baseQuery: BaseQuery,
      topologyMappings: Seq[TopologyMapping.Code],
      filterNamespace: String,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, CantonError, GenericStoredTopologyTransactions] =
    for {
      stores <- collectStores(baseQuery.filterStore)
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
      ): EitherT[Future, CantonError, Seq[GenericStoredTopologyTransactions]]
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
      (out: OutputStream) => getGenesisState(request.filterDomainStore, request.timestamp, out),
      responseObserver,
      byteString => GenesisStateResponse(byteString),
      processingTimeout.unbounded.duration,
    )

  private def getGenesisState(
      filterDomainStore: Option[Store],
      timestamp: Option[Timestamp],
      out: OutputStream,
  ): Future[Unit] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val res: EitherT[Future, CantonError, Unit] =
      for {
        _ <- member match {
          case _: ParticipantId =>
            wrapErr(
              ProtoConverter
                .required("filter_domain_store", filterDomainStore)
            )

          case _ => EitherT.rightT[Future, CantonError](())
        }
        topologyStoreO <- wrapErr(
          filterDomainStore.traverse(TopologyStore.fromProto(_, "filter_domain_store"))
        )
        domainTopologyStore <- collectDomainStore(topologyStoreO)
        timestampO <- wrapErr(
          timestamp
            .traverse(CantonTimestamp.fromProtoTimestamp)
        )

        sequencedTimestamp <- timestampO match {
          case Some(value) => EitherT.rightT[Future, CantonError](value)
          case None =>
            val sequencedTimeF = domainTopologyStore
              .maxTimestamp(CantonTimestamp.MaxValue, includeRejected = true)
              .collect {
                case Some((sequencedTime, _)) =>
                  Right(sequencedTime.value)

                case None =>
                  Left(
                    TopologyManagerError.InternalError.Other(s"No sequenced time found")
                  )
              }

            EitherT(sequencedTimeF)
        }

        // we exclude TrafficControlState from the genesis state because this mapping will be deleted.
        topologySnapshot <- EitherT.right[CantonError](
          domainTopologyStore.findEssentialStateAtSequencedTime(
            SequencedTime(sequencedTimestamp)
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
              )
            )
          )
      } yield {
        genesisState.toByteString(ProtocolVersion.latest).writeTo(out)
      }
    CantonGrpcUtil.mapErrNew(res)
  }

  override def listPurgeTopologyTransaction(
      request: ListPurgeTopologyTransactionRequest
  ): Future[ListPurgeTopologyTransactionResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStoresByFilterString(
        request.baseQuery,
        PurgeTopologyTransaction.code,
        request.filterDomain,
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
