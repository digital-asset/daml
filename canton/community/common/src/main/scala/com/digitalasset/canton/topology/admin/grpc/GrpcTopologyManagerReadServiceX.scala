// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Crypto, Fingerprint}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.wrapErr
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.admin.v30.{
  ListPartyHostingLimitsRequest,
  ListPartyHostingLimitsResponse,
  ListPurgeTopologyTransactionRequest,
  ListPurgeTopologyTransactionResponse,
  ListTrafficStateRequest,
  ListTrafficStateResponse,
}
import com.digitalasset.canton.topology.admin.v30 as adminProto
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.GenericStoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactionsX,
  TimeQuery,
  TopologyStoreId,
  TopologyStoreX,
}
import com.digitalasset.canton.topology.transaction.{
  AuthorityOfX,
  DecentralizedNamespaceDefinitionX,
  DomainParametersStateX,
  DomainTrustCertificateX,
  IdentifierDelegationX,
  MediatorDomainStateX,
  NamespaceDelegationX,
  OwnerToKeyMappingX,
  ParticipantDomainPermissionX,
  PartyHostingLimitsX,
  PartyToParticipantX,
  PurgeTopologyTransactionX,
  SequencerDomainStateX,
  SignedTopologyTransactionX,
  TopologyChangeOpX,
  TopologyMappingX,
  TrafficControlStateX,
  VettedPackagesX,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{EitherTUtil, OptionUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

final case class BaseQueryX(
    filterStore: Option[TopologyStore],
    proposals: Boolean,
    timeQuery: TimeQuery,
    ops: Option[TopologyChangeOpX],
    filterSigningKey: String,
    protocolVersion: Option[ProtocolVersion],
) {
  def toProtoV1: adminProto.BaseQuery =
    adminProto.BaseQuery(
      filterStore.map(_.toProto),
      proposals,
      ops.map(_.toProto).getOrElse(TopologyChangeOpX.Replace.toProto),
      filterOperation = true,
      timeQuery.toProtoV30,
      filterSigningKey,
      protocolVersion.map(_.toProtoPrimitiveS),
    )
}

object BaseQueryX {
  def apply(
      filterStore: String,
      proposals: Boolean,
      timeQuery: TimeQuery,
      ops: Option[TopologyChangeOpX],
      filterSigningKey: String,
      protocolVersion: Option[ProtocolVersion],
  ): BaseQueryX =
    BaseQueryX(
      OptionUtil.emptyStringAsNone(filterStore).map(TopologyStore.tryFromString),
      proposals,
      timeQuery,
      ops,
      filterSigningKey,
      protocolVersion,
    )

  def fromProto(value: Option[adminProto.BaseQuery]): ParsingResult[BaseQueryX] =
    for {
      baseQuery <- ProtoConverter.required("base_query", value)
      proposals = baseQuery.proposals
      filterSignedKey = baseQuery.filterSignedKey
      timeQuery <- TimeQuery.fromProto(baseQuery.timeQuery, "time_query")
      opsRaw <- TopologyChangeOpX.fromProtoV30(baseQuery.operation)
      protocolVersion <- baseQuery.protocolVersion.traverse(ProtocolVersion.fromProtoPrimitiveS)
      filterStore <- baseQuery.filterStore.traverse(TopologyStore.fromProto(_, "filter_store"))
    } yield BaseQueryX(
      filterStore,
      proposals,
      timeQuery,
      if (baseQuery.filterOperation) Some(opsRaw) else None,
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
  ): Either[ProtoDeserializationError, TopologyStore] = {
    store.store match {
      case adminProto.Store.Store.Empty => Left(ProtoDeserializationError.FieldNotSet(fieldName))
      case adminProto.Store.Store.Authorized(_) => Right(TopologyStore.Authorized)
      case adminProto.Store.Store.Domain(domain) =>
        DomainId.fromProtoPrimitive(domain.id, fieldName).map(TopologyStore.Domain)
    }
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
    stores: => Seq[TopologyStoreX[TopologyStoreId]],
    crypto: Crypto,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends adminProto.TopologyManagerReadServiceGrpc.TopologyManagerReadService
    with NamedLogging {

  private case class TransactionSearchResult(
      store: TopologyStoreId,
      sequenced: SequencedTime,
      validFrom: EffectiveTime,
      validUntil: Option[EffectiveTime],
      operation: TopologyChangeOpX,
      transactionHash: ByteString,
      serial: PositiveInt,
      signedBy: NonEmpty[Set[Fingerprint]],
  )

  private def collectStores(
      filterStoreO: Option[TopologyStore]
  ): EitherT[Future, CantonError, Seq[TopologyStoreX[TopologyStoreId]]] = filterStoreO match {
    case Some(filterStore) =>
      EitherT.rightT(stores.filter(_.storeId.filterName.startsWith(filterStore.filterString)))
    case None => EitherT.rightT(stores)
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
    None // TODO(#14048): Address when the topology client / processor pipeline is up and running

  /** Collects mappings of specified type from stores specified in baseQueryProto satisfying the
    * filters specified in baseQueryProto as well as separately specified filter either by
    * a namespace prefix (Left) or by a uid prefix (Right) depending on which applies to the mapping type.
    */
  private def collectFromStores(
      baseQueryProto: Option[adminProto.BaseQuery],
      typ: TopologyMappingX.Code,
      namespaceOrUidFilter: Either[String, String],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, CantonError, Seq[(TransactionSearchResult, TopologyMappingX)]] = {

    def fromStore(
        baseQuery: BaseQueryX,
        store: TopologyStoreX[TopologyStoreId],
    ): Future[Seq[(TransactionSearchResult, TopologyMappingX)]] = {
      val storeId = store.storeId

      store
        .inspect(
          proposals = baseQuery.proposals,
          timeQuery = baseQuery.timeQuery,
          recentTimestampO = getApproximateTimestamp(storeId),
          op = baseQuery.ops,
          typ = Some(typ),
          idFilter = namespaceOrUidFilter.merge,
          namespaceOnly = namespaceOrUidFilter.isLeft,
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
                    SignedTopologyTransactionX
                      .asVersion(tx.transaction, protocolVersion)(crypto)
                      .leftMap[Throwable](err =>
                        new IllegalStateException(s"Failed to convert topology transaction: $err")
                      )
                  }
                  .getOrElse {
                    // Keep the original transaction in its existing protocol version if no desired protocol version is specified
                    EitherT.rightT[Future, Throwable](tx.transaction)
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

              EitherTUtil.toFuture(resultE)
            }
        }
    }

    for {
      baseQuery <- wrapErr(BaseQueryX.fromProto(baseQueryProto))
      stores <- collectStores(baseQuery.filterStore)
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
        NamespaceDelegationX.code,
        Left(request.filterNamespace),
      )
    } yield {
      val results = res
        .collect {
          case (result, x: NamespaceDelegationX)
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
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listDecentralizedNamespaceDefinition(
      request: adminProto.ListDecentralizedNamespaceDefinitionRequest
  ): Future[adminProto.ListDecentralizedNamespaceDefinitionResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        DecentralizedNamespaceDefinitionX.code,
        Left(request.filterNamespace),
      )
    } yield {
      val results = res
        .collect { case (result, x: DecentralizedNamespaceDefinitionX) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListDecentralizedNamespaceDefinitionResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListDecentralizedNamespaceDefinitionResponse(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listIdentifierDelegation(
      request: adminProto.ListIdentifierDelegationRequest
  ): Future[adminProto.ListIdentifierDelegationResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        IdentifierDelegationX.code,
        Right(request.filterUid),
      )
    } yield {
      val results = res
        .collect {
          case (result, x: IdentifierDelegationX)
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
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listOwnerToKeyMapping(
      request: adminProto.ListOwnerToKeyMappingRequest
  ): Future[adminProto.ListOwnerToKeyMappingResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        OwnerToKeyMappingX.code,
        Right(request.filterKeyOwnerUid),
      )
    } yield {
      val results = res
        .collect {
          case (result, x: OwnerToKeyMappingX)
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
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listDomainTrustCertificate(
      request: adminProto.ListDomainTrustCertificateRequest
  ): Future[adminProto.ListDomainTrustCertificateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        DomainTrustCertificateX.code,
        Right(request.filterUid),
      )
    } yield {
      val results = res
        .collect { case (result, x: DomainTrustCertificateX) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListDomainTrustCertificateResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListDomainTrustCertificateResponse(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listPartyHostingLimits(
      request: ListPartyHostingLimitsRequest
  ): Future[ListPartyHostingLimitsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        PartyHostingLimitsX.code,
        Right(request.filterUid),
      )
    } yield {
      val results = res
        .collect { case (result, x: PartyHostingLimitsX) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListPartyHostingLimitsResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListPartyHostingLimitsResponse(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listParticipantDomainPermission(
      request: adminProto.ListParticipantDomainPermissionRequest
  ): Future[adminProto.ListParticipantDomainPermissionResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        ParticipantDomainPermissionX.code,
        Right(request.filterUid),
      )
    } yield {
      val results = res
        .collect { case (result, x: ParticipantDomainPermissionX) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListParticipantDomainPermissionResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListParticipantDomainPermissionResponse(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listVettedPackages(
      request: adminProto.ListVettedPackagesRequest
  ): Future[adminProto.ListVettedPackagesResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        VettedPackagesX.code,
        Right(request.filterParticipant),
      )
    } yield {
      val results = res
        .collect { case (result, x: VettedPackagesX) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListVettedPackagesResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListVettedPackagesResponse(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listPartyToParticipant(
      request: adminProto.ListPartyToParticipantRequest
  ): Future[adminProto.ListPartyToParticipantResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        PartyToParticipantX.code,
        Right(request.filterParty),
      )
    } yield {
      val results = res
        .collect {
          case (result, x: PartyToParticipantX)
              if x.partyId.toProtoPrimitive.startsWith(
                request.filterParty
              ) && (request.filterParticipant.isEmpty || x.participantIds.exists(
                _.toProtoPrimitive.contains(request.filterParticipant)
              )) =>
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
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listAuthorityOf(
      request: adminProto.ListAuthorityOfRequest
  ): Future[adminProto.ListAuthorityOfResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        AuthorityOfX.code,
        Right(request.filterParty),
      )
    } yield {
      val results = res
        .collect { case (result, x: AuthorityOfX) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListAuthorityOfResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListAuthorityOfResponse(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listDomainParametersState(
      request: adminProto.ListDomainParametersStateRequest
  ): Future[adminProto.ListDomainParametersStateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        DomainParametersStateX.code,
        Right(request.filterDomain),
      )
    } yield {
      val results = res
        .collect { case (result, x: DomainParametersStateX) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListDomainParametersStateResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.parameters.toProtoV30),
          )
        }

      adminProto.ListDomainParametersStateResponse(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listMediatorDomainState(
      request: adminProto.ListMediatorDomainStateRequest
  ): Future[adminProto.ListMediatorDomainStateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        MediatorDomainStateX.code,
        Right(request.filterDomain),
      )
    } yield {
      val results = res
        .collect { case (result, x: MediatorDomainStateX) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListMediatorDomainStateResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListMediatorDomainStateResponse(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listSequencerDomainState(
      request: adminProto.ListSequencerDomainStateRequest
  ): Future[adminProto.ListSequencerDomainStateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        SequencerDomainStateX.code,
        Right(request.filterDomain),
      )
    } yield {
      val results = res
        .collect { case (result, x: SequencerDomainStateX) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListSequencerDomainStateResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListSequencerDomainStateResponse(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listAvailableStores(
      request: adminProto.ListAvailableStoresRequest
  ): Future[adminProto.ListAvailableStoresResponse] = Future.successful(
    adminProto.ListAvailableStoresResponse(storeIds = stores.map(_.storeId.filterName))
  )

  override def listAll(request: adminProto.ListAllRequest): Future[adminProto.ListAllResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      baseQuery <- wrapErr(BaseQueryX.fromProto(request.baseQuery))
      stores <- collectStores(baseQuery.filterStore)
      results <- EitherT.right(
        stores.parTraverse { store =>
          store
            .inspect(
              proposals = baseQuery.proposals,
              timeQuery = baseQuery.timeQuery,
              recentTimestampO = getApproximateTimestamp(store.storeId),
              op = baseQuery.ops,
              typ = None,
              idFilter = "",
              namespaceOnly = false,
            )
        }
      ): EitherT[Future, CantonError, Seq[GenericStoredTopologyTransactionsX]]
    } yield {
      val res = results.foldLeft(StoredTopologyTransactionsX.empty) { case (acc, elem) =>
        StoredTopologyTransactionsX(
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
      adminProto.ListAllResponse(result = Some(res.toProtoV30))
    }
    CantonGrpcUtil.mapErrNew(res)
  }

  override def listPurgeTopologyTransaction(
      request: ListPurgeTopologyTransactionRequest
  ): Future[ListPurgeTopologyTransactionResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        PurgeTopologyTransactionX.code,
        Right(request.filterDomain),
      )
    } yield {
      val results = res
        .collect { case (result, x: PurgeTopologyTransactionX) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListPurgeTopologyTransactionResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListPurgeTopologyTransactionResponse(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listTrafficState(
      request: ListTrafficStateRequest
  ): Future[ListTrafficStateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        TrafficControlStateX.code,
        Right(request.filterMember),
      )
    } yield {
      val results = res
        .collect { case (result, x: TrafficControlStateX) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListTrafficStateResponse.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListTrafficStateResponse(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }
}
