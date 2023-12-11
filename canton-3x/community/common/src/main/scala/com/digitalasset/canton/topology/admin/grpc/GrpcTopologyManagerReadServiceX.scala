// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Crypto, Fingerprint}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.wrapErr
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.admin.v1.{
  ListPartyHostingLimitsRequest,
  ListPartyHostingLimitsResult,
  ListPurgeTopologyTransactionXRequest,
  ListPurgeTopologyTransactionXResult,
  ListTrafficStateRequest,
  ListTrafficStateResult,
}
import com.digitalasset.canton.topology.admin.v1 as adminProto
import com.digitalasset.canton.topology.client.IdentityProvidingServiceClient
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.GenericStoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactionsX,
  TimeQueryX,
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
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

final case class BaseQueryX(
    filterStore: String,
    proposals: Boolean,
    timeQuery: TimeQueryX,
    ops: Option[TopologyChangeOpX],
    filterSigningKey: String,
    protocolVersion: Option[ProtocolVersion],
) {
  def toProtoV1: adminProto.BaseQuery =
    adminProto.BaseQuery(
      filterStore,
      proposals,
      ops.map(_.toProto).getOrElse(TopologyChangeOpX.Replace.toProto),
      filterOperation = true,
      timeQuery.toProtoV1,
      filterSigningKey,
      protocolVersion.map(_.toProtoPrimitiveS),
    )
}

object BaseQueryX {
  def fromProto(value: Option[adminProto.BaseQuery]): ParsingResult[BaseQueryX] =
    for {
      baseQuery <- ProtoConverter.required("base_query", value)
      proposals = baseQuery.proposals
      filterStore = baseQuery.filterStore
      filterSignedKey = baseQuery.filterSignedKey
      timeQuery <- TimeQueryX.fromProto(baseQuery.timeQuery, "time_query")
      opsRaw <- TopologyChangeOpX.fromProtoV2(baseQuery.operation)
      protocolVersion <- baseQuery.protocolVersion.traverse(ProtocolVersion.fromProtoPrimitiveS)
    } yield BaseQueryX(
      filterStore,
      proposals,
      timeQuery,
      if (baseQuery.filterOperation) Some(opsRaw) else None,
      filterSignedKey,
      protocolVersion,
    )
}

class GrpcTopologyManagerReadServiceX(
    stores: => Seq[TopologyStoreX[TopologyStoreId]],
    ips: IdentityProvidingServiceClient,
    crypto: Crypto,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends adminProto.TopologyManagerReadServiceXGrpc.TopologyManagerReadServiceX
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
      filterStore: String
  ): EitherT[Future, CantonError, Seq[TopologyStoreX[TopologyStoreId]]] =
    EitherT.rightT(stores.filter(_.storeId.filterName.startsWith(filterStore)))

  private def createBaseResult(context: TransactionSearchResult): adminProto.BaseResult =
    new adminProto.BaseResult(
      store = context.store.filterName,
      sequenced = Some(context.sequenced.value.toProtoPrimitive),
      validFrom = Some(context.validFrom.value.toProtoPrimitive),
      validUntil = context.validUntil.map(_.value.toProtoPrimitive),
      operation = context.operation.toProto,
      transactionHash = context.transactionHash,
      serial = context.serial.unwrap,
      signedByFingerprints = context.signedBy.map(_.unwrap).toSeq,
    )

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
  ): Future[adminProto.ListNamespaceDelegationResult] = {
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
          new adminProto.ListNamespaceDelegationResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListNamespaceDelegationResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listDecentralizedNamespaceDefinition(
      request: adminProto.ListDecentralizedNamespaceDefinitionRequest
  ): Future[adminProto.ListDecentralizedNamespaceDefinitionResult] = {
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
          new adminProto.ListDecentralizedNamespaceDefinitionResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListDecentralizedNamespaceDefinitionResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listIdentifierDelegation(
      request: adminProto.ListIdentifierDelegationRequest
  ): Future[adminProto.ListIdentifierDelegationResult] = {
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
          new adminProto.ListIdentifierDelegationResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListIdentifierDelegationResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listOwnerToKeyMapping(
      request: adminProto.ListOwnerToKeyMappingRequest
  ): Future[adminProto.ListOwnerToKeyMappingResult] = {
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
          new adminProto.ListOwnerToKeyMappingResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }
      adminProto.ListOwnerToKeyMappingResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listDomainTrustCertificate(
      request: adminProto.ListDomainTrustCertificateRequest
  ): Future[adminProto.ListDomainTrustCertificateResult] = {
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
          new adminProto.ListDomainTrustCertificateResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListDomainTrustCertificateResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listPartyHostingLimits(
      request: ListPartyHostingLimitsRequest
  ): Future[ListPartyHostingLimitsResult] = {
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
          new adminProto.ListPartyHostingLimitsResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListPartyHostingLimitsResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listParticipantDomainPermission(
      request: adminProto.ListParticipantDomainPermissionRequest
  ): Future[adminProto.ListParticipantDomainPermissionResult] = {
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
          new adminProto.ListParticipantDomainPermissionResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListParticipantDomainPermissionResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listVettedPackages(
      request: adminProto.ListVettedPackagesRequest
  ): Future[adminProto.ListVettedPackagesResult] = {
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
          new adminProto.ListVettedPackagesResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListVettedPackagesResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listPartyToParticipant(
      request: adminProto.ListPartyToParticipantRequest
  ): Future[adminProto.ListPartyToParticipantResult] = {
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
                _.toProtoPrimitive.startsWith(request.filterParticipant)
              )) =>
            (result, x)
        }
        .map { case (context, elem) =>
          new adminProto.ListPartyToParticipantResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListPartyToParticipantResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listAuthorityOf(
      request: adminProto.ListAuthorityOfRequest
  ): Future[adminProto.ListAuthorityOfResult] = {
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
          new adminProto.ListAuthorityOfResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListAuthorityOfResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listDomainParametersState(
      request: adminProto.ListDomainParametersStateRequest
  ): Future[adminProto.ListDomainParametersStateResult] = {
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
          new adminProto.ListDomainParametersStateResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.parameters.toProtoV2),
          )
        }

      adminProto.ListDomainParametersStateResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listMediatorDomainState(
      request: adminProto.ListMediatorDomainStateRequest
  ): Future[adminProto.ListMediatorDomainStateResult] = {
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
          new adminProto.ListMediatorDomainStateResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListMediatorDomainStateResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listSequencerDomainState(
      request: adminProto.ListSequencerDomainStateRequest
  ): Future[adminProto.ListSequencerDomainStateResult] = {
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
          new adminProto.ListSequencerDomainStateResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListSequencerDomainStateResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listAvailableStores(
      request: adminProto.ListAvailableStoresRequest
  ): Future[adminProto.ListAvailableStoresResult] = Future.successful(
    adminProto.ListAvailableStoresResult(storeIds = stores.map(_.storeId.filterName))
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
      adminProto.ListAllResponse(result = Some(res.toProtoV0))
    }
    CantonGrpcUtil.mapErrNew(res)
  }

  override def listPurgeTopologyTransactionX(
      request: ListPurgeTopologyTransactionXRequest
  ): Future[ListPurgeTopologyTransactionXResult] = {
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
          new adminProto.ListPurgeTopologyTransactionXResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListPurgeTopologyTransactionXResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listTrafficState(
      request: ListTrafficStateRequest
  ): Future[ListTrafficStateResult] = {
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
          new adminProto.ListTrafficStateResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProto),
          )
        }

      adminProto.ListTrafficStateResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }
}
