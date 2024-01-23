// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.{Crypto, Fingerprint, KeyPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.wrapErr
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.admin.v0 as adminProto
import com.digitalasset.canton.topology.client.IdentityProvidingServiceClient
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactions,
  TimeQuery,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

final case class BaseQuery(
    filterStore: String,
    useStateStore: Boolean,
    timeQuery: TimeQuery,
    ops: Option[TopologyChangeOp],
    filterSigningKey: String,
    protocolVersion: Option[ProtocolVersion],
) {
  def toProtoV30: adminProto.BaseQuery =
    adminProto.BaseQuery(
      filterStore,
      useStateStore,
      ops.map(_.toProto).getOrElse(TopologyChangeOp.Add.toProto),
      ops.nonEmpty,
      timeQuery.toProtoV0,
      filterSigningKey,
      protocolVersion.map(_.toProtoPrimitiveS),
    )
}

object BaseQuery {
  def fromProto(value: Option[adminProto.BaseQuery]): ParsingResult[BaseQuery] =
    for {
      baseQuery <- ProtoConverter.required("base_query", value)
      filterStore = baseQuery.filterStore
      useStateStore = baseQuery.useStateStore
      filterSignedKey = baseQuery.filterSignedKey
      timeQuery <- TimeQuery.fromProto(baseQuery.timeQuery, "time_query")
      opsRaw <- TopologyChangeOp.fromProtoV30(baseQuery.operation)
      protocolVersion <- baseQuery.protocolVersion.traverse(ProtocolVersion.fromProtoPrimitiveS)
    } yield BaseQuery(
      filterStore,
      useStateStore,
      timeQuery,
      if (baseQuery.filterOperation) Some(opsRaw) else None,
      filterSignedKey,
      protocolVersion,
    )
}

/** GRPC service implementation for deep topology transaction inspection
  *
  * This service is mostly used for debugging and transaction exporting purposes.
  *
  * @param stores the various identity stores
  */
class GrpcTopologyManagerReadService(
    stores: => Seq[TopologyStore[TopologyStoreId]],
    ips: IdentityProvidingServiceClient,
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
      operation: TopologyChangeOp,
      serialized: ByteString,
      signedBy: Fingerprint,
  )

  private def collectStores(
      filterStore: String
  ): EitherT[Future, CantonError, Seq[TopologyStore[TopologyStoreId]]] =
    EitherT.rightT(stores.filter(_.storeId.filterName.startsWith(filterStore)))

  private def createBaseResult(context: TransactionSearchResult): adminProto.BaseResult =
    new adminProto.BaseResult(
      store = context.store.filterName,
      sequenced = Some(context.sequenced.value.toProtoPrimitive),
      validFrom = Some(context.validFrom.value.toProtoPrimitive),
      validUntil = context.validUntil.map(_.value.toProtoPrimitive),
      operation = context.operation.toProto,
      serialized = context.serialized,
      signedByFingerprint = context.signedBy.unwrap,
    )

  // to avoid race conditions, we want to use the approximateTimestamp of the topology client.
  // otherwise, we might read stuff from the database that isn't yet known to the node
  private def getApproximateTimestamp(storeId: TopologyStoreId): Option[CantonTimestamp] =
    storeId match {
      case DomainStore(domainId, _) =>
        ips.forDomain(domainId).map(_.approximateTimestamp)
      case _ => None
    }

  private def collectFromStores(
      baseQueryProto: Option[adminProto.BaseQuery],
      typ: DomainTopologyTransactionType,
      idFilter: String,
      namespaceOnly: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, CantonError, Seq[(TransactionSearchResult, TopologyMapping)]] = {

    def fromStore(
        baseQuery: BaseQuery,
        store: TopologyStore[TopologyStoreId],
    ): Future[Seq[(TransactionSearchResult, TopologyMapping)]] = {
      val storeId = store.storeId

      store
        .inspect(
          stateStore =
            // the topology manager does not write to the state store, so a state store query does not make sense
            if (storeId == TopologyStoreId.AuthorizedStore)
              false
            else baseQuery.useStateStore,
          timeQuery = baseQuery.timeQuery,
          recentTimestampO = getApproximateTimestamp(storeId),
          ops = baseQuery.ops,
          typ = Some(typ),
          idFilter = idFilter,
          namespaceOnly,
        )
        .flatMap { col =>
          col.result
            .filter(_.transaction.key.fingerprint.unwrap.startsWith(baseQuery.filterSigningKey))
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
                    EitherT.rightT[Future, Throwable](tx.transaction)
                  }

                result = TransactionSearchResult(
                  storeId,
                  tx.sequenced,
                  tx.validFrom,
                  tx.validUntil,
                  signedTx.operation,
                  signedTx.getCryptographicEvidence,
                  signedTx.key.fingerprint,
                )
              } yield (result, tx.transaction.transaction.element.mapping)

              EitherTUtil.toFuture(resultE)
            }
        }
    }

    for {
      baseQuery <- wrapErr(BaseQuery.fromProto(baseQueryProto))
      stores <- collectStores(baseQuery.filterStore)
      results <- EitherT.right(stores.parTraverse { store =>
        fromStore(baseQuery, store)
      })
    } yield {
      val res = results.flatten
      if (baseQuery.filterSigningKey.nonEmpty)
        res.filter(x => x._1.signedBy.unwrap.startsWith(baseQuery.filterSigningKey))
      else res
    }
  }

  override def listPartyToParticipant(
      request: adminProto.ListPartyToParticipantRequest
  ): Future[adminProto.ListPartyToParticipantResult] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      filterRequestSide <- wrapErr(
        request.filterRequestSide.map(_.value).traverse(RequestSide.fromProtoEnum)
      )
      filterPermission <- wrapErr(
        request.filterPermission.map(_.value).traverse(ParticipantPermission.fromProtoEnum)
      )
      res <- collectFromStores(
        request.baseQuery,
        DomainTopologyTransactionType.PartyToParticipant,
        request.filterParty,
        namespaceOnly = false,
      )
    } yield {
      val results = res
        .collect {
          case (result, x: PartyToParticipant)
              if x.party.filterString.startsWith(
                request.filterParty
              ) && x.participant.filterString.startsWith(request.filterParticipant)
                && filterRequestSide
                  .forall(_ == x.side) && filterPermission.forall(_ == x.permission) =>
            (result, x)
        }
        .map { case (context, elem) =>
          new adminProto.ListPartyToParticipantResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProtoV30),
          )
        }
      adminProto.ListPartyToParticipantResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listOwnerToKeyMapping(
      request: adminProto.ListOwnerToKeyMappingRequest
  ): Future[adminProto.ListOwnerToKeyMappingResult] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      filterKeyPurpose <- wrapErr(
        request.filterKeyPurpose.traverse(x =>
          KeyPurpose.fromProtoEnum("filterKeyPurpose", x.value)
        )
      )
      res <- collectFromStores(
        request.baseQuery,
        DomainTopologyTransactionType.OwnerToKeyMapping,
        request.filterKeyOwnerUid,
        namespaceOnly = false,
      )
    } yield {
      val results = res
        .collect {
          case (result, x: OwnerToKeyMapping)
              if x.owner.filterString.startsWith(request.filterKeyOwnerUid) &&
                (request.filterKeyOwnerType.isEmpty || request.filterKeyOwnerType == x.owner.code.threeLetterId.unwrap) &&
                (filterKeyPurpose.isEmpty || filterKeyPurpose.contains(x.key.purpose)) =>
            (result, x)
        }
        .map { case (context, elem) =>
          new adminProto.ListOwnerToKeyMappingResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProtoV30),
            keyFingerprint = elem.key.fingerprint.unwrap,
          )
        }
      adminProto.ListOwnerToKeyMappingResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listNamespaceDelegation(
      request: adminProto.ListNamespaceDelegationRequest
  ): Future[adminProto.ListNamespaceDelegationResult] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        DomainTopologyTransactionType.NamespaceDelegation,
        request.filterNamespace,
        namespaceOnly = true,
      )
    } yield {
      val results = res
        .collect { case (result, x: NamespaceDelegation) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListNamespaceDelegationResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProtoV30),
            targetKeyFingerprint = elem.target.fingerprint.unwrap,
          )
        }

      adminProto.ListNamespaceDelegationResult(results = results)
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
        DomainTopologyTransactionType.IdentifierDelegation,
        request.filterUid,
        namespaceOnly = false,
      )
    } yield {
      val results = res
        .collect { case (result, x: IdentifierDelegation) => (result, x) }
        .map { case (context, elem) =>
          new adminProto.ListIdentifierDelegationResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProtoV30),
            targetKeyFingerprint = elem.target.fingerprint.unwrap,
          )
        }
      adminProto.ListIdentifierDelegationResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listAvailableStores(
      request: adminProto.ListAvailableStoresRequest
  ): Future[adminProto.ListAvailableStoresResult] =
    Future.successful(
      adminProto.ListAvailableStoresResult(storeIds = stores.map(_.storeId.filterName))
    )

  override def listParticipantDomainState(
      request: adminProto.ListParticipantDomainStateRequest
  ): Future[adminProto.ListParticipantDomainStateResult] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        DomainTopologyTransactionType.ParticipantState,
        request.filterParticipant,
        namespaceOnly = false,
      )
    } yield {
      val results = res
        .collect {
          case (context, elem: ParticipantState)
              if elem.domain.filterString.startsWith(request.filterDomain) =>
            new adminProto.ListParticipantDomainStateResult.Result(
              context = Some(createBaseResult(context)),
              item = Some(elem.toProtoV30),
            )
        }
      adminProto.ListParticipantDomainStateResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listSignedLegalIdentityClaim(
      request: adminProto.ListSignedLegalIdentityClaimRequest
  ): Future[adminProto.ListSignedLegalIdentityClaimResult] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        DomainTopologyTransactionType.SignedLegalIdentityClaim,
        request.filterUid,
        namespaceOnly = false,
      )
    } yield {
      val results = res
        .collect { case (result, x: SignedLegalIdentityClaim) =>
          (result, x)
        }
        .map { case (context, elem) =>
          new adminProto.ListSignedLegalIdentityClaimResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(elem.toProtoV30),
          )
        }
      adminProto.ListSignedLegalIdentityClaimResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  /** Access all topology transactions
    */
  override def listAll(request: adminProto.ListAllRequest): Future[adminProto.ListAllResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      baseQuery <- wrapErr(BaseQuery.fromProto(request.baseQuery))
      stores <- collectStores(baseQuery.filterStore)
      results <- EitherT.right(
        stores.parTraverse { store =>
          store
            .inspect(
              stateStore =
                // state store doesn't make any sense for the authorized store
                if (store.storeId == TopologyStoreId.AuthorizedStore) false
                else baseQuery.useStateStore,
              timeQuery = baseQuery.timeQuery,
              recentTimestampO = getApproximateTimestamp(store.storeId),
              ops = baseQuery.ops,
              typ = None,
              idFilter = "",
              namespaceOnly = false,
            )
        }
      ): EitherT[Future, CantonError, Seq[StoredTopologyTransactions[TopologyChangeOp]]]
    } yield {
      val res = results.foldLeft(StoredTopologyTransactions.empty[TopologyChangeOp]) {
        case (acc, elem) =>
          StoredTopologyTransactions(
            acc.result ++ elem.result.filter(
              _.transaction.key.fingerprint.unwrap.startsWith(baseQuery.filterSigningKey)
            )
          )
      }
      adminProto.ListAllResponse(result = Some(res.toProtoV30))
    }
    CantonGrpcUtil.mapErrNew(res)
  }

  override def listVettedPackages(
      request: adminProto.ListVettedPackagesRequest
  ): Future[adminProto.ListVettedPackagesResult] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        DomainTopologyTransactionType.PackageUse,
        request.filterParticipant,
        namespaceOnly = false,
      )
    } yield {
      val results = res
        .collect { case (context, vetted: VettedPackages) =>
          new adminProto.ListVettedPackagesResult.Result(
            context = Some(createBaseResult(context)),
            item = Some(vetted.toProtoV30),
          )
        }
      adminProto.ListVettedPackagesResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

  override def listDomainParametersChanges(
      request: adminProto.ListDomainParametersChangesRequest
  ): Future[adminProto.ListDomainParametersChangesResult] = {
    import adminProto.ListDomainParametersChangesResult.*
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      res <- collectFromStores(
        request.baseQuery,
        DomainTopologyTransactionType.DomainParameters,
        "",
        namespaceOnly = false,
      )
    } yield {
      val results = res
        .collect { case (context, domainParametersChange: DomainParametersChange) =>
          val parameters =
            Some(Result.Parameters.V1(domainParametersChange.domainParameters.toProtoV30))

          parameters.map { parameters =>
            adminProto.ListDomainParametersChangesResult.Result(
              Some(createBaseResult(context)),
              parameters,
            )
          }
        }

      adminProto.ListDomainParametersChangesResult(results.flatten)
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
        DomainTopologyTransactionType.MediatorDomainState,
        request.filterMediator,
        namespaceOnly = false,
      )
    } yield {
      val results = res
        .collect {
          case (context, elem: MediatorDomainState)
              if elem.domain.filterString.startsWith(request.filterDomain) =>
            new adminProto.ListMediatorDomainStateResult.Result(
              context = Some(createBaseResult(context)),
              item = Some(elem.toProtoV30),
            )
        }
      adminProto.ListMediatorDomainStateResult(results = results)
    }
    CantonGrpcUtil.mapErrNew(ret)
  }

}
