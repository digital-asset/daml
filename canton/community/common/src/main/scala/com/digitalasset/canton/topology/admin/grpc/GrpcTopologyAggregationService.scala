// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.topology.admin.v30
import com.digitalasset.canton.topology.client.*
import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreX}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{DomainId, MemberCode, ParticipantId, PartyId}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{MonadUtil, OptionUtil}
import com.google.protobuf.timestamp.Timestamp as ProtoTimestamp

import scala.concurrent.{ExecutionContext, Future}

abstract class GrpcTopologyAggregationServiceCommon[
    Store <: TopologyStoreX[TopologyStoreId.DomainStore]
](
    stores: => Seq[Store],
    ips: IdentityProvidingServiceClient,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends v30.TopologyAggregationServiceGrpc.TopologyAggregationService
    with NamedLogging {

  protected def getTopologySnapshot(
      asOf: CantonTimestamp,
      store: Store,
  ): TopologySnapshotLoader

  private def snapshots(filterStore: String, asOf: Option[ProtoTimestamp])(implicit
      traceContext: TraceContext
  ): EitherT[Future, CantonError, List[(DomainId, TopologySnapshotLoader)]] = {
    for {
      asOfO <- wrapErr(asOf.traverse(CantonTimestamp.fromProtoPrimitive))
    } yield {
      stores.collect {
        case store if store.storeId.filterName.startsWith(filterStore) =>
          val domainId = store.storeId.domainId
          // get approximate timestamp from domain client to prevent race conditions (when we have written data into the stores but haven't yet updated the client)
          val asOf = asOfO.getOrElse(
            ips
              .forDomain(domainId)
              .map(_.approximateTimestamp)
              .getOrElse(CantonTimestamp.MaxValue)
          )
          (
            domainId,
            getTopologySnapshot(asOf, store),
          )
      }.toList
    }
  }

  private def groupBySnd[A, B, C](item: Seq[(A, B, C)]): Map[B, Seq[(A, C)]] =
    item.groupBy(_._2).map { case (b, res) =>
      (
        b,
        res.map { case (a, _, c) =>
          (a, c)
        },
      )
    }

  private def findMatchingParties(
      clients: List[(DomainId, TopologySnapshotLoader)],
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Set[PartyId]] = MonadUtil
    .foldLeftM((Set.empty[PartyId], false), clients) { case ((res, isDone), (_, client)) =>
      if (isDone) Future.successful((res, true))
      else
        client.inspectKnownParties(filterParty, filterParticipant, limit).map { found =>
          val tmp = found ++ res
          if (tmp.size >= limit) (tmp.take(limit), true) else (tmp, false)
        }
    }
    .map(_._1)

  private def findParticipants(
      clients: List[(DomainId, TopologySnapshotLoader)],
      partyId: PartyId,
  )(implicit
      traceContext: TraceContext
  ): Future[Map[ParticipantId, Map[DomainId, ParticipantPermission]]] =
    clients
      .parFlatTraverse { case (domainId, client) =>
        client
          .activeParticipantsOf(partyId.toLf)
          .map(_.map { case (participantId, attributes) =>
            (domainId, participantId, attributes.permission)
          }.toList)
      }
      .map(_.groupBy { case (_, participantId, _) => participantId }.map { case (k, v) =>
        (k, v.map { case (domain, _, permission) => (domain, permission) }.toMap)
      })

  override def listParties(
      request: v30.ListPartiesRequest
  ): Future[v30.ListPartiesResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.ListPartiesRequest(asOfP, limit, filterDomain, filterParty, filterParticipant) =
      request
    val res: EitherT[Future, CantonError, v30.ListPartiesResponse] = for {
      matched <- snapshots(filterDomain, asOfP)
      parties <- EitherT.right(
        findMatchingParties(matched, filterParty, filterParticipant, limit)
      )
      results <- EitherT.right(parties.toList.parTraverse { partyId =>
        findParticipants(matched, partyId).map(res => (partyId, res))
      })
    } yield {
      v30.ListPartiesResponse(
        results = results.map { case (partyId, participants) =>
          v30.ListPartiesResponse.Result(
            party = partyId.toProtoPrimitive,
            participants = participants.map { case (participantId, domains) =>
              v30.ListPartiesResponse.Result.ParticipantDomains(
                participant = participantId.toProtoPrimitive,
                domains = domains.map { case (domainId, permission) =>
                  v30.ListPartiesResponse.Result.ParticipantDomains.DomainPermissions(
                    domain = domainId.toProtoPrimitive,
                    permission = permission.toProtoV30,
                  )
                }.toSeq,
              )
            }.toSeq,
          )
        }
      )
    }
    CantonGrpcUtil.mapErrNew(res)
  }

  override def listKeyOwners(
      request: v30.ListKeyOwnersRequest
  ): Future[v30.ListKeyOwnersResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res: EitherT[Future, CantonError, v30.ListKeyOwnersResponse] = for {
      keyOwnerTypeO <- wrapErr(
        OptionUtil
          .emptyStringAsNone(request.filterKeyOwnerType)
          .traverse(code => MemberCode.fromProtoPrimitive(code, "filterKeyOwnerType"))
      ): EitherT[Future, CantonError, Option[MemberCode]]
      matched <- snapshots(request.filterDomain, request.asOf)
      res <- EitherT.right(matched.parTraverse { case (storeId, client) =>
        client.inspectKeys(request.filterKeyOwnerUid, keyOwnerTypeO, request.limit).map { res =>
          (storeId, res)
        }
      })
    } yield {
      val mapped = groupBySnd(res.flatMap { case (storeId, domainData) =>
        domainData.map { case (owner, keys) =>
          (storeId, owner, keys)
        }
      })
      v30.ListKeyOwnersResponse(
        results = mapped.toSeq.flatMap { case (owner, domainData) =>
          domainData.map { case (domain, keys) =>
            v30.ListKeyOwnersResponse.Result(
              keyOwner = owner.toProtoPrimitive,
              domain = domain.toProtoPrimitive,
              signingKeys = keys.signingKeys.map(_.toProtoV30),
              encryptionKeys = keys.encryptionKeys.map(_.toProtoV30),
            )
          }
        }
      )
    }
    CantonGrpcUtil.mapErrNew(res)
  }
}

class GrpcTopologyAggregationServiceX(
    stores: => Seq[TopologyStoreX[TopologyStoreId.DomainStore]],
    ips: IdentityProvidingServiceClient,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends GrpcTopologyAggregationServiceCommon[TopologyStoreX[TopologyStoreId.DomainStore]](
      stores,
      ips,
      loggerFactory,
    ) {
  override protected def getTopologySnapshot(
      asOf: CantonTimestamp,
      store: TopologyStoreX[TopologyStoreId.DomainStore],
  ): TopologySnapshotLoader =
    new StoreBasedTopologySnapshotX(
      asOf,
      store,
      StoreBasedDomainTopologyClient.NoPackageDependencies,
      loggerFactory,
    )
}
