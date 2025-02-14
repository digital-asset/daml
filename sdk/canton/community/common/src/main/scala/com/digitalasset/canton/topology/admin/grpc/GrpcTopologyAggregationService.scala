// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.topology.admin.v30
import com.digitalasset.canton.topology.client.*
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{MemberCode, ParticipantId, PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{MonadUtil, OptionUtil}
import com.google.protobuf.timestamp.Timestamp as ProtoTimestamp

import scala.concurrent.{ExecutionContext, Future}

class GrpcTopologyAggregationService(
    stores: => Seq[TopologyStore[TopologyStoreId.SynchronizerStore]],
    ips: IdentityProvidingServiceClient,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends v30.TopologyAggregationServiceGrpc.TopologyAggregationService
    with NamedLogging {

  private def getTopologySnapshot(
      asOf: CantonTimestamp,
      store: TopologyStore[TopologyStoreId.SynchronizerStore],
  ): TopologySnapshotLoader =
    new StoreBasedTopologySnapshot(
      asOf,
      store,
      StoreBasedSynchronizerTopologyClient.NoPackageDependencies,
      loggerFactory,
    )

  private def snapshots(synchronizerIds: Set[SynchronizerId], asOf: Option[ProtoTimestamp])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonError, List[(SynchronizerId, TopologySnapshotLoader)]] =
    for {
      asOfO <- wrapErrUS(asOf.traverse(CantonTimestamp.fromProtoTimestamp))
    } yield {
      stores.collect {
        case store
            if synchronizerIds.contains(store.storeId.synchronizerId) || synchronizerIds.isEmpty =>
          val synchronizerId = store.storeId.synchronizerId
          // get approximate timestamp from synchronizer client to prevent race conditions (when we have written data into the stores but haven't yet updated the client)
          val asOf = asOfO.getOrElse(
            ips
              .forSynchronizer(synchronizerId)
              .map(_.approximateTimestamp)
              .getOrElse(CantonTimestamp.MaxValue)
          )
          (
            synchronizerId,
            getTopologySnapshot(asOf, store),
          )
      }.toList
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
      clients: List[(SynchronizerId, TopologySnapshotLoader)],
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]] = MonadUtil
    .foldLeftM((Set.empty[PartyId], false), clients) { case ((res, isDone), (_, client)) =>
      if (isDone) FutureUnlessShutdown.pure((res, true))
      else
        client.inspectKnownParties(filterParty, filterParticipant).map { found =>
          val tmp = found ++ res
          if (tmp.sizeIs >= limit) (tmp.take(limit), true) else (tmp, false)
        }
    }
    .map(_._1)

  private def findParticipants(
      clients: List[(SynchronizerId, TopologySnapshotLoader)],
      partyId: PartyId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, Map[SynchronizerId, ParticipantPermission]]] =
    clients
      .parFlatTraverse { case (synchronizerId, client) =>
        client
          .activeParticipantsOf(partyId.toLf)
          .map(_.map { case (participantId, attributes) =>
            (synchronizerId, participantId, attributes.permission)
          }.toList)
      }
      .map(_.groupBy { case (_, participantId, _) => participantId }.map { case (k, v) =>
        (k, v.map { case (synchronizerId, _, permission) => (synchronizerId, permission) }.toMap)
      })

  override def listParties(
      request: v30.ListPartiesRequest
  ): Future[v30.ListPartiesResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.ListPartiesRequest(asOfP, limit, synchronizerIdsP, filterParty, filterParticipant) =
      request
    val res: EitherT[FutureUnlessShutdown, CantonError, v30.ListPartiesResponse] = for {
      synchronizerIds <- EitherT
        .fromEither[FutureUnlessShutdown](
          synchronizerIdsP.traverse(SynchronizerId.fromProtoPrimitive(_, "synchronizer_ids"))
        )
        .leftMap(ProtoDeserializationFailure.Wrap(_): CantonError)
      matched <- snapshots(synchronizerIds.toSet, asOfP)
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
            participants = participants.map { case (participantId, synchronizers) =>
              v30.ListPartiesResponse.Result.ParticipantSynchronizers(
                participantUid = participantId.uid.toProtoPrimitive,
                synchronizers = synchronizers.map { case (synchronizerId, permission) =>
                  v30.ListPartiesResponse.Result.ParticipantSynchronizers.SynchronizerPermissions(
                    synchronizerId = synchronizerId.toProtoPrimitive,
                    permission = permission.toProtoV30,
                  )
                }.toSeq,
              )
            }.toSeq,
          )
        }
      )
    }
    CantonGrpcUtil.mapErrNewEUS(res)
  }

  override def listKeyOwners(
      request: v30.ListKeyOwnersRequest
  ): Future[v30.ListKeyOwnersResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res: EitherT[FutureUnlessShutdown, CantonError, v30.ListKeyOwnersResponse] = for {
      keyOwnerTypeO <- wrapErrUS(
        OptionUtil
          .emptyStringAsNone(request.filterKeyOwnerType)
          .traverse(code => MemberCode.fromProtoPrimitive(code, "filterKeyOwnerType"))
      ): EitherT[FutureUnlessShutdown, CantonError, Option[MemberCode]]
      synchronizerIds <- EitherT
        .fromEither[FutureUnlessShutdown](
          request.synchronizerIds.traverse(SynchronizerId.fromProtoPrimitive(_, "synchronizer_ids"))
        )
        .leftMap(ProtoDeserializationFailure.Wrap(_): CantonError)

      matched <- snapshots(synchronizerIds.toSet, request.asOf)
      res <- EitherT.right(matched.parTraverse { case (storeId, client) =>
        client.inspectKeys(request.filterKeyOwnerUid, keyOwnerTypeO, request.limit).map { res =>
          (storeId, res)
        }
      })
    } yield {
      val mapped = groupBySnd(res.flatMap { case (storeId, keyPerMember) =>
        keyPerMember.map { case (owner, keys) =>
          (storeId, owner, keys)
        }
      })
      v30.ListKeyOwnersResponse(
        results = mapped.toSeq.flatMap { case (owner, keyPerSynchronizer) =>
          keyPerSynchronizer.map { case (synchronizerId, keys) =>
            v30.ListKeyOwnersResponse.Result(
              keyOwner = owner.toProtoPrimitive,
              synchronizerId = synchronizerId.toProtoPrimitive,
              signingKeys = keys.signingKeys.map(_.toProtoV30),
              encryptionKeys = keys.encryptionKeys.map(_.toProtoV30),
            )
          }
        }
      )
    }
    CantonGrpcUtil.mapErrNewEUS(res)
  }
}
