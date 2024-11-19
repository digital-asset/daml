// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.driver

import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, DomainSyncCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  OrderingTopologyProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.{
  OrderingTopology,
  SequencingParameters,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

private[driver] final class CantonOrderingTopologyProvider(
    cryptoApi: DomainSyncCryptoClient,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends OrderingTopologyProvider[PekkoEnv]
    with NamedLogging {

  override def getOrderingTopologyAt(
      activationTime: TopologyActivationTime,
      assumePendingTopologyChanges: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Option[(OrderingTopology, CryptoProvider[PekkoEnv])]] = {
    val name = s"get ordering topology at activation time $activationTime"

    val maxTimestampF =
      if (!assumePendingTopologyChanges) {
        // To understand if there are (potentially relevant) pending topology changes that have been already
        //  sequenced but will only become visible after observed time advances through the sequencing
        //  of further events, we need to retrieve the maximum effective timestamp after the topology processor
        //  processed everything sequenced up to and including the predecessor of the requested activation timestamp
        //  (note that `awaitMaxTimestampUS` is exclusive on its input).
        logger.debug(s"Awaiting max timestamp for snapshot at activation time $activationTime")
        cryptoApi.awaitMaxTimestampUS(activationTime.value).map { maxTimestamp =>
          logger.debug(
            s"Max timestamp $maxTimestamp awaited successfully for snapshot at activation time $activationTime"
          )
          Right(maxTimestamp)
        }
      } else {
        logger.debug(
          s"Skipping awaiting max timestamp for snapshot at activation time $activationTime"
        )
        FutureUnlessShutdown.pure(Left(()))
      }

    logger.debug(s"Querying topology snapshot for activation time $activationTime")
    val snapshotF = cryptoApi.awaitSnapshotUS(activationTime.value)

    val topologyWithCryptoProvider = for {
      snapshot <- snapshotF
      snapshotTimestamp = snapshot.ipsSnapshot.timestamp
      _ = logger.debug(
        s"Topology snapshot queried successfully at activation time: $activationTime, snapshot timestamp: $snapshotTimestamp"
      )

      maxTimestamp <- maxTimestampF

      maybeSequencerGroup <- FutureUnlessShutdown.outcomeF(snapshot.ipsSnapshot.sequencerGroup())
      _ = logger.debug(
        s"Sequencer group queried successfully on snapshot at $snapshotTimestamp: $maybeSequencerGroup"
      )

      maybePeers = maybeSequencerGroup.map(_.active)
      maybePeersFirstKnownAt <- FutureUnlessShutdown.outcomeF(
        maybePeers
          .map(computeFirstKnownAtTimestamps(_, snapshot))
          .sequence
      )
      _ = logger.debug(
        s"Peer \"first known at\" timestamps queried successfully on snapshot at $snapshotTimestamp: $maybePeersFirstKnownAt"
      )

      sequencingDynamicParameters <- FutureUnlessShutdown.outcomeF(
        getDynamicSequencingParameters(snapshot.ipsSnapshot)
      )
      _ = logger.debug(
        s"Dynamic sequencing parameters queried successfully on snapshot at $snapshotTimestamp: $sequencingDynamicParameters"
      )
    } yield maybePeersFirstKnownAt.map { peersFirstKnownAt =>
      val peersActiveAt = peersFirstKnownAt.view
        .mapValues(
          // We first get all the peers from the domain client, so the default value should never be needed.
          _.fold(TopologyActivationTime(CantonTimestamp.MaxValue)) { case (_, effectiveTime) =>
            TopologyActivationTime.fromEffectiveTime(effectiveTime)
          }
        )
        .toMap
      val topology =
        OrderingTopology(
          peersActiveAt,
          sequencingDynamicParameters,
          activationTime,
          areTherePendingCantonTopologyChanges = maxTimestamp match {
            case Left(_) =>
              true // We skip awaiting the max timestamp, so we assume there are pending changes
            case Right(result) =>
              result.exists { case (_maxSequencedTime, EffectiveTime(maxEffectiveTime)) =>
                // The comparison is strict to avoid considering the activation time as pending.
                maxEffectiveTime > activationTime.value
              }
          },
        )
      topology -> new CantonCryptoProvider(snapshot)
    }
    PekkoFutureUnlessShutdown(name, topologyWithCryptoProvider)
  }

  private def computeFirstKnownAtTimestamps(
      peers: Seq[SequencerId],
      snapshot: DomainSnapshotSyncCryptoApi,
  )(implicit traceContext: TraceContext) =
    peers
      .map { peerId =>
        snapshot.ipsSnapshot.memberFirstKnownAt(peerId).map(peerId -> _)
      }
      .sequence
      .map { peerIdsToTimestamps =>
        logger.debug("Peer \"first known at\" timestamps queried successfully")
        peerIdsToTimestamps.toMap
      }

  private def getDynamicSequencingParameters(
      snapshot: TopologySnapshot
  )(implicit traceContext: TraceContext) =
    for {
      parametersE <- snapshot.findDynamicSequencingParameters()
      parametersO = parametersE.toOption
      payloadO = parametersO.flatMap(_.parameters.payload)
      sequencingParametersO = payloadO.map(SequencingParameters.fromPayload)
    } yield sequencingParametersO match {
      case Some(value) =>
        value match {
          case Left(error) =>
            logger.warn(s"Sequencing parameters couldn't be parsed ($error), using default")
            SequencingParameters.Default
          case Right(value) => value
        }
      case None =>
        logger.debug("Sequencing parameters not set, using default")
        SequencingParameters.Default
    }
}
