// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.driver

import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.{
  SynchronizerSnapshotSyncCryptoApi,
  SynchronizerSyncCryptoClient,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  OrderingTopologyProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.topology.{
  OrderingTopology,
  SequencingParameters,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

private[driver] final class CantonOrderingTopologyProvider(
    cryptoApi: SynchronizerSyncCryptoClient,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends OrderingTopologyProvider[PekkoEnv]
    with NamedLogging {

  override def getOrderingTopologyAt(activationTime: TopologyActivationTime)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Option[(OrderingTopology, CryptoProvider[PekkoEnv])]] = {

    // The ordering topology for an epoch E is based on the topology snapshot queried on the instant
    //  just after the sequencing time of the last sequenced event in E-1.
    //
    // The ordering topology of E, however, also includes information about whether there are (potentially relevant)
    //  topology transactions that have been already sequenced in E-1 but have not yet become active: if that is the
    //  case, that there may be a different ordering topology for E+1 regardless of whether further topology
    //  transactions are going to be sequenced during E, but simply due to topology transactions sequenced
    //  in E-1 that may become active during E.
    //  Knowing that allows the BFT orderer to ensure that an up-to-date ordering topology will be used for E+1 by
    //  retrieving an up-to-date topology snapshot also when E ends.
    //
    // To retrieve this information, we find the maximum activation timestamp known to the topology
    //  processor after it has processed all events in E-1, and we check if it is greater than the
    //  activation time corresponding to topology snapshot being used to compute the ordering topology for E.

    logger.debug(s"Awaiting max timestamp for snapshot at activation time $activationTime")
    val maxTimestampF =
      // `awaitMaxTimestampUS` is exclusive on its input, so we call it on `activationTime.value`
      //   rather than on `activationTime.value.immediatePredecessor`.
      cryptoApi.awaitMaxTimestampUS(activationTime.value).map { maxTimestamp =>
        logger.debug(
          s"Max timestamp $maxTimestamp awaited successfully for snapshot at activation time $activationTime"
        )
        maxTimestamp
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

      maybeSequencerGroup <- snapshot.ipsSnapshot.sequencerGroup()
      _ = logger.debug(
        s"Sequencer group queried successfully on snapshot at $snapshotTimestamp: $maybeSequencerGroup"
      )

      maybePeers = maybeSequencerGroup.map(_.active)
      maybePeersFirstKnownAt <-
        maybePeers
          .map(computeFirstKnownAtTimestamps(_, snapshot))
          .sequence
      _ = logger.debug(
        s"Peer \"first known at\" timestamps queried successfully on snapshot at $snapshotTimestamp: $maybePeersFirstKnownAt"
      )

      sequencingDynamicParameters <- getDynamicSequencingParameters(snapshot.ipsSnapshot)
      _ = logger.debug(
        s"Dynamic sequencing parameters queried successfully on snapshot at $snapshotTimestamp: $sequencingDynamicParameters"
      )
    } yield maybePeersFirstKnownAt.map { peersFirstKnownAt =>
      val peersActiveAt = peersFirstKnownAt.view
        .mapValues(
          // We first get all the peers from the synchronizer client, so the default value should never be needed.
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
          areTherePendingCantonTopologyChanges = maxTimestamp.exists { case (_, maxEffectiveTime) =>
            TopologyActivationTime.fromEffectiveTime(maxEffectiveTime).value > activationTime.value
          },
        )
      topology -> new CantonCryptoProvider(snapshot)
    }
    PekkoFutureUnlessShutdown(
      s"get ordering topology at activation time $activationTime",
      topologyWithCryptoProvider,
    )
  }

  private def computeFirstKnownAtTimestamps(
      peers: Seq[SequencerId],
      snapshot: SynchronizerSnapshotSyncCryptoApi,
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
