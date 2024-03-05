// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import cats.data.EitherT
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, SyncCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitError
import com.digitalasset.canton.domain.sequencing.traffic.EnterpriseSequencerRateLimitManager.BalanceUpdateClient
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

/** Implementation of BalanceUpdateClient that uses the topology to get the traffic balance.
  * This will be replaced by the TrafficBalanceManager based implementation when stable.
  */
class BalanceUpdateClientTopologyImpl(
    syncCrypto: DomainSyncCryptoClient,
    protocolVersion: ProtocolVersion,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, closeContext: CloseContext)
    extends BalanceUpdateClient
    with NamedLogging {
  override def apply(
      member: Member,
      timestamp: CantonTimestamp,
      lastSeen: Option[CantonTimestamp],
      warnIfApproximate: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerRateLimitError, Option[TrafficBalance]] = {
    val balanceFUS = for {
      topology <- SyncCryptoClient.getSnapshotForTimestampUS(
        syncCrypto,
        timestamp,
        lastSeen,
        protocolVersion,
        warnIfApproximate,
      )
      trafficBalance <- FutureUnlessShutdown.outcomeF(
        topology.ipsSnapshot
          .trafficControlStatus(Seq(member))
          .map { statusMap =>
            statusMap
              .get(member)
              .flatten
              .map { status =>
                // Craft a `TrafficBalance` from the `MemberTrafficControlState` coming from topology
                // This is temporary while we have both implementations in place
                // Note that we use the topology "effectiveTimestamp" as the "sequencingTimestamp"
                // In the new balance implementation they are the same, but semantically for the topology one it makes more sense
                // to use the effective timestamp.
                TrafficBalance(
                  member,
                  status.serial,
                  status.totalExtraTrafficLimit.toNonNegative,
                  status.effectiveTimestamp,
                )
              }
          }
      )
    } yield trafficBalance

    EitherT.liftF(balanceFUS)
  }
  override def close(): Unit = ()

  override def lastKnownTimestamp: Option[CantonTimestamp] = Some(
    syncCrypto.approximateTimestamp
  )
}
