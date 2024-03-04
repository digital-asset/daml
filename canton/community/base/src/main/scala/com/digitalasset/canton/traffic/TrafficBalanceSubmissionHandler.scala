// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.traffic

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, DomainSyncCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  SetTrafficBalanceMessage,
  SignedProtocolMessage,
}
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.client.{SendCallback, SendResult, SequencerClientSend}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.TrafficControlErrors.TrafficControlError
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext
import scala.math.Ordered.orderingToOrdered

/** Utility class to send traffic balance requests protocol messages to be sequenced.
  * This is abstracted out so that it can be re-used in any node's Admin API.
  */
class TrafficBalanceSubmissionHandler(
    clock: Clock,
    override protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  /** Send a signed traffic balance request.
    * @param member recipient of the new balance
    * @param domainId domainId of the domain where the top up is being sent to
    * @param protocolVersion protocol version used
    * @param serial monotonically increasing serial number for the request
    * @param totalTrafficBalance new total traffic balance
    * @param sequencerClient sequencer client to use to send the balance request
    * @param cryptoApi crypto api used to access topology
    */
  def sendTrafficBalanceRequest(
      member: Member,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      serial: PositiveInt,
      totalTrafficBalance: NonNegativeLong,
      sequencerClient: SequencerClientSend,
      cryptoApi: DomainSyncCryptoClient,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, TrafficControlError, CantonTimestamp] = {
    val topology: DomainSnapshotSyncCryptoApi = cryptoApi.currentSnapshotApproximation
    val snapshot = topology.ipsSnapshot

    for {
      trafficParams <- EitherT
        .fromOptionF(
          snapshot.trafficControlParameters(protocolVersion),
          TrafficControlErrors.TrafficControlDisabled.Error(),
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      sequencerGroup <- EitherT
        .liftF(
          snapshot
            .sequencerGroup()
            .map(
              _.getOrElse(
                ErrorUtil.invalidState(
                  "No sequencer group was found on the domain. There should at least be one sequencer (this one)."
                )
              )
            )
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      aggregationRule = AggregationRule(
        eligibleMembers = sequencerGroup.active.map(_.member),
        threshold = sequencerGroup.threshold,
        protocolVersion,
      )
      setTrafficBalanceMessage = SetTrafficBalanceMessage(
        member,
        serial,
        totalTrafficBalance,
        domainId,
        protocolVersion,
      )
      signedTrafficBalanceMessage <- EitherT
        .liftF(
          SignedProtocolMessage.trySignAndCreate(
            setTrafficBalanceMessage,
            topology,
            protocolVersion,
          )
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      batch = Batch.of(
        protocolVersion = protocolVersion,
        // This recipient tree structure allows the recipient of the top up to verify that the sequencers were also addressed
        signedTrafficBalanceMessage -> Recipients(
          NonEmpty.mk(
            Seq,
            RecipientsTree.ofMembers(
              NonEmpty.mk(Set, member), // Root of recipient tree: recipient of the top up
              Seq(
                RecipientsTree.recipientsLeaf( // Leaf of the tree: sequencers of domain group
                  NonEmpty.mk(
                    Set,
                    SequencersOfDomain: Recipient,
                  )
                )
              ),
            ),
          )
        ),
      )
      maxSequencingTimes = computeMaxSequencingTimes(trafficParams)
      _ <- maxSequencingTimes.forgetNE.parTraverse_ { maxSequencingTime =>
        logger.debug(
          s"Submitting traffic balance request for $member with balance ${totalTrafficBalance.value}, serial ${serial.value} and max sequencing time $maxSequencingTime"
        )
        sendRequest(sequencerClient, batch, aggregationRule, maxSequencingTime)
      }
    } yield maxSequencingTimes.last1
  }

  private def sendRequest(
      sequencerClient: SequencerClientSend,
      batch: Batch[DefaultOpenEnvelope],
      aggregationRule: AggregationRule,
      maxSequencingTime: CantonTimestamp,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, TrafficControlError, Unit] = {
    val callback = SendCallback.future
    for {
      _ <- sequencerClient
        .sendAsync(
          batch,
          aggregationRule = Some(aggregationRule),
          maxSequencingTime = maxSequencingTime,
          callback = callback,
        )
        .leftMap(err => TrafficControlErrors.TrafficBalanceRequestAsyncSendFailed.Error(err.show))
        .leftWiden[TrafficControlError]
        .mapK(FutureUnlessShutdown.outcomeK)
      _ <- EitherT(
        callback.future
          .map {
            case SendResult.Success(_) =>
              logger.debug(
                s"Traffic balance request with max sequencing time $maxSequencingTime successfully submitted"
              )
              Right(())
            case SendResult.Error(err) =>
              Left(TrafficControlErrors.TrafficBalanceRequestAsyncSendFailed.Error(err.show))
            case SendResult.Timeout(time) =>
              Left(
                TrafficControlErrors.TrafficBalanceRequestAsyncSendFailed.Error(
                  s"Submission timed out after sequencing time $time has elapsed"
                )
              )
          }
      ).leftWiden[TrafficControlError]
    } yield ()
  }

  private def computeMaxSequencingTimes(
      trafficParams: TrafficControlParameters
  ): NonEmpty[Seq[CantonTimestamp]] = {
    val timeWindowSize = trafficParams.setBalanceRequestSubmissionWindowSize
    val now = clock.now
    val windowUpperBound = CantonTimestamp.ofEpochMilli(
      timeWindowSize.duration
        .multipliedBy(
          now.toEpochMilli / timeWindowSize.duration.toMillis + 1
        )
        .toMillis
    )
    // If we're close to the upper bound, we'll submit the top up to both time windows to ensure low latency
    // We use 20% of the window size as the threshold
    if (
      windowUpperBound - now <= trafficParams.setBalanceRequestSubmissionWindowSize.duration
        .dividedBy(100 / 20)
    ) {
      NonEmpty.mk(Seq, windowUpperBound, windowUpperBound.plus(timeWindowSize.duration))
    } else
      NonEmpty.mk(Seq, windowUpperBound)
  }
}
