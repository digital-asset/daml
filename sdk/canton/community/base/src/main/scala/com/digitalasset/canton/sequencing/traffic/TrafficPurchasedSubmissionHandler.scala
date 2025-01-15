// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.traffic

import cats.data.EitherT
import cats.implicits.catsSyntaxAlternativeSeparate
import cats.syntax.parallel.*
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.{
  SynchronizerSnapshotSyncCryptoApi,
  SynchronizerSyncCryptoClient,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  SetTrafficPurchasedMessage,
  SignedProtocolMessage,
}
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.client.{SendCallback, SendResult, SequencerClientSend}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficControlErrors.TrafficControlError
import com.digitalasset.canton.time.{Clock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, FutureUnlessShutdownUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext
import scala.math.Ordered.orderingToOrdered

/** Utility class to send traffic purchased entry requests protocol messages to be sequenced.
  * This is abstracted out so that it can be re-used in any node's Admin API.
  */
class TrafficPurchasedSubmissionHandler(
    clock: Clock,
    override protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  /** Send a signed traffic purchased entry request.
    * @param member recipient of the new balance
    * @param synchronizerId synchronizerId of the synchronizer where the top up is being sent to
    * @param protocolVersion protocol version used
    * @param serial monotonically increasing serial number for the request
    * @param totalTrafficPurchased new total traffic purchased entry
    * @param sequencerClient sequencer client to use to send the balance request
    * @param cryptoApi crypto api used to access topology
    */
  def sendTrafficPurchasedRequest(
      member: Member,
      synchronizerId: SynchronizerId,
      protocolVersion: ProtocolVersion,
      serial: PositiveInt,
      totalTrafficPurchased: NonNegativeLong,
      sequencerClient: SequencerClientSend,
      synchronizerTimeTracker: SynchronizerTimeTracker,
      cryptoApi: SynchronizerSyncCryptoClient,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, TrafficControlError, Unit] = {
    val topology: SynchronizerSnapshotSyncCryptoApi = cryptoApi.currentSnapshotApproximation
    val snapshot = topology.ipsSnapshot

    def send(
        maxSequencingTimes: NonEmpty[Seq[CantonTimestamp]],
        batch: Batch[OpenEnvelope[SignedProtocolMessage[SetTrafficPurchasedMessage]]],
        aggregationRule: AggregationRule,
    ): EitherT[FutureUnlessShutdown, TrafficControlError, Unit] = {
      // We don't simply `parTraverse` over `maxSequencingTimes` because as long as at least one request was
      // successfully sent, errors (such as max sequencing time already expired) should not result in a failure.

      val fut = for {
        resultsE <- maxSequencingTimes.forgetNE.parTraverse { maxSequencingTime =>
          logger.debug(
            s"Submitting traffic purchased entry request for $member with balance ${totalTrafficPurchased.value}, serial ${serial.value} and max sequencing time $maxSequencingTime"
          )
          sendRequest(
            sequencerClient,
            synchronizerTimeTracker,
            batch,
            aggregationRule,
            maxSequencingTime,
          ).value
        }
        (errors, successes) = resultsE.separate
      } yield {
        Either.cond(
          successes.nonEmpty,
          (),
          errors.headOption.getOrElse(
            // This should never happen because `maxSequencingTimes` is non-empty
            ErrorUtil.invalidState(
              "No error or success for a non-empty list of max-sequencing-time"
            )
          ),
        )
      }
      EitherT(fut)
    }

    for {
      trafficParams <- EitherT
        .fromOptionF(
          snapshot.trafficControlParameters(protocolVersion),
          TrafficControlErrors.TrafficControlDisabled.Error(),
        )
      sequencerGroup <- EitherT
        .liftF(
          snapshot
            .sequencerGroup()
            .map(
              _.getOrElse(
                ErrorUtil.invalidState(
                  "No sequencer group was found on the synchronizer. There should at least be one sequencer (this one)."
                )
              )
            )
        )
      activeSequencers <- EitherT.fromOption[FutureUnlessShutdown](
        NonEmpty
          .from(sequencerGroup.active.map(_.member)),
        ErrorUtil.invalidState(
          "No active sequencers found on the synchronizer. There should at least be one sequencer."
        ),
      )
      aggregationRule = AggregationRule(
        eligibleMembers = activeSequencers,
        threshold = sequencerGroup.threshold,
        protocolVersion,
      )
      setTrafficPurchasedMessage = SetTrafficPurchasedMessage(
        member,
        serial,
        totalTrafficPurchased,
        synchronizerId,
        protocolVersion,
      )
      signedTrafficPurchasedMessage <- EitherT
        .liftF(
          SignedProtocolMessage.trySignAndCreate(
            setTrafficPurchasedMessage,
            topology,
            protocolVersion,
          )
        )
      batch = Batch.of(
        protocolVersion = protocolVersion,
        // This recipient tree structure allows the recipient of the top up to verify that the sequencers were also addressed
        signedTrafficPurchasedMessage -> Recipients(
          NonEmpty.mk(
            Seq,
            RecipientsTree.ofMembers(
              NonEmpty.mk(Set, member), // Root of recipient tree: recipient of the top up
              Seq(
                RecipientsTree.recipientsLeaf( // Leaf of the tree: sequencers of synchronizer group
                  NonEmpty.mk(
                    Set,
                    SequencersOfSynchronizer: Recipient,
                  )
                )
              ),
            ),
          )
        ),
      )
      maxSequencingTimes = computeMaxSequencingTimes(trafficParams)
      _ <- send(maxSequencingTimes, batch, aggregationRule)
    } yield ()
  }

  private def sendRequest(
      sequencerClient: SequencerClientSend,
      synchronizerTimeTracker: SynchronizerTimeTracker,
      batch: Batch[DefaultOpenEnvelope],
      aggregationRule: AggregationRule,
      maxSequencingTime: CantonTimestamp,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, TrafficControlError, Unit] = {
    val callback = SendCallback.future
    implicit val metricsContext: MetricsContext = MetricsContext("type" -> "traffic-purchase")
    // Make sure that the sequencer will ask for a time proof if it doesn't observe the sequencing in time
    synchronizerTimeTracker.requestTick(maxSequencingTime)
    for {
      _ <- sequencerClient
        .sendAsync(
          batch,
          aggregationRule = Some(aggregationRule),
          maxSequencingTime = maxSequencingTime,
          callback = callback,
        )
        .leftMap(err =>
          TrafficControlErrors.TrafficPurchasedRequestAsyncSendFailed
            .Error(err.show): TrafficControlError
        )
    } yield {
      val logOutcomeF = callback.future.map {
        case SendResult.Success(_) =>
          logger.debug(
            s"Traffic balance request with max sequencing time $maxSequencingTime successfully submitted"
          )
        case SendResult.Error(
              DeliverError(
                _,
                _,
                _,
                _,
                SequencerErrors.AggregateSubmissionAlreadySent(message),
                _,
              )
            ) =>
          logger.info(s"The top-up request was already sent: $message")
        case SendResult.Error(err) =>
          logger.info(show"The traffic balance request submission failed: $err")
        case SendResult.Timeout(time) =>
          logger.warn(
            show"The traffic balance request submission timed out after sequencing time $time has elapsed"
          )
      }
      FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
        logOutcomeF,
        "Traffic balance request submission failed",
      )
    }
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
    // We use 25% of the window size as the threshold
    if (windowUpperBound - now <= timeWindowSize.duration.dividedBy(100 / 25)) {
      NonEmpty.mk(Seq, windowUpperBound, windowUpperBound.plus(timeWindowSize.duration))
    } else
      NonEmpty.mk(Seq, windowUpperBound)
  }
}
