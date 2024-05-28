// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.traffic

import cats.data.EitherT
import cats.implicits.catsSyntaxAlternativeSeparate
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
  SetTrafficPurchasedMessage,
  SignedProtocolMessage,
}
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.client.{SendCallback, SendResult, SequencerClientSend}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext
import scala.math.Ordered.orderingToOrdered

import TrafficControlErrors.TrafficControlError

/** Utility class to send traffic purchased entry requests protocol messages to be sequenced.
  * This is abstracted out so that it can be re-used in any node's Admin API.
  */
class TrafficPurchasedSubmissionHandler(
    clock: Clock,
    override protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  /** Send a signed traffic purchased entry request.
    * @param member recipient of the new balance
    * @param domainId domainId of the domain where the top up is being sent to
    * @param protocolVersion protocol version used
    * @param serial monotonically increasing serial number for the request
    * @param totalTrafficPurchased new total traffic purchased entry
    * @param sequencerClient sequencer client to use to send the balance request
    * @param cryptoApi crypto api used to access topology
    */
  def sendTrafficPurchasedRequest(
      member: Member,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      serial: PositiveInt,
      totalTrafficPurchased: NonNegativeLong,
      sequencerClient: SequencerClientSend,
      cryptoApi: DomainSyncCryptoClient,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, TrafficControlError, CantonTimestamp] = {
    val topology: DomainSnapshotSyncCryptoApi = cryptoApi.currentSnapshotApproximation
    val snapshot = topology.ipsSnapshot

    def send(
        maxSequencingTimes: NonEmpty[Seq[CantonTimestamp]],
        batch: Batch[OpenEnvelope[SignedProtocolMessage[SetTrafficPurchasedMessage]]],
        aggregationRule: AggregationRule,
    ): EitherT[FutureUnlessShutdown, TrafficControlError, CantonTimestamp] = {
      // We don't simply `parTraverse` over `maxSequencingTimes` because as long as at least one request was
      // successfully sent, errors (such as max sequencing time already expired) should not result in a failure.

      val fut = for {
        resultsE <- maxSequencingTimes.forgetNE.parTraverse { maxSequencingTime =>
          logger.debug(
            s"Submitting traffic purchased entry request for $member with balance ${totalTrafficPurchased.value}, serial ${serial.value} and max sequencing time $maxSequencingTime"
          )
          sendRequest(sequencerClient, batch, aggregationRule, maxSequencingTime).value
        }
        (errors, successes) = resultsE.separate
      } yield (NonEmpty.from(errors), NonEmpty.from(successes)) match {
        case (None, None) =>
          // This should never happen because `maxSequencingTimes` is non-empty
          throw new IllegalStateException(
            "No error or success for a non-empty list of max-sequencing-time"
          )

        case (Some(errorsNE), None) =>
          // None of the requests was successfully sent -- return the first error
          Left(errorsNE.head1)

        case (_, Some(successesNE)) =>
          // At least one of the requests was successfully sent -- return the latest max-sequencing-time
          Right(successesNE.last1)
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
      setTrafficPurchasedMessage = SetTrafficPurchasedMessage(
        member,
        serial,
        totalTrafficPurchased,
        domainId,
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
      latestMaxSequencingTime <- send(maxSequencingTimes, batch, aggregationRule)
    } yield latestMaxSequencingTime
  }

  private def sendRequest(
      sequencerClient: SequencerClientSend,
      batch: Batch[DefaultOpenEnvelope],
      aggregationRule: AggregationRule,
      maxSequencingTime: CantonTimestamp,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, TrafficControlError, CantonTimestamp] = {
    val callback = SendCallback.future
    for {
      _ <- sequencerClient
        .sendAsync(
          batch,
          aggregationRule = Some(aggregationRule),
          maxSequencingTime = maxSequencingTime,
          callback = callback,
        )
        .leftMap(err => TrafficControlErrors.TrafficPurchasedRequestAsyncSendFailed.Error(err.show))
        .leftWiden[TrafficControlError]
      _ <- EitherT(
        callback.future
          .map {
            case SendResult.Success(_) =>
              logger.debug(
                s"Traffic balance request with max sequencing time $maxSequencingTime successfully submitted"
              )
              Right(())
            case SendResult.Error(
                  DeliverError(_, _, _, _, SequencerErrors.AggregateSubmissionAlreadySent(message))
                ) =>
              logger.info(s"The top-up request was already sent: $message")
              Right(())
            case SendResult.Error(err) =>
              Left(TrafficControlErrors.TrafficPurchasedRequestAsyncSendFailed.Error(err.show))
            case SendResult.Timeout(time) =>
              Left(
                TrafficControlErrors.TrafficPurchasedRequestAsyncSendFailed.Error(
                  s"Submission timed out after sequencing time $time has elapsed"
                )
              )
          }
      ).leftWiden[TrafficControlError]
    } yield maxSequencingTime
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
    if (
      windowUpperBound - now <= trafficParams.setBalanceRequestSubmissionWindowSize.duration
        .dividedBy(100 / 25)
    ) {
      NonEmpty.mk(Seq, windowUpperBound, windowUpperBound.plus(timeWindowSize.duration))
    } else
      NonEmpty.mk(Seq, windowUpperBound)
  }
}
