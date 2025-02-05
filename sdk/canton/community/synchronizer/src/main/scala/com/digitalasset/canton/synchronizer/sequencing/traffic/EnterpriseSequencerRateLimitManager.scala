// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.traffic

import cats.data.{EitherT, OptionT}
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.crypto.{
  Signature,
  SyncCryptoApi,
  SyncCryptoClient,
  SynchronizerCryptoClient,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  LifeCycle,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.SequencedEventValidator
import com.digitalasset.canton.sequencing.client.SequencedEventValidator.TopologyTimestampAfterSequencingTime
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.*
import com.digitalasset.canton.sequencing.traffic.EventCostCalculator.EventCostDetails
import com.digitalasset.canton.sequencing.traffic.TrafficConsumedManager.NotEnoughTraffic
import com.digitalasset.canton.sequencing.{GroupAddressResolver, TrafficControlParameters}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerRateLimitError.SequencingCostValidationError
import com.digitalasset.canton.synchronizer.sequencer.traffic.{
  SequencerRateLimitError,
  SequencerRateLimitManager,
  SequencerTrafficConfig,
}
import com.digitalasset.canton.synchronizer.sequencing.traffic.EnterpriseSequencerRateLimitManager.*
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.TrafficConsumedStore
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{MediatorId, Member, ParticipantId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class EnterpriseSequencerRateLimitManager(
    @VisibleForTesting
    private[canton] val trafficPurchasedManager: TrafficPurchasedManager,
    override val trafficConsumedStore: TrafficConsumedStore,
    override protected val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
    metrics: SequencerMetrics,
    synchronizerSyncCryptoApi: SynchronizerCryptoClient,
    protocolVersion: ProtocolVersion,
    trafficConfig: SequencerTrafficConfig,
    sequencerMemberRateLimiterFactory: TrafficConsumedManagerFactory =
      DefaultTrafficConsumedManagerFactory,
    eventCostCalculator: EventCostCalculator,
)(implicit executionContext: ExecutionContext)
    extends SequencerRateLimitManager
    with NamedLogging
    with FlagCloseable {

  // We keep traffic consumed records per member to avoid unnecessary db lookups and enable async db writes.
  // We don't use a cache here, as it is not safe to retrieve invalidated member records from the database,
  // as it is only written to the database at the end of the chunk processing together with events
  // to enable high throughput.
  private val trafficConsumedPerMember
      : TrieMap[Member, FutureUnlessShutdown[TrafficConsumedManager]] = TrieMap.empty

  private def getOrCreateTrafficConsumedManager(
      member: Member
  ): FutureUnlessShutdown[TrafficConsumedManager] = {
    import TraceContext.Implicits.Empty.emptyTraceContext
    trafficConsumedPerMember.getOrElseUpdate(
      member,
      performUnlessClosingUSF("getOrCreateTrafficConsumedManager") {
        trafficConsumedStore
          .lookupLast(member)
          .map(lastConsumed =>
            sequencerMemberRateLimiterFactory.create(
              member,
              lastConsumed.getOrElse(TrafficConsumed.init(member)),
              loggerFactory,
              metrics.trafficControl.trafficConsumption,
            )
          )
      },
    )
  }

  // Only participants and mediators are rate limited
  private def isRateLimited(member: Member): Boolean = member match {
    case _: ParticipantId => true
    case _: MediatorId => true
    case _: SequencerId => false
  }

  override def registerNewMemberAt(
      member: Member,
      timestamp: CantonTimestamp,
      trafficControlParameters: TrafficControlParameters,
  )(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Unit] =
    trafficConsumedStore.store(
      Seq(
        TrafficConsumed.empty(
          member,
          timestamp,
          trafficControlParameters.maxBaseTrafficAmount,
        )
      )
    )

  private def getTrafficPurchased(
      timestamp: CantonTimestamp,
      lastBalanceUpdateTimestamp: Option[CantonTimestamp],
      warnIfApproximate: Boolean,
  )(member: Member)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerRateLimitError.TrafficNotFound, Option[
    TrafficPurchased
  ]] =
    trafficPurchasedManager
      .getTrafficPurchasedAt(member, timestamp, lastBalanceUpdateTimestamp, warnIfApproximate)
      .leftMap { case TrafficPurchasedManager.TrafficPurchasedAlreadyPruned(member, timestamp) =>
        logger.warn(
          s"Failed to retrieve traffic purchased entry for $member at $timestamp as it was already pruned"
        )
        SequencerRateLimitError.TrafficNotFound(member)
      }

  override def lastKnownBalanceFor(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[TrafficPurchased]] =
    trafficPurchasedManager.getLatestKnownBalance(member)

  override def onClosed(): Unit =
    LifeCycle.close(trafficPurchasedManager)(logger)
  override def balanceUpdateSubscriber: SequencerTrafficControlSubscriber =
    trafficPurchasedManager.subscription

  override def prune(upToExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[String] =
    (
      trafficPurchasedManager.prune(upToExclusive),
      trafficConsumedStore.pruneBelowExclusive(upToExclusive),
    ).parMapN { case (purchasePruningResult, consumedPruningResult) =>
      s"$purchasePruningResult\n$consumedPruningResult"
    }

  override def balanceKnownUntil: Option[CantonTimestamp] = trafficPurchasedManager.maxTsO

  /** Compute the cost of a request using the provided topology and compare it to the cost provided in the submission
    * request.
    * Returns a Left if there's an inconsistency between the correct traffic cost from the topology and the provided
    * cost in the request.
    * Otherwise return the cost and traffic control parameters used for the validation (if traffic control is enabled, None otherwise)
    * @param request Submission request
    * @param topology Topology to use to validate the cost
    */
  private def validateCostIsCorrect(
      request: SubmissionRequest,
      topology: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CostValidationError, Option[ValidCostWithDetails]] = EitherT {
    (
      FutureUnlessShutdown.pure(request.submissionCost.map(_.cost)),
      computeEventCost(request.batch, topology),
    ).parMapN {
      // Submission contains a cost but traffic control is disabled in the topology snapshot
      case (Some(submissionCost), None) =>
        Left(TrafficControlDisabled(submissionCost, topology.timestamp))
      case (Some(submissionCost), Some(ValidCostWithDetails(parameters, _, _)))
          if !parameters.enforceRateLimiting =>
        Left(TrafficControlDisabled(submissionCost, topology.timestamp))
      // Submission cost is present and matches the correct cost, we're golden
      case (Some(submissionCost), Some(validCost @ ValidCostWithDetails(_, _, correctCostDetails)))
          if submissionCost == correctCostDetails.eventCost =>
        Right(Some(validCost))
      // Submission contains a cost but it does not match the cost at the topology snapshot
      case (
            Some(submissionCost),
            Some(ValidCostWithDetails(parameters, _, correctCostDetails)),
          ) =>
        Left(
          IncorrectCost(
            submissionCost,
            topology.timestamp,
            parameters,
            correctCostDetails,
          )
        )
      // Submission does not contain a cost but traffic control is enabled in the topology snapshot
      case (None, Some(ValidCostWithDetails(parameters, _, correctCostDetails)))
          if correctCostDetails.eventCost != NonNegativeLong.zero =>
        Left(NoCostProvided(correctCostDetails, topology.timestamp, parameters))
      // No cost was provided but traffic control is disabled or correct cost is 0, all good
      case (None, _) => Right(None)
    }
  }

  /** Validate that the sender has enough traffic to send the request.
    * Does NOT consume any traffic.
    * @param sender sender of the request
    * @param lastSequencedTimestamp timestamp of the last known sequenced event
    * @param cost cost of the event
    * @param parameters traffic parameters to use to compute base traffic
    */
  private def validateEnoughTraffic(
      sender: Member,
      lastSequencedTimestamp: CantonTimestamp,
      cost: NonNegativeLong,
      parameters: TrafficControlParameters,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerRateLimitError, Unit] =
    for {
      trafficConsumedManager <- EitherT
        .liftF[FutureUnlessShutdown, SequencerRateLimitError, TrafficConsumedManager](
          getOrCreateTrafficConsumedManager(sender)
        )
      // Get the traffic balance at the timestamp of the last known sequenced event
      trafficPurchased <-
        getTrafficPurchased(lastSequencedTimestamp, None, warnIfApproximate = false)(sender)
          .leftWiden[SequencerRateLimitError]
      trafficConsumed = trafficConsumedManager.getTrafficConsumed
      // It's possible traffic gets consumed for the member between lastSequencedTimestamp is picked, and here,
      // which would make trafficConsumed.sequencingTimestamp > lastSequencedTimestamp
      // This would throw off the base rate computation, so we make sure to pick the max of both timestamp
      // which gives us the most up to date state anyway
      consumeAtTimestamp = trafficConsumed.sequencingTimestamp.max(lastSequencedTimestamp)
      // Check that there's enough traffic available
      _ <- EitherT
        .fromEither[FutureUnlessShutdown]
        .apply[NotEnoughTraffic, Unit](
          trafficConsumed.canConsumeAt(
            parameters,
            cost,
            consumeAtTimestamp,
            trafficPurchased,
            logger,
          )
        )
        .leftMap { case NotEnoughTraffic(member, cost, state) =>
          SequencerRateLimitError.AboveTrafficLimit(member, cost, state)
        }
        .leftWiden[SequencerRateLimitError]
    } yield ()

  override def validateRequestAtSubmissionTime(
      request: SubmissionRequest,
      submissionTimestampO: Option[CantonTimestamp],
      lastSequencedTimestamp: CantonTimestamp,
      lastSequencerEventTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, SequencerRateLimitError, Unit] = if (
    isRateLimited(request.sender)
  ) {
    // At submission time, we use the current snapshot approximation to validate the request cost.
    // Obviously the request has not been sequenced yet so that's the best we can do.
    // The optional submissionTimestamp provided in the request will only be used if the first validation fails
    // It will allow us to differentiate between a benign submission with an outdated submission cost and
    // a malicious submission with a truly incorrect cost.
    // If the submitted cost diverges from the one computed using this snapshot, but is valid using the snapshot
    // the sender claims to have used, and is within the tolerance window, we'll accept the submission.
    // If not we'll reject it.
    val topology = synchronizerSyncCryptoApi.currentSnapshotApproximation
    val currentTopologySnapshot = topology.ipsSnapshot

    validateRequest(
      request,
      submissionTimestampO,
      currentTopologySnapshot,
      processingSequencerSignature = None,
      latestSequencerEventTimestamp = lastSequencerEventTimestamp,
      warnIfApproximate = true,
      lastSequencedTimestamp,
      // At submission time we allow submission timestamps slightly ahead of our current topology
      allowSubmissionTimestampInFuture = true,
    )
      .flatMap {
        // If there's a cost to validate against the current available traffic, do that
        case Some(ValidCost(cost, params, _)) =>
          validateEnoughTraffic(request.sender, lastSequencedTimestamp, cost, params)
        // Otherwise let the request through
        case None => EitherT.pure(())
      }
  } else EitherT.pure(())

  /** Compute the cost of a batch using the provided topology.
    * If traffic control parameters are not set in the topology, return None.
    * Otherwise return the cost and the parameters used for the computation.
    * @param batch batch to compute the cost of
    * @param snapshot topology snapshot to use
    */
  private def computeEventCost(
      batch: Batch[ClosedEnvelope],
      snapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[ValidCostWithDetails]] = {
    val groups =
      batch.envelopes.flatMap(_.recipients.allRecipients).collect { case g: GroupRecipient => g }
    val result = for {
      parameters <- OptionT(snapshot.trafficControlParameters(protocolVersion))
      groupToMembers <- OptionT
        .liftF(GroupAddressResolver.resolveGroupsToMembers(groups.toSet, snapshot))
      eventCostDetails = eventCostCalculator.computeEventCost(
        batch,
        parameters.readVsWriteScalingFactor,
        groupToMembers,
        protocolVersion,
      )
    } yield ValidCostWithDetails(
      parameters,
      snapshot.timestamp,
      eventCostDetails,
    )

    result.value
  }

  /** Validate the submitted cost of a submission request.
    * This method is used for the validation at submission time as we all as sequencing time, albeit with different parameters.
    * Notable, at submission time the topology used for validation is the current snapshot approximation of the sequencer
    * processing the request. At sequencing time, it's the topology at sequencing time.
    * @param request request to be validated
    * @param submissionTimestampO submission timestamp the sender claims to have used to compute the traffic cost
    * @param validationSnapshot validation snapshot to be used
    * @param processingSequencerSignature Optionally, signature of the sequencer that processed the request. This only is set at sequencing time.
    * @param latestSequencerEventTimestamp Timestamp of the latest sequencer event timestamp.
    * @param warnIfApproximate Whether to warn if getting approximate topology.
    * @param mostRecentKnownSynchronizerTimestamp Most recent sequenced timestamp this sequencer has knowledge of.
    *                                       At submission time, this is the last observed sequenced timestamp.
    *                                       At sequencing time, it's the sequencing timestamp of the request being processed.
    * @return An error if the request is invalid, or a pair of (cost, traffic parameters).
    *         They represent the correct cost of the request and associated traffic params to be checked against the traffic state.
    *         Note that at sequencing time, even an invalid request (causing this method to return a left), will still debit traffic cost from the sender.
    */
  private def validateRequest(
      request: SubmissionRequest,
      submissionTimestampO: Option[CantonTimestamp],
      validationSnapshot: TopologySnapshot,
      processingSequencerSignature: Option[Signature],
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      warnIfApproximate: Boolean,
      mostRecentKnownSynchronizerTimestamp: CantonTimestamp,
      allowSubmissionTimestampInFuture: Boolean,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, SequencingCostValidationError, Option[ValidCost]] = {
    val sender = request.sender
    val submittedCost = request.submissionCost.map(_.cost)

    def handleIncorrectCostWithSubmissionTimestamp(
        submissionTimestamp: CantonTimestamp,
        correctCostDetails: EventCostDetails,
    ) = {
      logger.debug(
        s"Submitted cost was not correct for the validation topology at ${validationSnapshot.timestamp} (correct cost: $correctCostDetails). Now checking if the submitted cost is valid for the submission topology at $submissionTimestamp."
      )

      /* Before we keep going, we want to ensure that the submissionTimestamp even makes sense to validate at this point.
       * There are 4 cases:
       *
       *       T1                  T2                     T3                T4
       *        │                   │                      │                 │
       *   ─────┴───────┬───────────┴──────────┬───────────┴─────────┬───────┴────
       *                │                      │                     │
       *          tolerance                validation          tolerance
       *         window start              timestamp           window end
       *
       * validation timestamp = mostRecentKnownSynchronizerTimestamp =
       *    - timestamp of current snapshot approximation at submission time (when receiving the submission request from the sender)
       *    - sequencing timestamp at sequencing time (after ordering)
       * tolerance window start = validation timestamp - submissionCostTimestampTopologyTolerance
       * tolerance window end = validation timestamp + submissionTimestampInFutureTolerance
       *
       * T1 and T4 are outside of the tolerance window (in the past and future respectively):
       *    We reject the submission without even verifying if the cost was correctly computed
       * T2 is before the validation timestamp but within the window: We grab the corresponding topology snapshot
       *    (which we have already because we have topology at least until "validation timestamp"), and check
       *    the cost computation was correct for that snapshot
       * T3 is after the validation timestamp but within the window: We will wait to observe that topology timestamp
       *    and then check the cost computation against it like for T2.
       *    Note that we only allow T3 when receiving the submission request on the submission side, NOT after sequencing.
       *    That's because while it's possible for a sender to be slightly ahead of the receiving sequencer's topology due to lag,
       *    it can't possibly be ahead of the topology at sequencing time.
       */

      for {
        // We call validateTopologyTimestampUS which will accept T2 timestamps only
        topologyAtSubmissionTime <- SequencedEventValidator
          .validateTopologyTimestampUS(
            synchronizerSyncCryptoApi,
            submissionTimestamp,
            mostRecentKnownSynchronizerTimestamp,
            latestSequencerEventTimestamp,
            protocolVersion,
            warnIfApproximate = true,
            _.submissionCostTimestampTopologyTolerance,
          )
          .biflatMap[SequencingCostValidationError, SyncCryptoApi](
            {
              // If it fails because the submission timestamp is after mostRecentKnownSynchronizerTimestamp, we check to see if it could still be a T3
              case TopologyTimestampAfterSequencingTime
                  if allowSubmissionTimestampInFuture && submissionTimestamp < mostRecentKnownSynchronizerTimestamp
                    .plus(trafficConfig.submissionTimestampInFutureTolerance.asJava) =>
                // If so we wait to observe that topology
                EitherT
                  .liftF[FutureUnlessShutdown, SequencingCostValidationError, SyncCryptoApi](
                    SyncCryptoClient
                      .getSnapshotForTimestamp(
                        synchronizerSyncCryptoApi,
                        submissionTimestamp,
                        latestSequencerEventTimestamp,
                        protocolVersion,
                        warnIfApproximate,
                      )
                  )
              // If not we've got a T1 or T4, we fail with OutdatedEventCost
              case err =>
                logger.debug(
                  s"Submitted cost from $sender was incorrect at validation time and the submission timestamp" +
                    s" $submissionTimestamp is outside the allowed tolerance window around the validation timestamp" +
                    s" used by this sequencer $mostRecentKnownSynchronizerTimestamp: $err"
                )
                EitherT.leftT(
                  SequencerRateLimitError.OutdatedEventCost(
                    sender,
                    submittedCost,
                    submissionTimestamp,
                    correctCostDetails.eventCost,
                    mostRecentKnownSynchronizerTimestamp,
                    // this will be filled in at the end of the processing when we update the traffic consumed, even in case of failure
                    Option.empty[TrafficReceipt],
                  )
                )
            },
            // If timestamp validation passes it's a T2, so we can use that snapshot to validate the cost
            topologySnapshot => EitherT.pure(topologySnapshot),
          )
          .map(_.ipsSnapshot)
          .leftWiden[SequencingCostValidationError]
        costValidAtSubmissionTime <-
          validateCostIsCorrect(request, topologyAtSubmissionTime)
            .leftMap { _ =>
              // Cost is incorrect, missing, or wrongly provided, even according to the submission timestamp.
              // At submission time this is a sign of malicious behavior by the sender.
              // At sequencing time this is a sign of malicious behavior by the sender and sequencer: The sender sent a submission with an
              // incorrect cost that failed to be filtered out by the sequencer processing it.
              SequencerRateLimitError.IncorrectEventCost.Error(
                sender,
                Some(submissionTimestamp),
                submittedCost,
                validationSnapshot.timestamp,
                processingSequencerSignature.map(_.signedBy),
                // this will be filled in at the end of the processing when we update the traffic consumed, even in case of failure
                Option.empty[TrafficReceipt],
                correctCostDetails,
              )
            }
            .leftWiden[SequencingCostValidationError]
      } yield {
        // Cost is correct using the submission timestamp, so likely traffic parameters changed in the topology
        // which is not necessarily a sign of malicious activity. Because it is within the tolerance window
        // we accept the cost and consume it.
        logger.debug(
          s"Sender $sender submitted an outdated cost ($submittedCost). The correct cost at validation time was ${correctCostDetails.eventCost})." +
            s" However the submitted cost is correct according" +
            s" to the submission timestamp ($submissionTimestamp) and within the tolerance window" +
            s" from the validation topology at $mostRecentKnownSynchronizerTimestamp," +
            s" given the most recent known sequenced event is at $mostRecentKnownSynchronizerTimestamp"
        )
        costValidAtSubmissionTime.map(_.toValidCost)
      }
    }

    def handleIncorrectCost(
        correctCostDetails: EventCostDetails
    ): EitherT[FutureUnlessShutdown, SequencingCostValidationError, Option[ValidCost]] =
      submissionTimestampO match {
        case Some(submissionTimestamp) =>
          handleIncorrectCostWithSubmissionTimestamp(submissionTimestamp, correctCostDetails)

        // The cost was either not provided or incorrect, but we can't verify the honesty of the sender
        // because they did not specify the timestamp of the topology they used.
        case None =>
          val error = SequencerRateLimitError.IncorrectEventCost.Error(
            sender,
            None,
            submittedCost,
            validationSnapshot.timestamp,
            processingSequencerSignature.map(_.signedBy),
            // this will be filled in at the end of the processing when we update the traffic consumed, even in case of failure
            Option.empty[TrafficReceipt],
            correctCostDetails,
          )
          EitherT.leftT(error)
      }

    validateCostIsCorrect(request, validationSnapshot)
      .biflatMap(
        {
          // If the cost is incorrect but greater than the correct one, we let the submission through, as the sender
          // committed to spend that cost for the submission
          case incorrect: IncorrectCost if incorrect.submittedCost >= incorrect.correctCost =>
            EitherT.pure(
              Some(
                ValidCost(
                  incorrect.submittedCost,
                  incorrect.parameters,
                  incorrect.validationTimestamp,
                )
              )
            )
          // Cost is incorrect relative to the validation snapshot
          // Let's see if it at least makes sense according to the topology the sender claims to have used
          case incorrect: IncorrectCost =>
            handleIncorrectCost(incorrect.correctCostDetails)
          // If the cost is not provided, handle it the same as an incorrect cost
          case notProvided: NoCostProvided =>
            handleIncorrectCost(notProvided.correctCostDetails)
          // A cost was provided by the sender even though traffic control is now disabled on the current topology
          // Could be that this sequencer is behind and hasn't caught up yet with a topology update that enabled traffic control.
          // Even if that's the case since we think here that traffic control is disabled we can let the request through,
          // if it turns out that traffic control was indeed re-enabled, the cost will be validated against it and consumed at sequencing time.
          case trafficDisabledWithCurrentTopology: TrafficControlDisabled =>
            logger.info(
              s"${request.sender} provided a submission cost ${trafficDisabledWithCurrentTopology.submittedCost}," +
                s" however traffic control is disabled in the topology used for validation at ${trafficDisabledWithCurrentTopology.validationTimestamp}."
            )
            EitherT.rightT(None)
        },
        validCostWithDetails => EitherT.pure(validCostWithDetails.map(_.toValidCost)),
      )
  }

  override def validateRequestAndConsumeTraffic(
      request: SubmissionRequest,
      sequencingTime: CantonTimestamp,
      submissionTimestampO: Option[CantonTimestamp],
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      warnIfApproximate: Boolean,
      sequencerSignature: Signature,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): EitherT[
    FutureUnlessShutdown,
    SequencerRateLimitError,
    Option[TrafficReceipt],
  ] = if (isRateLimited(request.sender)) {
    val sender = request.sender
    implicit val memberMetricsContext: MetricsContext = MetricsContext("member" -> sender.toString)

    def consumeEvent(
        validCost: ValidCost
    ): EitherT[FutureUnlessShutdown, SequencerRateLimitError, TrafficReceipt] = {
      val ValidCost(cost, parameters, _) = validCost
      for {
        rateLimiter <- EitherT
          .liftF[
            FutureUnlessShutdown,
            SequencerRateLimitError,
            TrafficConsumedManager,
          ](
            getOrCreateTrafficConsumedManager(sender)
          )
        trafficPurchased <- getTrafficPurchased(
          sequencingTime,
          latestSequencerEventTimestamp,
          warnIfApproximate,
        )(
          sender
        )
          .leftWiden[SequencerRateLimitError]
        currentTrafficConsumed = rateLimiter.getTrafficConsumed
        currentTrafficConsumedTs = currentTrafficConsumed.sequencingTimestamp
        // If the sequencing timestamp is after the current state, go ahead and try to consume
        trafficConsumed <-
          if (currentTrafficConsumedTs < sequencingTime) {
            EitherT
              .fromEither[FutureUnlessShutdown](
                rateLimiter
                  .consumeIfEnoughTraffic(parameters, cost, sequencingTime, trafficPurchased)
              )
              .leftMap(notEnoughTraffic =>
                SequencerRateLimitError.AboveTrafficLimit(notEnoughTraffic)
              )
              .leftWiden[SequencerRateLimitError]
          } else {
            // If not, we will NOT consume, because we assume the event has already been consumed
            // We then fetch the traffic consumed state at the sequencing time from the store
            // This should not really happen, as messages are processed in order
            logger.debug(
              s"Tried to consume traffic at $sequencingTime for $sender, but the traffic consumed state is already at $currentTrafficConsumedTs"
            )
            EitherT
              .fromOptionF[FutureUnlessShutdown, SequencerRateLimitError, TrafficConsumed](
                trafficConsumedStore
                  .lookupAt(sender, sequencingTime),
                SequencerRateLimitError.TrafficNotFound(sender),
              )
          }
      } yield {
        // Here we correctly consumed the traffic, so submitted cost and consumed cost are the same
        trafficConsumed.toTrafficReceipt
      }
    }

    // Whatever happens in terms of traffic consumption we'll want to at
    // least make sure the traffic consumed state is at sequencing timestamp.
    // That includes updating the timestamp itself and the base traffic remainder accumulation that results.
    def ensureTrafficConsumedAtSequencingTime(snapshotAtSequencingTime: TopologySnapshot) =
      for {
        tcm <- getOrCreateTrafficConsumedManager(sender)
        paramsO <- snapshotAtSequencingTime.trafficControlParameters(protocolVersion)
      } yield paramsO.map(params => tcm.updateAt(sequencingTime, params, logger))

    def processWithTopologySnapshot(topologyAtSequencingTime: SyncCryptoApi) = {
      val snapshotAtSequencingTime = topologyAtSequencingTime.ipsSnapshot
      val result = for {
        costToBeDeducted <- validateRequest(
          request,
          submissionTimestampO,
          snapshotAtSequencingTime,
          Some(sequencerSignature),
          latestSequencerEventTimestamp,
          warnIfApproximate,
          sequencingTime,
          // It is not possible for the submitted to have picked a timestamp more recent than the sequencing timestamp
          // right as the event is being processed. So here we don't allow future-dated submission timestamps.
          allowSubmissionTimestampInFuture = false,
        )
          .leftWiden[SequencerRateLimitError]
        // If the validation is successful, consume the cost
        trafficConsumed <- costToBeDeducted.traverse(consumeEvent)
      } yield trafficConsumed

      result
        // Even if we fail, make sure the traffic consumed state is updated with the sequencing time
        .leftFlatMap { err =>
          val errorUpdatedWithTrafficConsumed
              : EitherT[FutureUnlessShutdown, SequencerRateLimitError, SequencerRateLimitError] =
            for {
              trafficReceipt <-
                EitherT
                  .liftF[FutureUnlessShutdown, SequencerRateLimitError, Option[TrafficReceipt]](
                    // Update the traffic consumed at sequencing time, and convert it to a receipt. Cost = 0 because we failed to consume traffic
                    ensureTrafficConsumedAtSequencingTime(snapshotAtSequencingTime)
                      .map(
                        _.map { trafficConsumed =>
                          require(
                            trafficConsumed.lastConsumedCost.unwrap == 0L,
                            "Consumed cost should be zero",
                          )
                          trafficConsumed.toTrafficReceipt
                        }
                      )
                  )
            } yield {
              // Then update the error with the receipt so we can propagate it upstream
              err match {
                case incorrect: SequencerRateLimitError.IncorrectEventCost.Error =>
                  incorrect.copy(trafficReceipt = trafficReceipt)
                case outdated: SequencerRateLimitError.OutdatedEventCost =>
                  outdated.copy(trafficReceipt = trafficReceipt)
                // Traffic not found error is evidence of a bug and will throw an exception later, no need to update
                case notFound: SequencerRateLimitError.TrafficNotFound => notFound
                // The above traffic limit error already contains the updated traffic state,
                // no need to update it again here
                case above: SequencerRateLimitError.AboveTrafficLimit => above
              }
            }
          EitherT[FutureUnlessShutdown, SequencerRateLimitError, Option[TrafficReceipt]](
            errorUpdatedWithTrafficConsumed.merge.map(err => Left(err))
          )
        }
    }

    for {
      cryptoApi <- EitherT.right(
        SyncCryptoClient.getSnapshotForTimestamp(
          synchronizerSyncCryptoApi,
          sequencingTime,
          latestSequencerEventTimestamp,
          protocolVersion,
          warnIfApproximate,
        )
      )
      result <- processWithTopologySnapshot(cryptoApi)
    } yield result
  } else EitherT.pure(None)

  // Get the traffic state for the provided traffic consumed
  // Optionally provide a minTimestamp. If provided, the traffic state returned will be at least at minTimestamp
  // Note that if the currently known traffic consumed state is < minTimestamp, the returned value is an
  // APPROXIMATION of the the traffic at minTimestamp, since its correct value can only be known for certain
  // when the sequencer has sequenced minTimestamp and updated traffic consumption accordingly.
  private def getTrafficState(
      trafficConsumed: TrafficConsumed,
      member: Member,
      minTimestampO: Option[CantonTimestamp],
      lastSequencerEventTimestamp: Option[CantonTimestamp],
      warnIfApproximate: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerRateLimitError.TrafficNotFound, TrafficState] =
    minTimestampO match {
      case Some(minTimestamp) if minTimestamp > trafficConsumed.sequencingTimestamp =>
        for {
          topology <- EitherT.right(
            SyncCryptoClient.getSnapshotForTimestamp(
              synchronizerSyncCryptoApi,
              minTimestamp,
              lastSequencerEventTimestamp,
              protocolVersion,
              warnIfApproximate,
            )
          )
          paramsO <- EitherT.right(topology.ipsSnapshot.trafficControlParameters(protocolVersion))
          updatedBaseTraffic = paramsO
            .map(trafficConsumed.updateTimestamp(minTimestamp, _, logger))
            .getOrElse(trafficConsumed)
          trafficPurchasedO <- getTrafficPurchased(
            minTimestamp,
            lastSequencerEventTimestamp,
            warnIfApproximate,
          )(member)
        } yield updatedBaseTraffic.toTrafficState(trafficPurchasedO)
      case _ =>
        getTrafficPurchased(
          trafficConsumed.sequencingTimestamp,
          lastSequencerEventTimestamp,
          warnIfApproximate,
        )(member).map(trafficConsumed.toTrafficState)
    }

  override def getTrafficStateForMemberAt(
      member: Member,
      timestamp: CantonTimestamp,
      lastSequencerEventTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerRateLimitError.TrafficNotFound, Option[
    TrafficState
  ]] =
    for {
      trafficConsumedO <- EitherT
        .liftF(
          trafficConsumedStore.lookupLatestBeforeInclusiveForMember(member, timestamp)
        )
      trafficStateO <- trafficConsumedO.traverse(
        getTrafficState(
          _,
          member,
          Some(timestamp),
          lastSequencerEventTimestamp,
          warnIfApproximate = true,
        )
      )
    } yield trafficStateO

  override def getStates(
      requestedMembers: Set[Member],
      minTimestamp: Option[CantonTimestamp],
      lastSequencerEventTimestamp: Option[CantonTimestamp],
      warnIfApproximate: Boolean = true,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[Member, Either[String, TrafficState]]] = {
    // If the requested set is empty, get all members from the in-memory state
    val membersToGetTrafficFor = {
      if (requestedMembers.isEmpty) {
        trafficConsumedPerMember.keySet
      } else requestedMembers
    }.toSeq
      .filter(isRateLimited)

    membersToGetTrafficFor.toList
      .parTraverse { member =>
        for {
          tcm <- getOrCreateTrafficConsumedManager(member)
          trafficConsumed = tcm.getTrafficConsumed
          trafficState <- getTrafficState(
            trafficConsumed,
            member,
            minTimestamp,
            lastSequencerEventTimestamp,
            warnIfApproximate,
          ).value
        } yield member -> trafficState.leftMap(_.toString)
      }
      .map(_.toMap)
  }
}

object EnterpriseSequencerRateLimitManager {

  /** Wrapper class for a cost that is has been validated with the provided parameters and can be consumed.
    * Note that the cost might actually be greater than the cost computed with the parameters, because
    * we allow submitted costs >= to the actual cost to be consumed.
    */
  private final case class ValidCost(
      cost: NonNegativeLong,
      parameters: TrafficControlParameters,
      timestamp: CantonTimestamp,
  )

  /** Like ValidCost but with details of the cost computation.
    */
  private final case class ValidCostWithDetails(
      parameters: TrafficControlParameters,
      timestamp: CantonTimestamp,
      details: EventCostDetails,
  ) {
    def toValidCost: ValidCost = ValidCost(details.eventCost, parameters, timestamp)
  }

  private sealed trait CostValidationError

  /** The submitted cost did not match the cost computed using the topology at validationTimestamp.
    */
  private final case class IncorrectCost(
      submittedCost: NonNegativeLong,
      validationTimestamp: CantonTimestamp,
      parameters: TrafficControlParameters,
      correctCostDetails: EventCostDetails,
  ) extends CostValidationError {
    val correctCost = correctCostDetails.eventCost
  }

  /** Traffic control is disabled in the topology at validationTimestamp but a cost was provided in the request.
    */
  private final case class TrafficControlDisabled(
      submittedCost: NonNegativeLong,
      validationTimestamp: CantonTimestamp,
  ) extends CostValidationError

  /** Traffic control is enabled in the topology at validationTimestamp but not cost was provided in the request.
    */
  private final case class NoCostProvided(
      correctCostDetails: EventCostDetails,
      validationTimestamp: CantonTimestamp,
      parameters: TrafficControlParameters,
  ) extends CostValidationError
}
