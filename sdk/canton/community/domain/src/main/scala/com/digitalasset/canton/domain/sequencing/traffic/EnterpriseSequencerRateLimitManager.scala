// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import cats.data.{EitherT, OptionT}
import cats.instances.future.*
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.caching.CaffeineCache
import com.digitalasset.canton.caching.CaffeineCache.FutureAsyncCacheLoader
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.crypto.{
  DomainSyncCryptoClient,
  Signature,
  SyncCryptoApi,
  SyncCryptoClient,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitError.SequencingCostValidationError
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.{
  SequencerRateLimitError,
  SequencerRateLimitManager,
  SequencerTrafficConfig,
}
import com.digitalasset.canton.domain.sequencing.traffic.EnterpriseSequencerRateLimitManager.*
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficConsumedStore
import com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  Lifecycle,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.SequencedEventValidator
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficConsumedManager.NotEnoughTraffic
import com.digitalasset.canton.sequencing.traffic.*
import com.digitalasset.canton.sequencing.{GroupAddressResolver, TrafficControlParameters}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{MediatorId, Member, ParticipantId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.version.ProtocolVersion
import com.github.benmanes.caffeine.cache as caffeine
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Success

class EnterpriseSequencerRateLimitManager(
    @VisibleForTesting
    private[canton] val trafficPurchasedManager: TrafficPurchasedManager,
    override val trafficConsumedStore: TrafficConsumedStore,
    override protected val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
    metrics: SequencerMetrics,
    domainSyncCryptoApi: DomainSyncCryptoClient,
    protocolVersion: ProtocolVersion,
    trafficConfig: SequencerTrafficConfig,
    sequencerMemberRateLimiterFactory: TrafficConsumedManagerFactory =
      DefaultTrafficConsumedManagerFactory,
    eventCostCalculator: EventCostCalculator,
)(implicit executionContext: ExecutionContext)
    extends SequencerRateLimitManager
    with NamedLogging
    with FlagCloseable {

  private val trafficConsumedPerMember
      : CaffeineCache.AsyncLoadingCaffeineCache[Member, TrafficConsumedManager] = {
    import TraceContext.Implicits.Empty.emptyTraceContext
    new CaffeineCache.AsyncLoadingCaffeineCache(
      caffeine.Caffeine
        .newBuilder()
        // Automatically cleans up inactive members from the cache
        .expireAfterAccess(trafficConfig.trafficConsumedCacheTTL.asJava)
        .maximumSize(trafficConfig.maximumTrafficConsumedCacheSize.value.toLong)
        .buildAsync(
          new FutureAsyncCacheLoader[Member, TrafficConsumedManager](member =>
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
          )
        ),
      metrics.trafficControl.consumedCache,
    )
  }

  private def getOrCreateTrafficConsumedManager(
      member: Member
  ): FutureUnlessShutdown[TrafficConsumedManager] = FutureUnlessShutdown.outcomeF {
    trafficConsumedPerMember.get(member)
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
  ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.outcomeF {
    trafficConsumedStore.store(
      TrafficConsumed.empty(
        member,
        timestamp,
        trafficControlParameters.maxBaseTrafficAmount,
      )
    )
  }

  private def getTrafficPurchased(
      timestamp: CantonTimestamp,
      lastBalanceUpdateTimestamp: Option[CantonTimestamp],
      warnIfApproximate: Boolean,
  )(member: Member)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerRateLimitError.TrafficNotFound, Option[
    TrafficPurchased
  ]] = {
    trafficPurchasedManager
      .getTrafficPurchasedAt(member, timestamp, lastBalanceUpdateTimestamp, warnIfApproximate)
      .leftMap { case TrafficPurchasedManager.TrafficPurchasedAlreadyPruned(member, timestamp) =>
        logger.warn(
          s"Failed to retrieve traffic purchased entry for $member at $timestamp as it was already pruned"
        )
        SequencerRateLimitError.TrafficNotFound(member)
      }
  }

  override def lastKnownBalanceFor(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[TrafficPurchased]] =
    trafficPurchasedManager.getLatestKnownBalance(member)

  override def onClosed(): Unit = {
    Lifecycle.close(trafficPurchasedManager)(logger)
  }
  override def balanceUpdateSubscriber: SequencerTrafficControlSubscriber =
    trafficPurchasedManager.subscription

  override def prune(upToExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[String] = {
    (
      trafficPurchasedManager.prune(upToExclusive),
      trafficConsumedStore.pruneBelowExclusive(upToExclusive),
    ).parMapN { case (purchasePruningResult, consumedPruningResult) =>
      s"$purchasePruningResult\n$consumedPruningResult"
    }
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
  ): EitherT[FutureUnlessShutdown, CostValidationError, Option[ValidCost]] = EitherT {
    (
      FutureUnlessShutdown.pure(request.submissionCost.map(_.cost)),
      computeEventCost(request.batch, topology),
    ).parMapN {
      // Submission cost is present and matches the correct cost, we're golden
      case (Some(submissionCost), Some(validCost @ ValidCost(correctCost, _, _)))
          if submissionCost == correctCost =>
        Right(Some(validCost))
      // Submission contains a cost but it does not match the cost at the topology snapshot
      case (Some(submissionCost), Some(ValidCost(correctCost, parameters, _))) =>
        Left(IncorrectCost(submissionCost, correctCost, topology.timestamp, parameters))
      // Submission contains a cost but traffic control is disabled in the topology snapshot
      case (Some(submissionCost), None) =>
        Left(TrafficControlDisabled(submissionCost, topology.timestamp))
      // Submission does not contain a cost but traffic control is enabled in the topology snapshot
      case (None, Some(ValidCost(correctCost, parameters, _))) =>
        Left(NoCostProvided(correctCost, topology.timestamp, parameters))
      // No cost was provided but traffic control is disabled, all good
      case (None, None) => Right(None)
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
  ): EitherT[FutureUnlessShutdown, SequencerRateLimitError, Unit] = {
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
  }

  override def validateRequestAtSubmissionTime(
      request: SubmissionRequest,
      submissionTimestampO: Option[CantonTimestamp],
      lastSequencedTimestamp: CantonTimestamp,
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
    // a malicious submission with a truly incorrect cost. Either way, we will only let the request through
    // if the first validation (using the current snapshot approximation) passes.
    val topology = domainSyncCryptoApi.currentSnapshotApproximation
    val currentTopologySnapshot = topology.ipsSnapshot

    validateRequest(
      request,
      submissionTimestampO,
      currentTopologySnapshot,
      processingSequencerSignature = None,
      latestSequencerEventTimestamp = None,
      warnIfApproximate = false,
      lastSequencedTimestamp,
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
  ): FutureUnlessShutdown[Option[ValidCost]] = {
    val groups =
      batch.envelopes.flatMap(_.recipients.allRecipients).collect { case g: GroupRecipient => g }
    val result = for {
      parameters <- OptionT(snapshot.trafficControlParameters(protocolVersion))
      groupToMembers <- OptionT
        .liftF(GroupAddressResolver.resolveGroupsToMembers(groups.toSet, snapshot))
        .mapK(FutureUnlessShutdown.outcomeK)
      eventCost = eventCostCalculator.computeEventCost(
        batch,
        parameters.readVsWriteScalingFactor,
        groupToMembers,
        protocolVersion,
      )
    } yield ValidCost(
      eventCost,
      parameters,
      snapshot.timestamp,
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
    * @param latestSequencerEventTimestamp Timestamp of the latest sequencer event timestamp. Only at sequencing time.
    * @param warnIfApproximate Whether to warn if getting approximate topology.
    * @param mostRecentKnownDomainTimestamp Most recent sequenced timestamp this sequencer has knowledge of.
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
      mostRecentKnownDomainTimestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, SequencingCostValidationError, Option[ValidCost]] = {
    val sender = request.sender
    val submittedCost = request.submissionCost.map(_.cost)

    def handleIncorrectCostButValidAtSubmissionTime(
        submissionTimestamp: CantonTimestamp,
        correctCost: NonNegativeLong,
        validCost: Option[ValidCost],
    ): EitherT[FutureUnlessShutdown, SequencingCostValidationError, Option[ValidCost]] = {
      SequencedEventValidator
        .validateTopologyTimestampUS(
          domainSyncCryptoApi,
          submissionTimestamp,
          mostRecentKnownDomainTimestamp,
          latestSequencerEventTimestamp,
          protocolVersion,
          warnIfApproximate = true,
          _.submissionCostTimestampTopologyTolerance,
        )
        .biflatMap(
          err => {
            logger.debug(
              s"Submitted cost $validCost from $sender was incorrect at sequencing time but correct according to the submission topology." +
                s"However the submission timestamp $submissionTimestamp used to compute the cost is outside the" +
                s" tolerance window using $mostRecentKnownDomainTimestamp as the most recent domain timestamp: $err"
            )
            EitherT.leftT(
              SequencerRateLimitError.OutdatedEventCost(
                sender,
                submittedCost,
                submissionTimestamp,
                correctCost,
                validationSnapshot.timestamp,
                // this will be filled in at the end of the processing when we update the traffic consumed, even in case of failure
                Option.empty[TrafficReceipt],
              )
            )
          },
          _ => {
            // Cost is correct using the submission timestamp, so likely traffic parameters changed in the topology
            // which is not necessarily a sign of malicious activity. Because it is within the tolerance window
            // we accept the cost and consume it.
            logger.debug(
              s"Sender $sender submitted an outdated cost ($submittedCost, correct cost at validation time was $correctCost) but correct according" +
                s" to the submission timestamp ($submissionTimestamp) and within the tolerance window" +
                s" from topology at $mostRecentKnownDomainTimestamp," +
                s" given the most recent known sequenced event is at $mostRecentKnownDomainTimestamp"
            )
            EitherT.pure(validCost)
          },
        )
    }

    def handleIncorrectCostWithSubmissionTimestamp(
        submissionTimestamp: CantonTimestamp,
        correctCost: NonNegativeLong,
    ) = for {
      // If the submission time is lower than the sequencing timestamp, retrieve topology for it
      topologyAtSubmissionTime <- EitherT
        .liftF[FutureUnlessShutdown, SequencingCostValidationError, TopologySnapshot](
          SyncCryptoClient
            .getSnapshotForTimestampUS(
              domainSyncCryptoApi,
              submissionTimestamp,
              latestSequencerEventTimestamp,
              protocolVersion,
              warnIfApproximate,
            )
            .map(_.ipsSnapshot)
        )
      costValidation <-
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
              correctCost,
              validationSnapshot.timestamp,
              processingSequencerSignature.map(_.signedBy),
              // this will be filled in at the end of the processing when we update the traffic consumed, even in case of failure
              Option.empty[TrafficReceipt],
            )
          }
      result <- handleIncorrectCostButValidAtSubmissionTime(
        submissionTimestamp,
        correctCost,
        costValidation,
      )
    } yield result

    def handleIncorrectCost(
        correctCost: NonNegativeLong
    ): EitherT[FutureUnlessShutdown, SequencingCostValidationError, Option[ValidCost]] = {
      submissionTimestampO match {
        case Some(submissionTimestamp) =>
          handleIncorrectCostWithSubmissionTimestamp(submissionTimestamp, correctCost)

        // The cost was either not provided or incorrect, but we can't verify the honesty of the sender
        // because they did not specify the timestamp of the topology they used.
        case None =>
          val error = SequencerRateLimitError.IncorrectEventCost.Error(
            sender,
            None,
            submittedCost,
            correctCost,
            validationSnapshot.timestamp,
            processingSequencerSignature.map(_.signedBy),
            // this will be filled in at the end of the processing when we update the traffic consumed, even in case of failure
            Option.empty[TrafficReceipt],
          )
          EitherT.leftT(error)
      }
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
            handleIncorrectCost(incorrect.correctCost)
          // If the cost is not provided, handle it the same as an incorrect cost
          case notProvided: NoCostProvided =>
            handleIncorrectCost(notProvided.correctCost)
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
        EitherT.pure(_),
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
    implicit val senderMetricsContext: MetricsContext = MetricsContext("sender" -> sender.toString)

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
              // Regardless of the outcome, record it into the store
              .thereafterF {
                case Success(Outcome(Right(consumed))) =>
                  trafficConsumedStore.store(consumed)
                case Success(Outcome(Left(aboveTrafficLimit))) =>
                  // Even if above traffic limit, record the current traffic consumed state
                  // (extra traffic consumed won't have changed but the base traffic remainder may have since time advanced)
                  trafficConsumedStore.store(
                    aboveTrafficLimit.trafficState.toTrafficConsumed(sender)
                  )
                // Other failures (shutdown or failed future) are not recoverable so we don't have anything to store
                case _ => Future.unit
              }
              .leftWiden[SequencerRateLimitError]
          } else {
            // If not, we will NOT consume, because we assume the even already has been consumed
            EitherT
              .fromOptionF[Future, SequencerRateLimitError, TrafficConsumed](
                trafficConsumedStore
                  .lookupAt(sender, sequencingTime),
                SequencerRateLimitError.TrafficNotFound(sender),
              )
              .mapK(FutureUnlessShutdown.outcomeK)
          }
      } yield {
        // Here we correctly consumed the traffic, so submitted cost and consumed cost are the same
        trafficConsumed.toTrafficReceipt
      }
    }

    // Whatever happens in terms of traffic consumption we'll want to at
    // least make sure the traffic consumed state is at sequencing timestamp.
    // That includes updating the timestamp itself and the base traffic remainder accumulation that results.
    def ensureTrafficConsumedAtSequencingTime(snapshotAtSequencingTime: TopologySnapshot) = {
      for {
        tcm <- getOrCreateTrafficConsumedManager(sender)
        paramsO <- snapshotAtSequencingTime.trafficControlParameters(protocolVersion)
      } yield paramsO.map(params => tcm.updateAt(sequencingTime, params, logger))
    }

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
        )
          .leftWiden[SequencerRateLimitError]
        // If the validation is successful, consume the cost
        trafficConsumed <- costToBeDeducted.traverse(consumeEvent)
      } yield trafficConsumed

      result
        // Even if we fail, make sure the traffic consumed state is updated with the sequencing time
        .leftFlatMap(err => {
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
        })
    }

    for {
      cryptoApi <- EitherT.liftF(
        SyncCryptoClient.getSnapshotForTimestampUS(
          domainSyncCryptoApi,
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
  ): EitherT[FutureUnlessShutdown, SequencerRateLimitError.TrafficNotFound, TrafficState] = {
    implicit val closeContext: CloseContext = CloseContext(FlagCloseable(logger, timeouts))

    minTimestampO match {
      case Some(minTimestamp) if minTimestamp > trafficConsumed.sequencingTimestamp =>
        for {
          topology <- EitherT.liftF(
            SyncCryptoClient.getSnapshotForTimestampUS(
              domainSyncCryptoApi,
              minTimestamp,
              lastSequencerEventTimestamp,
              protocolVersion,
              warnIfApproximate,
            )
          )
          paramsO <- EitherT.liftF(topology.ipsSnapshot.trafficControlParameters(protocolVersion))
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
  }

  override def getTrafficStateForMemberAt(
      member: Member,
      timestamp: CantonTimestamp,
      lastSequencerEventTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerRateLimitError.TrafficNotFound, Option[
    TrafficState
  ]] = {
    for {
      trafficConsumedO <- EitherT
        .liftF(
          trafficConsumedStore.lookupLatestBeforeInclusiveForMember(member, timestamp)
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      trafficStateO <- trafficConsumedO.traverse(
        getTrafficState(_, member, None, lastSequencerEventTimestamp, warnIfApproximate = true)
      )
    } yield trafficStateO
  }

  override def getStates(
      requestedMembers: Set[Member],
      minTimestamp: Option[CantonTimestamp],
      lastSequencerEventTimestamp: Option[CantonTimestamp],
      warnIfApproximate: Boolean = true,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[Member, Either[String, TrafficState]]] = {
    // If the requested set is empty, get all members from the cache
    val membersToGetTrafficFor = {
      if (requestedMembers.isEmpty) {
        trafficConsumedPerMember.underlying.asMap().keySet().asScala
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

  private sealed trait CostValidationError

  /** The submitted cost did not match the cost computed using the topology at validationTimestamp.
    */
  private final case class IncorrectCost(
      submittedCost: NonNegativeLong,
      correctCost: NonNegativeLong,
      validationTimestamp: CantonTimestamp,
      parameters: TrafficControlParameters,
  ) extends CostValidationError

  /** Traffic control is disabled in the topology at validationTimestamp but a cost was provided in the request.
    */
  private final case class TrafficControlDisabled(
      submittedCost: NonNegativeLong,
      validationTimestamp: CantonTimestamp,
  ) extends CostValidationError

  /** Traffic control is enabled in the topology at validationTimestamp but not cost was provided in the request.
    */
  private final case class NoCostProvided(
      correctCost: NonNegativeLong,
      validationTimestamp: CantonTimestamp,
      parameters: TrafficControlParameters,
  ) extends CostValidationError
}
