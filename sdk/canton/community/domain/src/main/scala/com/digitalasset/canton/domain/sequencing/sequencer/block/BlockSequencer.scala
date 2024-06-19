// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, HashPurpose, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v30.TrafficControlErrorReason
import com.digitalasset.canton.domain.block.BlockSequencerStateManagerBase
import com.digitalasset.canton.domain.block.data.SequencerBlockStore
import com.digitalasset.canton.domain.block.update.{BlockUpdateGeneratorImpl, LocalBlockUpdate}
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.PruningError.UnsafePruningPoint
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer.{
  EventSource,
  SignedOrderingRequest,
}
import com.digitalasset.canton.domain.sequencing.sequencer.*
import com.digitalasset.canton.domain.sequencing.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.domain.sequencing.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.TimestampSelector.{
  ExactTimestamp,
  TimestampSelector,
  *,
}
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.{
  SequencerRateLimitError,
  SequencerRateLimitManager,
  SequencerTrafficStatus,
}
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficPurchasedStore
import com.digitalasset.canton.health.admin.data.SequencerHealthStatus
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.pretty.CantonPrettyPrinter
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.scheduler.PruningScheduler
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficControlErrors.TrafficControlError
import com.digitalasset.canton.sequencing.traffic.{
  TrafficConsumed,
  TrafficControlErrors,
  TrafficPurchased,
  TrafficPurchasedSubmissionHandler,
}
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.util.{EitherTUtil, PekkoUtil, SimpleExecutionQueue}
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.ServerServiceDefinition
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{Keep, Merge, Sink, Source}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.*
import scala.util.{Failure, Success}

class BlockSequencer(
    blockOrderer: BlockOrderer,
    name: String,
    domainId: DomainId,
    cryptoApi: DomainSyncCryptoClient,
    sequencerId: SequencerId,
    stateManager: BlockSequencerStateManagerBase,
    store: SequencerBlockStore,
    trafficPurchasedStore: TrafficPurchasedStore,
    storage: Storage,
    futureSupervisor: FutureSupervisor,
    health: Option[SequencerHealthConfig],
    clock: Clock,
    protocolVersion: ProtocolVersion,
    blockRateLimitManager: SequencerRateLimitManager,
    orderingTimeFixMode: OrderingTimeFixMode,
    processingTimeouts: ProcessingTimeout,
    logEventDetails: Boolean,
    prettyPrinter: CantonPrettyPrinter,
    metrics: SequencerMetrics,
    loggerFactory: NamedLoggerFactory,
    unifiedSequencer: Boolean,
)(implicit executionContext: ExecutionContext, materializer: Materializer, tracer: Tracer)
    extends DatabaseSequencer(
      SequencerWriterStoreFactory.singleInstance,
      // TODO(#18407): Allow partial configuration of DBS as a part of unified sequencer
      DatabaseSequencerConfig.ForBlockSequencer(),
      None,
      TotalNodeCountValues.SingleSequencerTotalNodeCount,
      new LocalSequencerStateEventSignaller(
        processingTimeouts,
        loggerFactory,
      ),
      None,
      // TODO(#18407): Dummy config which will be ignored anyway as `config.highAvailabilityEnabled` is false
      OnlineSequencerCheckConfig(),
      processingTimeouts,
      storage,
      None,
      health,
      clock,
      domainId,
      sequencerId,
      protocolVersion,
      cryptoApi,
      SequencerMetrics.noop("TODO"), // TODO(#18406)
      loggerFactory,
      unifiedSequencer,
    )
    with DatabaseSequencerIntegration
    with NamedLogging
    with FlagCloseableAsync {

  private[sequencer] val pruningQueue = new SimpleExecutionQueue(
    "block-sequencer-pruning-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  override lazy val rateLimitManager: Option[SequencerRateLimitManager] = Some(
    blockRateLimitManager
  )

  private val trafficPurchasedSubmissionHandler =
    new TrafficPurchasedSubmissionHandler(clock, loggerFactory)
  override private[sequencing] def firstSequencerCounterServeableForSequencer: SequencerCounter =
    stateManager.firstSequencerCounterServableForSequencer

  private val (killSwitch, localEventsQueue, done) = {
    val headState = stateManager.getHeadState
    noTracingLogger.info(s"Subscribing to block source from ${headState.block.height}")

    val updateGenerator = new BlockUpdateGeneratorImpl(
      domainId,
      protocolVersion,
      cryptoApi,
      sequencerId,
      stateManager.maybeLowerTopologyTimestampBound,
      blockRateLimitManager,
      orderingTimeFixMode,
      metrics,
      loggerFactory,
      unifiedSequencer = unifiedSequencer,
      memberValidator = memberValidator,
    )(CloseContext(cryptoApi))

    val driverSource = blockOrderer
      .subscribe()(TraceContext.empty)
      // Explicit async to make sure that the block processing runs in parallel with the block retrieval
      .async
      .map(updateGenerator.extractBlockEvents)
      .via(stateManager.processBlock(updateGenerator))

    val localSource = Source
      .queue[Traced[BlockSequencer.LocalEvent]](bufferSize = 1000, OverflowStrategy.backpressure)
      .map(_.map(event => LocalBlockUpdate(event)))
    val combinedSource = Source.combineMat(driverSource, localSource)(Merge(_))(Keep.both)
    val sequencerIntegration = if (unifiedSequencer) {
      this
    } else {
      SequencerIntegration.Noop
    }
    val combinedSourceWithBlockHandling = combinedSource.async
      .via(stateManager.applyBlockUpdate(sequencerIntegration))
      .map { case Traced(lastTs) =>
        metrics.sequencerClient.handler.delay.updateValue((clock.now - lastTs).toMillis)
      }
    val ((killSwitch, localEventsQueue), done) = PekkoUtil.runSupervised(
      ex => logger.error("Fatally failed to handle state changes", ex)(TraceContext.empty), {
        combinedSourceWithBlockHandling.toMat(Sink.ignore)(Keep.both)
      },
    )
    (killSwitch, localEventsQueue, done)
  }

  done onComplete {
    case Success(_) => noTracingLogger.debug("Sequencer flow has shutdown")
    case Failure(ex) => noTracingLogger.error("Sequencer flow has failed", ex)
  }

  private def validateMaxSequencingTime(
      submission: SubmissionRequest
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncError, Unit] = {
    val estimatedSequencingTimestamp = clock.now
    submission.aggregationRule match {
      case Some(_) =>
        for {
          _ <- EitherTUtil.condUnitET[Future](
            submission.maxSequencingTime > estimatedSequencingTimestamp,
            SendAsyncError.RequestInvalid(
              s"The sequencer clock timestamp $estimatedSequencingTimestamp is already past the max sequencing time ${submission.maxSequencingTime} for submission with id ${submission.messageId}"
            ),
          )
          // We can't easily use snapshot(topologyTimestamp), because the effective last snapshot transaction
          // visible in the BlockSequencer can be behind the topologyTimestamp and tracking that there's an
          // intermediate topology change is impossible here (will need to communicate with the BlockUpdateGenerator).
          // If topologyTimestamp happens to be ahead of current topology's timestamp we grab the latter
          // to prevent a deadlock.
          topologyTimestamp = cryptoApi.approximateTimestamp.min(
            submission.topologyTimestamp.getOrElse(CantonTimestamp.MaxValue)
          )
          snapshot <- EitherT.right(cryptoApi.snapshot(topologyTimestamp))
          domainParameters <- EitherT(
            snapshot.ipsSnapshot.findDynamicDomainParameters()
          )
            .leftMap(error =>
              SendAsyncError.Internal(s"Could not fetch dynamic domain parameters: $error")
            )
          maxSequencingTimeUpperBound = estimatedSequencingTimestamp.add(
            domainParameters.parameters.sequencerAggregateSubmissionTimeout.duration
          )
          _ <- EitherTUtil.condUnitET[Future](
            submission.maxSequencingTime < maxSequencingTimeUpperBound,
            SendAsyncError.RequestInvalid(
              s"Max sequencing time ${submission.maxSequencingTime} for submission with id ${submission.messageId} is too far in the future, currently bounded at $maxSequencingTimeUpperBound"
            ): SendAsyncError,
          )
        } yield ()
      case None => EitherT.right[SendAsyncError](Future.unit)
    }
  }

  override protected def sendAsyncInternal(
      submission: SubmissionRequest
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SendAsyncError, Unit] = {
    val signedContent = SignedContent(submission, Signature.noSignature, None, protocolVersion)
    sendAsyncSignedInternal(signedContent)
  }

  override def adminServices: Seq[ServerServiceDefinition] = blockOrderer.adminServices

  private def signOrderingRequest[A <: HasCryptographicEvidence](
      content: SignedContent[SubmissionRequest]
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncError.Internal, SignedOrderingRequest] = {
    val privateCrypto = cryptoApi.currentSnapshotApproximation
    for {
      signed <- SignedContent
        .create(
          cryptoApi.pureCrypto,
          privateCrypto,
          content,
          Some(privateCrypto.ipsSnapshot.timestamp),
          HashPurpose.OrderingRequestSignature,
          protocolVersion,
        )
        .leftMap(error => SendAsyncError.Internal(s"Could not sign ordering request: $error"))
    } yield signed
  }

  private def enforceRateLimiting(
      request: SignedContent[SubmissionRequest]
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, SendAsyncError, Unit] = {
    blockRateLimitManager
      .validateRequestAtSubmissionTime(
        request.content,
        request.timestampOfSigningKey,
        stateManager.getHeadState.block.lastTs,
      )
      .leftMap {
        // If the cost is outdated, we bounce the request with a specific SendAsyncError so the
        // sender has the required information to retry the request with the correct cost
        case notEnoughTraffic: SequencerRateLimitError.AboveTrafficLimit =>
          logger.debug(
            s"Rejecting submission request because not enough traffic is available: $notEnoughTraffic"
          )
          SendAsyncError.TrafficControlError(
            TrafficControlErrorReason.Error(
              TrafficControlErrorReason.Error.Reason.InsufficientTraffic(
                s"Submission was rejected because not traffic is available: $notEnoughTraffic"
              )
            )
          ): SendAsyncError
        // If the cost is outdated, we bounce the request with a specific SendAsyncError so the
        // sender has the required information to retry the request with the correct cost
        case outdated: SequencerRateLimitError.OutdatedEventCost =>
          logger.debug(
            s"Rejecting submission request because the cost was computed using an outdated topology: $outdated"
          )
          SendAsyncError.TrafficControlError(
            TrafficControlErrorReason.Error(
              TrafficControlErrorReason.Error.Reason.InsufficientTraffic(
                s"Submission was refused because traffic cost was outdated. Re-submit after the having observed the validation timestamp and processed its topology state: $outdated"
              )
            )
          ): SendAsyncError
        case error =>
          SendAsyncError.RequestRefused(
            s"Submission was refused because traffic control validation failed: $error"
          ): SendAsyncError
      }
  }

  override protected def sendAsyncSignedInternal(
      signedSubmission: SignedContent[SubmissionRequest]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SendAsyncError, Unit] = {
    val submission = signedSubmission.content
    val SubmissionRequest(
      sender,
      _,
      batch,
      maxSequencingTime,
      _,
      _aggregationRule,
      _submissionCost,
    ) = submission
    logger.debug(
      s"Request to send submission with id ${submission.messageId} with max sequencing time $maxSequencingTime from $sender to ${batch.allRecipients}"
    )

    for {
      // TODO(i17584): revisit the consequences of no longer enforcing that
      //  aggregated submissions with signed envelopes define a topology snapshot
      _ <- validateMaxSequencingTime(submission).mapK(FutureUnlessShutdown.outcomeK)
      memberCheck <- EitherT
        .right[SendAsyncError](
          // Using currentSnapshotApproximation due to members registration date
          // expected to be before submission sequencing time
          cryptoApi.currentSnapshotApproximation.ipsSnapshot
            .allMembers()
            .map(allMembers => (member: Member) => allMembers.contains(member))
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      // TODO(#19476): Why we don't check group recipients here?
      _ <- SequencerValidations
        .checkSenderAndRecipientsAreRegistered(
          submission,
          memberCheck,
        )
        .toEitherT[FutureUnlessShutdown]
      _ = if (logEventDetails)
        logger.debug(
          s"Invoking send operation on the ledger with the following protobuf message serialized to bytes ${prettyPrinter
              .printAdHoc(submission.toProtoVersioned)}"
        )
      signedOrderingRequest <- signOrderingRequest(signedSubmission)
      _ <- enforceRateLimiting(signedSubmission)
      _ <-
        EitherT(
          futureSupervisor
            .supervised(
              s"Sending submission request with id ${submission.messageId} from $sender to ${batch.allRecipients}"
            )(
              blockOrderer.send(signedOrderingRequest).value
            )
        ).mapK(FutureUnlessShutdown.outcomeK)
    } yield ()
  }

  override def readInternal(member: Member, offset: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CreateSubscriptionError, EventSource] = {
    logger.debug(s"Answering readInternal(member = $member, offset = $offset)")
    if (unifiedSequencer) {
      super.readInternal(member, offset)
    } else {
      EitherT
        .right(cryptoApi.currentSnapshotApproximation.ipsSnapshot.isMemberKnown(member))
        .flatMap { isKnown =>
          if (isKnown) {
            EitherT.fromEither[Future](stateManager.readEventsForMember(member, offset))
          } else {
            EitherT.leftT(CreateSubscriptionError.UnknownMember(member))
          }
        }
    }
  }

  override def isRegistered(
      member: Member
  )(implicit traceContext: TraceContext): Future[Boolean] = {
    if (unifiedSequencer) {
      super.isRegistered(member)
    } else {
      cryptoApi.headSnapshot.ipsSnapshot.isMemberKnown(member)
    }
  }

  /** Important: currently both the disable member and the prune functionality on the block sequencer are
    * purely local operations that do not affect other block sequencers that share the same source of
    * events.
    */
  override protected def disableMemberInternal(
      member: Member
  )(implicit traceContext: TraceContext): Future[Unit] = {
    if (!unifiedSequencer) {
      if (!stateManager.isMemberRegistered(member)) {
        logger.warn(s"disableMember attempted to use member [$member] but they are not registered")
        Future.unit
      } else if (!stateManager.isMemberEnabled(member)) {
        logger.debug(
          s"disableMember attempted to use member [$member] but they are already disabled"
        )
        Future.unit
      } else {
        val disabledF =
          futureSupervisor.supervised(s"Waiting for member $member to be disabled")(
            stateManager.waitForMemberToBeDisabled(member)
          )
        for {
          _ <- placeLocalEvent(BlockSequencer.DisableMember(member))
          _ <- disabledF
          _ <- super.disableMemberInternal(
            member
          ) // Now members are also always stored in DBS, so we disable there as well
        } yield ()
      }
    } else {
      super.disableMemberInternal(member)
    }
  }

  override protected def localSequencerMember: Member = sequencerId

  override protected def acknowledgeSignedInternal(
      signedAcknowledgeRequest: SignedContent[AcknowledgeRequest]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val req = signedAcknowledgeRequest.content
    logger.debug(s"Request for member ${req.member} to acknowledge timestamp ${req.timestamp}")
    val waitForAcknowledgementF =
      stateManager.waitForAcknowledgementToComplete(req.member, req.timestamp)
    for {
      _ <- blockOrderer.acknowledge(signedAcknowledgeRequest)
      _ <- waitForAcknowledgementF
    } yield ()
  }

  override def snapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[Future, String, SequencerSnapshot] = {
    // TODO(#12676) Make sure that we don't request a snapshot for a state that was already pruned

    for {
      bsSnapshot <- store
        .readStateForBlockContainingTimestamp(timestamp)
        .biflatMap(
          _ =>
            EitherT.leftT[Future, SequencerSnapshot](
              s"Provided timestamp $timestamp is not linked to a block"
            ),
          blockEphemeralState => {
            for {
              // Look up traffic info at the latest timestamp from the block,
              // because that's where the onboarded sequencer will start reading
              trafficPurchased <- EitherT.liftF[Future, String, Seq[TrafficPurchased]](
                trafficPurchasedStore
                  .lookupLatestBeforeInclusive(blockEphemeralState.latestBlock.lastTs)
              )
              trafficConsumed <- EitherT.liftF[Future, String, Seq[TrafficConsumed]](
                blockRateLimitManager.trafficConsumedStore
                  .lookupLatestBeforeInclusive(blockEphemeralState.latestBlock.lastTs)
              )
            } yield blockEphemeralState
              .toSequencerSnapshot(protocolVersion, trafficPurchased, trafficConsumed)
              .tap(snapshot =>
                if (logger.underlying.isDebugEnabled()) {
                  logger.debug(
                    s"Snapshot for timestamp $timestamp generated from ephemeral state:\n$blockEphemeralState"
                  )
                  logger.debug(
                    s"Resulting snapshot for timestamp $timestamp:\n$snapshot"
                  )
                }
              )
          },
        )
      finalSnapshot <- {
        if (unifiedSequencer) {
          super.snapshot(timestamp).map { dbsSnapshot =>
            dbsSnapshot.copy(
              inFlightAggregations = bsSnapshot.inFlightAggregations,
              additional = bsSnapshot.additional,
            )(dbsSnapshot.representativeProtocolVersion)
          }
        } else {
          EitherT.pure[Future, String](bsSnapshot)
        }
      }
    } yield finalSnapshot
  }

  override def pruningStatus(implicit
      traceContext: TraceContext
  ): Future[SequencerPruningStatus] = {
    if (unifiedSequencer) {
      for {
        blockPruningStatus <- store
          .pruningStatus()
          .map(_.toSequencerPruningStatus(clock.now))
        dbsStatus <- super[DatabaseSequencer].pruningStatus
      } yield {
        dbsStatus.copy(
          lowerBound = dbsStatus.lowerBound.min(blockPruningStatus.lowerBound),
          // TODO(#19526): Combine pruning statuses?
          members = dbsStatus.members ++ blockPruningStatus.members,
        )
      }
    } else {
      store.pruningStatus().map(_.toSequencerPruningStatus(clock.now))
    }
  }

  /** Important: currently both the disable member and the prune functionality on the block sequencer are
    * purely local operations that do not affect other block sequencers that share the same source of
    * events.
    */
  override def prune(requestedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, PruningError, String] = {

    val (isNew, pruningF) = stateManager.waitForPruningToComplete(requestedTimestamp)
    val supervisedPruningF = futureSupervisor.supervised(
      s"Waiting for local pruning operation at $requestedTimestamp to complete"
    )(pruningF)

    if (isNew)
      for {
        status <- EitherT.right[PruningError](this.pruningStatus)
        _ <- condUnitET[Future](
          requestedTimestamp <= status.safePruningTimestamp,
          UnsafePruningPoint(requestedTimestamp, status.safePruningTimestamp): PruningError,
        )
        msg <- EitherT.right(
          pruningQueue
            .execute(
              for {
                eventsMsg <- store.prune(requestedTimestamp)
                trafficMsg <- blockRateLimitManager.prune(requestedTimestamp)
              } yield s"$eventsMsg\n$trafficMsg",
              s"pruning sequencer at $requestedTimestamp",
            )
            .unwrap
            .map(
              _.onShutdown(s"pruning at $requestedTimestamp canceled because we're shutting down")
            )
        )
        _ <- EitherT.right(
          placeLocalEvent(BlockSequencer.UpdateInitialMemberCounters(requestedTimestamp))
        )
        _ <- EitherT.right(supervisedPruningF)
        dbsMsg <-
          if (unifiedSequencer) {
            // TODO(#19526): Move in together within the pruningQueue.execute above
            super[DatabaseSequencer].prune(requestedTimestamp)
          } else {
            EitherT.pure[Future, PruningError]("")
          }
      } yield {
        if (unifiedSequencer) {
          s"${msg}\n${dbsMsg}"
        } else {
          msg
        }
      }
    else
      EitherT.right(
        supervisedPruningF.map(_ =>
          s"Pruning at $requestedTimestamp is already happening due to an earlier request"
        )
      )
  }

  private def placeLocalEvent(event: BlockSequencer.LocalEvent)(implicit
      traceContext: TraceContext
  ): Future[Unit] = localEventsQueue.offer(Traced(event)).flatMap {
    case QueueOfferResult.Enqueued => Future.unit
    case QueueOfferResult.Dropped => // this should never happen
      Future.failed[Unit](new RuntimeException(s"Request queue is full. cannot take local $event"))
    case QueueOfferResult.Failure(cause) => Future.failed(cause)
    case QueueOfferResult.QueueClosed =>
      logger.info(s"Tried to place a local $event request after the sequencer has been shut down.")
      Future.unit
  }

  override def locatePruningTimestamp(index: PositiveInt)(implicit
      traceContext: TraceContext
  ): EitherT[Future, PruningSupportError, Option[CantonTimestamp]] =
    EitherT.leftT[Future, Option[CantonTimestamp]](PruningError.NotSupported)

  override def reportMaxEventAgeMetric(
      oldestEventTimestamp: Option[CantonTimestamp]
  ): Either[PruningSupportError, Unit] = Either.left(PruningError.NotSupported)

  override def pruningSchedulerBuilder: Option[Storage => PruningScheduler] =
    if (unifiedSequencer) super.pruningSchedulerBuilder else None

  override protected def healthInternal(implicit
      traceContext: TraceContext
  ): Future[SequencerHealthStatus] =
    for {
      ledgerStatus <- blockOrderer.health
      isStorageActive = storage.isActive
      _ = logger.trace(s"Storage active: ${storage.isActive}")
    } yield {
      if (!ledgerStatus.isActive) SequencerHealthStatus(isActive = false, ledgerStatus.description)
      else
        SequencerHealthStatus(
          isStorageActive,
          if (isStorageActive) None else Some("Can't connect to database"),
        )
    }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.*
    logger.debug(s"$name sequencer shutting down")
    Seq[AsyncOrSyncCloseable](
      SyncCloseable("pruningQueue", pruningQueue.close()),
      SyncCloseable("stateManager.close()", stateManager.close()),
      SyncCloseable("localEventsQueue.complete", localEventsQueue.complete()),
      AsyncCloseable(
        "localEventsQueue.watchCompletion",
        localEventsQueue.watchCompletion(),
        timeouts.shutdownProcessing,
      ),
      // The kill switch ensures that we don't process the remaining contents of the queue buffer
      SyncCloseable("killSwitch.shutdown()", killSwitch.shutdown()),
      SyncCloseable(
        "DatabaseSequencer.onClose()",
        super[DatabaseSequencer].onClosed(),
      ),
      AsyncCloseable("done", done, timeouts.shutdownProcessing),
      SyncCloseable("blockOrderer.close()", blockOrderer.close()),
    )
  }

  /** Compute traffic states for the specified members at the provided timestamp.
    * @param requestedMembers members for which to compute traffic states
    * @param approximateUsingWallClock if true, the max between wall clock time and last sequenced event timestamp
    *                                  will be used when computing the traffic states. This is useful mostly for testing
    *                                  when using SimClock to get updates on the state when domain time does not advance.
    *                                  When set to true, the result may NOT be the correct one by the time the domain
    *                                  reaches the wall clock timestamp.
    */
  private def trafficStatesForMembers(
      requestedMembers: Set[Member],
      selector: TimestampSelector,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[Member, Either[String, TrafficState]]] = {
    val timestamp = selector match {
      case ExactTimestamp(timestamp) => Some(timestamp)
      case LastUpdatePerMember => None
      // For the latest safe timestamp, we use the last timestamp of the latest processed block.
      // Even though it may be more recent than the TrafficConsumed timestamp of individual members,
      // we are sure that nothing has been consumed since then, because by the time we update getHeadState.block.lastTs
      // all traffic has been consumed for that block. This means we can use this timestamp to compute an updated
      // base traffic that will be correct.
      case LatestSafe => Some(stateManager.getHeadState.block.lastTs)
      case LatestApproximate => Some(clock.now.max(stateManager.getHeadState.block.lastTs))
    }

    blockRateLimitManager.getStates(
      requestedMembers,
      timestamp,
      stateManager.getHeadState.block.latestSequencerEventTimestamp,
      // Warn on approximate topology or traffic purchased when getting exact traffic states only (so when selector is not LatestApproximate)
      warnIfApproximate = selector != LatestApproximate,
    )
  }

  override def setTrafficPurchased(
      member: Member,
      serial: PositiveInt,
      totalTrafficPurchased: NonNegativeLong,
      sequencerClient: SequencerClient,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficControlError, CantonTimestamp] = {
    for {
      latestBalanceO <- EitherT.right(blockRateLimitManager.lastKnownBalanceFor(member))
      maxSerialO = latestBalanceO.map(_.serial)
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        maxSerialO.forall(_ < serial),
        TrafficControlErrors.TrafficControlSerialTooLow.Error(
          s"The provided serial value $serial is too low. Latest serial used by this member is $maxSerialO"
        ),
      )
      timestamp <- trafficPurchasedSubmissionHandler.sendTrafficPurchasedRequest(
        member,
        domainId,
        protocolVersion,
        serial,
        totalTrafficPurchased,
        sequencerClient,
        cryptoApi,
      )
    } yield timestamp
  }

  override def trafficStatus(requestedMembers: Seq[Member], selector: TimestampSelector)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerTrafficStatus] = {
    trafficStatesForMembers(
      if (requestedMembers.isEmpty) {
        // If requestedMembers get the traffic states of all known member in the head state
        stateManager.getHeadState.chunk.ephemeral.status.membersMap.keySet
      } else requestedMembers.toSet,
      selector,
    )
      .map(SequencerTrafficStatus.apply)
  }

  override def getTrafficStateAt(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerRateLimitError.TrafficNotFound, Option[
    TrafficState
  ]] = {
    blockRateLimitManager.getTrafficStateForMemberAt(
      member,
      timestamp,
      stateManager.getHeadState.block.latestSequencerEventTimestamp,
    )
  }
}

object BlockSequencer {
  sealed trait LocalEvent extends Product with Serializable
  final case class DisableMember(member: Member) extends LocalEvent
  final case class UpdateInitialMemberCounters(timestamp: CantonTimestamp) extends LocalEvent
}
