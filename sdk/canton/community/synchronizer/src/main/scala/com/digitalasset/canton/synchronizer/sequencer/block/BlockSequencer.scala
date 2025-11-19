// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.RichGeneratedMessage
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.{Signature, SynchronizerCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonBaseError
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.pretty.CantonPrettyPrinter
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.{DbExceptionRetryPolicy, Storage}
import com.digitalasset.canton.sequencing.client.SequencerClientSend
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.protocol.SequencerErrors.{
  Overloaded,
  SubmissionRequestRefused,
}
import com.digitalasset.canton.sequencing.traffic.TrafficControlErrors.TrafficControlError
import com.digitalasset.canton.sequencing.traffic.{
  TrafficControlErrors,
  TrafficPurchasedSubmissionHandler,
}
import com.digitalasset.canton.synchronizer.block.BlockSequencerStateManagerBase
import com.digitalasset.canton.synchronizer.block.data.SequencerBlockStore
import com.digitalasset.canton.synchronizer.block.update.BlockUpdateGeneratorImpl
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.*
import com.digitalasset.canton.synchronizer.sequencer.PruningError.UnsafePruningPoint
import com.digitalasset.canton.synchronizer.sequencer.Sequencer.SignedSubmissionRequest
import com.digitalasset.canton.synchronizer.sequencer.admin.data.SequencerHealthStatus
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError.{
  BlockNotFound,
  ExceededMaxSequencingTime,
}
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerStore
import com.digitalasset.canton.synchronizer.sequencer.traffic.TimestampSelector.*
import com.digitalasset.canton.synchronizer.sequencer.traffic.{
  SequencerRateLimitError,
  SequencerRateLimitManager,
  SequencerTrafficStatus,
}
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.TrafficPurchasedStore
import com.digitalasset.canton.time.{Clock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.util.retry.Pause
import com.digitalasset.canton.util.{EitherTUtil, PekkoUtil, SimpleExecutionQueue}
import io.grpc.ServerServiceDefinition
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.slf4j.event.Level

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

import BlockSequencerFactory.OrderingTimeFixMode

class BlockSequencer(
    blockOrderer: BlockOrderer,
    name: String,
    cryptoApi: SynchronizerCryptoClient,
    sequencerId: SequencerId,
    stateManager: BlockSequencerStateManagerBase,
    store: SequencerBlockStore,
    dbSequencerStore: SequencerStore,
    blockSequencerConfig: BlockSequencerConfig,
    useTimeProofsToObserveEffectiveTime: Boolean,
    trafficPurchasedStore: TrafficPurchasedStore,
    storage: Storage,
    futureSupervisor: FutureSupervisor,
    health: Option[SequencerHealthConfig],
    clock: Clock,
    blockRateLimitManager: SequencerRateLimitManager,
    orderingTimeFixMode: OrderingTimeFixMode,
    sequencingTimeLowerBoundExclusive: Option[CantonTimestamp],
    processingTimeouts: ProcessingTimeout,
    logEventDetails: Boolean,
    prettyPrinter: CantonPrettyPrinter,
    metrics: SequencerMetrics,
    loggerFactory: NamedLoggerFactory,
    exitOnFatalFailures: Boolean,
    runtimeReady: FutureUnlessShutdown[Unit],
)(implicit executionContext: ExecutionContext, materializer: Materializer, val tracer: Tracer)
    extends DatabaseSequencer(
      SequencerWriterStoreFactory.singleInstance,
      dbSequencerStore,
      blockSequencerConfig.toDatabaseSequencerConfig,
      None,
      TotalNodeCountValues.SingleSequencerTotalNodeCount,
      new LocalSequencerStateEventSignaller(
        processingTimeouts,
        loggerFactory,
      ),
      None,
      None,
      processingTimeouts,
      storage,
      None,
      health,
      clock,
      sequencerId,
      cryptoApi,
      metrics,
      loggerFactory,
      blockSequencerMode = true,
      sequencingTimeLowerBoundExclusive = sequencingTimeLowerBoundExclusive,
      rateLimitManagerO = Some(blockRateLimitManager),
    )
    with DatabaseSequencerIntegration
    with NamedLogging
    with FlagCloseableAsync {

  private val protocolVersion = cryptoApi.protocolVersion

  private[sequencer] val pruningQueue = new SimpleExecutionQueue(
    "block-sequencer-pruning-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )
  private val sequencerPruningPromises = TrieMap[CantonTimestamp, Promise[Unit]]()

  override lazy val rateLimitManager: Option[SequencerRateLimitManager] = Some(
    blockRateLimitManager
  )

  private val trafficPurchasedSubmissionHandler =
    new TrafficPurchasedSubmissionHandler(clock, loggerFactory)

  override protected def resetWatermarkTo: SequencerWriter.ResetWatermark =
    sequencingTimeLowerBoundExclusive match {
      case Some(boundExclusive) =>
        SequencerWriter.ResetWatermarkToTimestamp(
          stateManager.getHeadState.block.lastTs.max(boundExclusive)
        )

      case None =>
        SequencerWriter.ResetWatermarkToTimestamp(
          stateManager.getHeadState.block.lastTs
        )
    }

  private val circuitBreaker = BlockSequencerCircuitBreaker(
    blockSequencerConfig.circuitBreaker,
    clock,
    metrics,
    materializer,
    loggerFactory,
  )
  private val throughputCap =
    new BlockSequencerThroughputCap(
      blockSequencerConfig.throughputCap,
      clock,
      materializer.system.scheduler,
      metrics,
      loggerFactory,
    )

  private val (killSwitchF, done) = {
    val headState = stateManager.getHeadState
    noTracingLogger.info(s"Subscribing to block source from ${headState.block.height + 1}")

    val updateGenerator = new BlockUpdateGeneratorImpl(
      protocolVersion,
      cryptoApi,
      sequencerId,
      blockRateLimitManager,
      orderingTimeFixMode,
      sequencingTimeLowerBoundExclusive = sequencingTimeLowerBoundExclusive,
      useTimeProofsToObserveEffectiveTime,
      metrics,
      loggerFactory,
      memberValidator = memberValidator,
    )(CloseContext(cryptoApi), tracer)

    implicit val traceContext: TraceContext = TraceContext.empty

    val driverSource = Source
      .futureSource(runtimeReady.unwrap.map {
        case UnlessShutdown.AbortedDueToShutdown =>
          noTracingLogger.debug("Not initiating subscription to block source due to shutdown")
          Source.empty.viaMat(KillSwitches.single)(Keep.right)
        case UnlessShutdown.Outcome(_) =>
          noTracingLogger.debug("Subscribing to block source")
          blockOrderer.subscribe()
      })
      // Explicit async to make sure that the block processing runs in parallel with the block retrieval
      .async
      .map(updateGenerator.extractBlockEvents)
      .via(stateManager.processBlock(updateGenerator))
      .wireTap { update =>
        throughputCap.addBlockUpdate(update.value)
      }
      .async
      .via(stateManager.applyBlockUpdate(this))
      .wireTap { lastTs =>
        circuitBreaker.registerLastBlockTimestamp(lastTs)
      }
    PekkoUtil.runSupervised(
      driverSource.toMat(Sink.ignore)(Keep.both),
      errorLogMessagePrefix = "Fatally failed to handle state changes",
    )
  }

  done onComplete {
    case Success(_) => noTracingLogger.debug("Sequencer flow has shutdown")
    case Failure(ex) => noTracingLogger.error("Sequencer flow has failed", ex)
  }

  private def validateMaxSequencingTime(
      submission: SubmissionRequest
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonBaseError, Unit] =
    for {
      estimatedSequencingTimestampO <- EitherT.right(sequencingTime)
      estimatedSequencingTimestamp = estimatedSequencingTimestampO.getOrElse(clock.now)
      _ = logger.debug(
        s"Estimated sequencing time $estimatedSequencingTimestamp for submission with id ${submission.messageId}"
      )
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        submission.maxSequencingTime > estimatedSequencingTimestamp,
        ExceededMaxSequencingTime.Error(
          estimatedSequencingTimestamp,
          submission.maxSequencingTime,
          s"Estimation for message id ${submission.messageId}",
        ),
      )
      _ <- submission.aggregationRule.traverse_ { _ =>
        // We can't easily use snapshot(topologyTimestamp), because the effective last snapshot transaction
        // visible in the BlockSequencer can be behind the topologyTimestamp and tracking that there's an
        // intermediate topology change is impossible here (will need to communicate with the BlockUpdateGenerator).
        // If topologyTimestamp happens to be ahead of current topology's timestamp we grab the latter
        // to prevent a deadlock.
        val topologyTimestamp = cryptoApi.approximateTimestamp.min(
          submission.topologyTimestamp.getOrElse(CantonTimestamp.MaxValue)
        )
        for {
          snapshot <- EitherT.right(cryptoApi.snapshot(topologyTimestamp))
          synchronizerParameters <- EitherT(
            snapshot.ipsSnapshot.findDynamicSynchronizerParameters()
          ).leftMap(error =>
            SequencerErrors.Internal(
              s"Could not fetch dynamic synchronizer parameters: $error"
            ): CantonBaseError
          )
          maxSequencingTimeUpperBound = estimatedSequencingTimestamp.add(
            synchronizerParameters.parameters.sequencerAggregateSubmissionTimeout.duration
          )
          _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
            submission.maxSequencingTime < maxSequencingTimeUpperBound,
            SequencerErrors.SubmissionRequestRefused(
              s"Max sequencing time ${submission.maxSequencingTime} for submission with id ${submission.messageId} is too far in the future, currently bounded at $maxSequencingTimeUpperBound"
            ): CantonBaseError,
          )
        } yield ()
      }
    } yield ()

  override protected def sendAsyncInternal(
      submission: SubmissionRequest
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonBaseError, Unit] = {
    val signedContent = SignedContent(submission, Signature.noSignature, None, protocolVersion)
    sendAsyncSignedInternal(signedContent)
  }

  override def adminServices: Seq[ServerServiceDefinition] = blockOrderer.adminServices

  private def enforceRateLimiting(
      request: SignedSubmissionRequest
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] =
    blockRateLimitManager
      .validateRequestAtSubmissionTime(
        request.content,
        request.timestampOfSigningKey,
        // Use the timestamp of the latest chunk here, such that top ups that happened in an earlier chunk of the
        // current block can be reflected in the traffic state used to validate the request
        stateManager.getHeadState.chunk.lastTs,
        stateManager.getHeadState.chunk.latestSequencerEventTimestamp,
      )
      .leftMap {
        // If the cost is outdated, we bounce the request with a specific SendAsyncError so the
        // sender has the required information to retry the request with the correct cost
        case notEnoughTraffic: SequencerRateLimitError.AboveTrafficLimit =>
          logger.debug(
            s"Rejecting submission request because not enough traffic is available: $notEnoughTraffic"
          )
          SequencerErrors.TrafficCredit(
            s"Submission was rejected because no traffic is available: $notEnoughTraffic"
          )
        // If the cost is outdated, we bounce the request with a specific SendAsyncError so the
        // sender has the required information to retry the request with the correct cost
        case outdated: SequencerRateLimitError.OutdatedEventCost =>
          logger.debug(
            s"Rejecting submission request because the cost was computed using an outdated topology: $outdated"
          )
          SequencerErrors.OutdatedTrafficCost(
            s"Submission was refused because traffic cost was outdated. Re-submit after having observed the validation timestamp and processed its topology state: $outdated"
          )
        case error =>
          SequencerErrors.SubmissionRequestRefused(
            s"Submission was refused because traffic control validation failed: $error"
          )
      }

  private def enforceThroughputCap(
      submission: SubmissionRequest
  ): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] =
    EitherT
      .fromEither[FutureUnlessShutdown](
        throughputCap.shouldRejectTransaction(submission.requestType, submission.sender, 0)
      )
      .leftMap { msg =>
        SequencerErrors.Overloaded(
          s"Member ${submission.sender} has reached its throughput cap: $msg"
        )
      }

  /** This method rejects submissions before or at the lower bound of sequencing time. It compares
    * clock.now against the configured sequencingTimeLowerBoundExclusive. It cannot use the time
    * from the head state, because any blocks before sequencingTimeLowerBoundExclusive are filtered
    * out and therefore the time would never advance. The ordering service is expected to lag a bit
    * behind the (synchronized) wall clock, therefore this method does not reject submissions that
    * would be sequenced after sequencingTimeLowerBoundExclusive. It could however pass through
    * submissions that then get dropped, because they end up getting sequenced before
    * sequencingTimeLowerBoundExclusive.
    */
  private def rejectSubmissionsBeforeOrAtSequencingTimeLowerBound()
      : EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] = {
    val currentTime = clock.now

    sequencingTimeLowerBoundExclusive match {
      case Some(boundExclusive) =>
        EitherTUtil.condUnitET[FutureUnlessShutdown](
          currentTime > boundExclusive,
          SubmissionRequestRefused(
            s"Cannot submit before or at the lower bound for sequencing time $boundExclusive; time is currently at $currentTime"
          ),
        )

      case None => EitherTUtil.unitUS
    }
  }

  private def rejectSubmissionsIfOverloaded(
      submission: SubmissionRequest
  ): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] =
    if (circuitBreaker.shouldRejectRequests(submission))
      EitherT.leftT(
        Overloaded("Sequencer can't take requests because it is behind on processing events")
      )
    else EitherT.rightT(())

  private def rejectAcknowledgementIfOverloaded()
      : EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] =
    if (circuitBreaker.shouldRejectAcknowledgements)
      EitherT.leftT(
        Overloaded("Sequencer can't take requests because it is behind on processing events")
      )
    else EitherT.rightT(())

  override protected def sendAsyncSignedInternal(
      signedSubmission: SignedContent[SubmissionRequest]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonBaseError, Unit] = {
    val submission = signedSubmission.content
    val SubmissionRequest(
      sender,
      _,
      batch,
      maxSequencingTime,
      _,
      _,
      _,
    ) = submission
    logger.debug(
      s"Request to send submission with id ${submission.messageId} with max sequencing time $maxSequencingTime from $sender to ${batch.allRecipients}"
    )

    for {
      _ <- rejectSubmissionsBeforeOrAtSequencingTimeLowerBound()
      _ <- enforceThroughputCap(submission)
      _ <- rejectSubmissionsIfOverloaded(submission)
      // TODO(i17584): revisit the consequences of no longer enforcing that
      //  aggregated submissions with signed envelopes define a topology snapshot
      _ <- validateMaxSequencingTime(submission)
      // TODO(#19476): Why we don't check group recipients here?
      _ <- SubmissionRequestValidations
        .checkSenderAndRecipientsAreRegistered(
          submission,
          // Using currentSnapshotApproximation due to members registration date
          // expected to be before submission sequencing time
          cryptoApi.currentSnapshotApproximation.ipsSnapshot,
        )
        .leftMap(_.toSequencerDeliverError)
      _ = if (logEventDetails)
        logger.debug(
          s"Invoking send operation on the ledger with the following protobuf message serialized to bytes ${prettyPrinter
              .printAdHoc(submission.toProtoVersioned)}"
        )
      _ <- enforceRateLimiting(signedSubmission)
      _ <- EitherT(
        futureSupervisor.supervised(
          s"Sending submission request with id ${submission.messageId} from $sender to ${batch.allRecipients}"
        )(blockOrderer.send(signedSubmission).value)
      ).mapK(FutureUnlessShutdown.outcomeK).leftWiden[CantonBaseError]
    } yield ()
  }

  override protected def localSequencerMember: Member = sequencerId

  override protected def acknowledgeSignedInternal(
      signedAcknowledgeRequest: SignedContent[AcknowledgeRequest]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val req = signedAcknowledgeRequest.content
    logger.debug(s"Request for member ${req.member} to acknowledge timestamp ${req.timestamp}")
    for {
      _ <- EitherTUtil.toFutureUnlessShutdown(
        rejectAcknowledgementIfOverloaded().leftMap(_.asGrpcError)
      )
      _ <- EitherTUtil.toFutureUnlessShutdown(
        rejectSubmissionsBeforeOrAtSequencingTimeLowerBound().leftMap(_.asGrpcError)
      )
      waitForAcknowledgementF = stateManager.waitForAcknowledgementToComplete(
        req.member,
        req.timestamp,
      )
      _ <- FutureUnlessShutdown.outcomeF(blockOrderer.acknowledge(signedAcknowledgeRequest))
      _ <- FutureUnlessShutdown.outcomeF(waitForAcknowledgementF)
    } yield ()
  }

  override def awaitSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerError, SequencerSnapshot] = {
    val delay = 1.second
    val waitForBlockContainingTimestamp = EitherT.right(
      Pause(
        logger,
        this,
        maxRetries = (timeouts.default.duration / delay).toInt,
        delay,
        s"$functionFullName($timestamp)",
      )
        .unlessShutdown(
          FutureUnlessShutdown.pure(timestamp <= stateManager.getHeadState.block.lastTs),
          DbExceptionRetryPolicy,
        )
    )

    waitForBlockContainingTimestamp.flatMap { foundBlock =>
      if (foundBlock) {
        // if we found a block, now also await for the underlying DatabaseSequencer's snapshot to be ready.
        // since it uses a different, watermark-based mechanism to determine how far in time it has progressed.
        super[DatabaseSequencer].awaitSnapshot(timestamp).flatMap(_ => snapshot(timestamp))
      } else
        EitherT.leftT[FutureUnlessShutdown, SequencerSnapshot](
          BlockNotFound.InvalidTimestamp(timestamp): SequencerError
        )
    }
  }

  override def awaitContainingBlockLastTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerError, CantonTimestamp] = {
    val delay = 1.second
    EitherT(
      Pause(
        logger,
        this,
        maxRetries = (timeouts.default.duration / delay).toInt,
        delay,
        s"$functionFullName($timestamp)",
      ).unlessShutdown(
        store
          .findBlockContainingTimestamp(timestamp)
          .map(_.lastTs)
          .value,
        DbExceptionRetryPolicy,
      )
    )
  }

  override def snapshot(
      timestamp: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerError, SequencerSnapshot] =
    // TODO(#12676) Make sure that we don't request a snapshot for a state that was already pruned

    for {
      additionalInfo <- blockOrderer
        .sequencerSnapshotAdditionalInfo(timestamp)
        .mapK(FutureUnlessShutdown.outcomeK)

      implementationSpecificInfo = additionalInfo.map(info =>
        SequencerSnapshot.ImplementationSpecificInfo(
          implementationName = "BlockSequencer",
          info.checkedToByteString,
        )
      )

      topologySnapshot <- EitherT.right(cryptoApi.awaitSnapshot(timestamp))
      parameterChanges <- EitherT.right(
        topologySnapshot.ipsSnapshot.listDynamicSynchronizerParametersChanges()
      )
      maxSequencingTimeBound = SequencerUtils.maxSequencingTimeUpperBoundAt(
        timestamp,
        parameterChanges,
      )
      blockState <- store
        .readStateForBlockContainingTimestamp(timestamp, maxSequencingTimeBound)
      // Look up traffic info at the latest timestamp from the block,
      // because that's where the onboarded sequencer will start reading
      trafficPurchased <- EitherT
        .right[SequencerError](
          trafficPurchasedStore
            .lookupLatestBeforeInclusive(blockState.latestBlock.lastTs)
        )
      trafficConsumed <- EitherT
        .right[SequencerError](
          blockRateLimitManager.trafficConsumedStore
            .lookupLatestBeforeInclusive(blockState.latestBlock.lastTs)
        )

      _ = if (logger.underlying.isDebugEnabled()) {
        logger.debug(
          s"""BlockSequencer data for snapshot for timestamp $timestamp:
             |blockState: $blockState
             |trafficPurchased: $trafficPurchased
             |trafficConsumed: $trafficConsumed
             |implementationSpecificInfo: $implementationSpecificInfo""".stripMargin
        )
      }
      finalSnapshot <- {
        super.snapshot(blockState.latestBlock.lastTs).map { dbsSnapshot =>
          val finalSnapshot = dbsSnapshot.copy(
            latestBlockHeight = blockState.latestBlock.height,
            inFlightAggregations = blockState.inFlightAggregations,
            additional = implementationSpecificInfo,
            trafficPurchased = trafficPurchased,
            trafficConsumed = trafficConsumed,
          )(dbsSnapshot.representativeProtocolVersion)
          finalSnapshot
        }
      }
    } yield {
      logger.trace(
        s"Resulting snapshot for timestamp $timestamp:\n$finalSnapshot"
      )
      finalSnapshot
    }

  private def waitForPruningToComplete(timestamp: CantonTimestamp): (Boolean, Future[Unit]) = {
    val newPromise = Promise[Unit]()
    val (isNew, promise) = sequencerPruningPromises
      .putIfAbsent(timestamp, newPromise)
      .fold((true, newPromise))(oldPromise => (false, oldPromise))
    (isNew, promise.future)
  }

  private def resolveSequencerPruning(timestamp: CantonTimestamp): Unit =
    sequencerPruningPromises.remove(timestamp) foreach { promise => promise.success(()) }

  /** Important: currently both the disable member and the prune functionality on the block
    * sequencer are purely local operations that do not affect other block sequencers that share the
    * same source of events.
    */
  override def prune(requestedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, PruningError, String] = {

    val (isNewRequest, pruningF) = waitForPruningToComplete(requestedTimestamp)
    val supervisedPruningF = futureSupervisor.supervised(
      description = s"Waiting for local pruning operation at $requestedTimestamp to complete",
      logLevel = Level.INFO,
    )(pruningF)

    if (isNewRequest)
      for {
        status <- EitherT.right[PruningError](this.pruningStatus)
        _ <- condUnitET[FutureUnlessShutdown](
          requestedTimestamp <= status.safePruningTimestamp,
          UnsafePruningPoint(requestedTimestamp, status.safePruningTimestamp): PruningError,
        )
        msg <- EitherT(
          pruningQueue
            .executeUS(
              for {
                eventsMsg <- store.prune(requestedTimestamp)
                trafficMsg <- blockRateLimitManager.prune(requestedTimestamp)
                msgEither <-
                  super[DatabaseSequencer]
                    .prune(requestedTimestamp)
                    .map(dbsMsg =>
                      s"${eventsMsg.replace("0 events and ", "")}\n$dbsMsg\n$trafficMsg"
                    )
                    .value
              } yield msgEither,
              s"pruning sequencer at $requestedTimestamp",
            )
            .unwrap
            .map(
              _.onShutdown(
                Right(s"pruning at $requestedTimestamp canceled because we're shutting down")
              )
            )
        ).mapK(FutureUnlessShutdown.outcomeK)
        _ = resolveSequencerPruning(requestedTimestamp)
        _ <- EitherT.right(supervisedPruningF).mapK(FutureUnlessShutdown.outcomeK)
      } yield msg
    else
      EitherT.right(
        FutureUnlessShutdown
          .outcomeF(supervisedPruningF)
          .map(_ =>
            s"Pruning at $requestedTimestamp is already happening due to an earlier request"
          )
      )
  }

  override def findPruningTimestamp(index: PositiveInt)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, PruningSupportError, Option[CantonTimestamp]] =
    EitherT.leftT[FutureUnlessShutdown, Option[CantonTimestamp]](PruningError.NotSupported)

  override def reportMaxEventAgeMetric(
      oldestEventTimestamp: Option[CantonTimestamp]
  ): Either[PruningSupportError, Unit] = Either.left(PruningError.NotSupported)

  override protected def healthInternal(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerHealthStatus] =
    for {
      ledgerStatus <- FutureUnlessShutdown.outcomeF(blockOrderer.health)
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
      // The kill switch ensures that we don't process the remaining contents of the queue buffer
      AsyncCloseable(
        "killSwitchF(_.shutdown())",
        killSwitchF.map(_.shutdown()),
        timeouts.shutdownProcessing,
      ),
      SyncCloseable(
        "DatabaseSequencer.onClose()",
        super[DatabaseSequencer].onClosed(),
      ),
      AsyncCloseable("done", done, timeouts.shutdownProcessing), // Close the consumer first
      SyncCloseable("blockOrderer.close()", blockOrderer.close()),
      SyncCloseable("cryptoApi.close()", cryptoApi.close()),
      SyncCloseable("circuitBreaker.close()", circuitBreaker.close()),
      SyncCloseable("throughputCap.close()", throughputCap.close()),
    )
  }

  /** Compute traffic states for the specified members at the provided timestamp.
    * @param requestedMembers
    *   members for which to compute traffic states
    * @param selector
    *   timestamp selector determining at what time the traffic states will be computed
    */
  private def trafficStatesForMembers(
      requestedMembers: Set[Member],
      selector: TimestampSelector,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[Member, Either[String, TrafficState]]] =
    if (requestedMembers.isEmpty) {
      // getStates interprets an empty list of members as "return all members"
      // so we handle it here.
      FutureUnlessShutdown.pure(Map.empty)
    } else {
      val timestamp = selector match {
        case ExactTimestamp(timestamp) => Some(timestamp)
        case LastUpdatePerMember => None
        // For the latest safe timestamp, we use the last timestamp of the latest processed block.
        // Even though it may be more recent than the TrafficConsumed timestamp of individual members,
        // we are sure that nothing has been consumed since then, because by the time we update getHeadState.block.lastTs
        // all traffic has been consumed for that block. This means we can use this timestamp to compute an updated
        // base traffic that will be correct. More precisely, we take the immediate successor such that we include
        // all the changes of that last block.
        case LatestSafe => Some(stateManager.getHeadState.block.lastTs.immediateSuccessor)
        case LatestApproximate =>
          Some(clock.now.max(stateManager.getHeadState.block.lastTs.immediateSuccessor))
      }

      blockRateLimitManager.getStates(
        requestedMembers,
        timestamp,
        stateManager.getHeadState.block.latestSequencerEventTimestamp,
        // TODO(#18401) set warnIfApproximate to true and check that we don't get warnings
        // Warn on approximate topology or traffic purchased when getting exact traffic states only (so when selector is not LatestApproximate)
        // selector != LatestApproximate

        // Also don't warn until the sequencer has at least received one event
        // This used to check the ephemeral state for headCounter(sequencerId).exists(_ > Genesis),
        // but because the ephemeral state for the block sequencer didn't actually contain
        // any sequencer counter data anymore, this condition was always false, which made the overall expression
        // for warnIfApproximate false
        warnIfApproximate = false,
      )
    }

  override def setTrafficPurchased(
      member: Member,
      serial: PositiveInt,
      totalTrafficPurchased: NonNegativeLong,
      sequencerClient: SequencerClientSend,
      synchronizerTimeTracker: SynchronizerTimeTracker,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficControlError, Unit] =
    for {
      latestBalanceO <- EitherT.right(blockRateLimitManager.lastKnownBalanceFor(member))
      maxSerialO = latestBalanceO.map(_.serial)
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        maxSerialO.forall(_ < serial),
        TrafficControlErrors.TrafficControlSerialTooLow.Error(
          s"The provided serial value $serial is too low. Latest serial used by this member is $maxSerialO"
        ),
      )
      _ <- trafficPurchasedSubmissionHandler.sendTrafficPurchasedRequest(
        member,
        serial,
        totalTrafficPurchased,
        sequencerClient,
        synchronizerTimeTracker,
        cryptoApi,
      )
    } yield ()

  override def trafficStatus(requestedMembers: Seq[Member], selector: TimestampSelector)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerTrafficStatus] =
    for {
      members <-
        if (requestedMembers.isEmpty) {
          // If requestedMembers is not set get the traffic states of all known members
          cryptoApi.currentSnapshotApproximation.ipsSnapshot.allMembers()
        } else {
          cryptoApi.currentSnapshotApproximation.ipsSnapshot
            .allMembers()
            .map { registered =>
              requestedMembers.toSet.intersect(registered)
            }
        }
      trafficState <- trafficStatesForMembers(
        members,
        selector,
      )
    } yield SequencerTrafficStatus(trafficState)

  override def getTrafficStateAt(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerRateLimitError.TrafficNotFound, Option[
    TrafficState
  ]] =
    blockRateLimitManager.getTrafficStateForMemberAt(
      member,
      timestamp,
      stateManager.getHeadState.block.latestSequencerEventTimestamp,
    )

  override def sequencingTime(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    blockOrderer.sequencingTime

  override private[canton] def orderer: Some[BlockOrderer] = Some(blockOrderer)
}
