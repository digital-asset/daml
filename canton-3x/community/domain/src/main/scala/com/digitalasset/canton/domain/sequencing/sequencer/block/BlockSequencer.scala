// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, Signature}
import com.digitalasset.canton.data.PeanoQueue.NotInserted
import com.digitalasset.canton.data.{CantonTimestamp, Counter, PeanoTreeQueue}
import com.digitalasset.canton.domain.block.data.SequencerBlockStore
import com.digitalasset.canton.domain.block.{
  BlockSequencerStateManager,
  BlockUpdateGenerator,
  RawLedgerBlock,
}
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.PruningError.UnsafePruningPoint
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer.EventSource
import com.digitalasset.canton.domain.sequencing.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.domain.sequencing.sequencer.errors.{
  CreateSubscriptionError,
  OperationError,
  RegisterMemberError,
  SequencerWriteError,
}
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.{
  SequencerRateLimitManager,
  SequencerTrafficStatus,
}
import com.digitalasset.canton.domain.sequencing.sequencer.{
  BaseSequencer,
  LedgerIdentity,
  PruningError,
  PruningSupportError,
  SequencerHealthConfig,
  SequencerSnapshot,
  SequencerValidations,
  SignatureVerifier,
  *,
}
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.health.admin.data.SequencerHealthStatus
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.scheduler.PruningScheduler
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, OptionUtil, PekkoUtil}
import com.digitalasset.canton.version.ProtocolVersion
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{Keep, Merge, Sink, Source}

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

class BlockSequencer(
    blockSequencerOps: BlockSequencerOps,
    name: String,
    domainId: DomainId,
    initialBlockHeight: Option[Long],
    cryptoApi: DomainSyncCryptoClient,
    topologyClientMember: Member,
    stateManager: BlockSequencerStateManager,
    store: SequencerBlockStore,
    storage: Storage,
    futureSupervisor: FutureSupervisor,
    health: Option[SequencerHealthConfig],
    clock: Clock,
    protocolVersion: ProtocolVersion,
    rateLimitManager: Option[SequencerRateLimitManager],
    implicitMemberRegistration: Boolean,
    orderingTimeFixMode: OrderingTimeFixMode,
    parameters: CantonNodeParameters,
    metrics: SequencerMetrics,
    loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, materializer: Materializer, trace: Tracer)
    extends BaseSequencer(
      DomainTopologyManagerId(domainId),
      loggerFactory,
      health,
      clock,
      SignatureVerifier(cryptoApi),
    )
    with NamedLogging
    with FlagCloseableAsync {

  override def timeouts: ProcessingTimeout = parameters.processingTimeouts

  override private[sequencing] def firstSequencerCounterServeableForSequencer: SequencerCounter =
    stateManager.firstSequencerCounterServableForSequencer

  noTracingLogger.info(
    s"Subscribing to block source from ${stateManager.getHeadState.block.height}"
  )
  private val (killSwitch, localEventsQueue, done) = {
    val updateGenerator = new BlockUpdateGenerator(
      domainId,
      protocolVersion,
      cryptoApi,
      topologyClientMember,
      stateManager.maybeLowerSigningTimestampBound,
      rateLimitManager,
      implicitMemberRegistration,
      orderingTimeFixMode,
      loggerFactory,
    )
    val ((killSwitch, localEventsQueue), done) = PekkoUtil.runSupervised(
      ex => logger.error("Fatally failed to handle state changes", ex)(TraceContext.empty),
      Source
        .combineMat(
          blockSequencerOps
            .subscribe()(TraceContext.empty)
            .statefulMapConcat(BlockSequencer.ensureBlocksAreOrdered(initialBlockHeight))
            .map(block => Right(block): Either[BlockSequencer.LocalEvent, RawLedgerBlock]),
          Source
            .queue[BlockSequencer.LocalEvent](bufferSize = 1000, OverflowStrategy.backpressure)
            .map(event => Left(event): Either[BlockSequencer.LocalEvent, RawLedgerBlock]),
        )(Merge(_))(Keep.both)
        .filterNot(_ => isClosing)
        .mapAsync(
          // `stateManager.handleBlock` in `handleBlockContents` must execute sequentially.
          parallelism = 1
        ) {
          case Right(blockEvents) =>
            implicit val tc: TraceContext =
              blockEvents.events.headOption.map(_.traceContext).getOrElse(TraceContext.empty)
            logger.debug(
              s"Handle block with height=${blockEvents.blockHeight} with num-events=${blockEvents.events.length}"
            )
            stateManager
              .handleBlock(
                updateGenerator.asBlockUpdate(blockEvents)
              )
              .map { state =>
                metrics.sequencerClient.delay
                  .updateValue((clock.now - state.latestBlock.lastTs).toMillis)
                ()
              }
          case Left(localEvent) => stateManager.handleLocalEvent(localEvent)(TraceContext.empty)
        }
        .toMat(Sink.ignore)(Keep.both),
    )
    (killSwitch, localEventsQueue, done)
  }

  done onComplete {
    case Success(_) => noTracingLogger.debug("Sequencer flow has shutdown")
    case Failure(ex) => noTracingLogger.error("Sequencer flow has failed", ex)
  }

  private object TimestampOfSigningKeyCheck

  private def ensureTimestampOfSigningKeyPresentForAggregationRule(
      submission: SubmissionRequest
  ): EitherT[Future, SendAsyncError, TimestampOfSigningKeyCheck.type] = {
    EitherTUtil
      .condUnitET[Future](
        submission.aggregationRule.isEmpty || submission.timestampOfSigningKey.isDefined,
        SendAsyncError.RequestInvalid(
          s"Submission id ${submission.messageId} has `aggregationRule` set, but `timestampOfSigningKey` is not defined. Please check that `timestampOfSigningKey` has been set for the submission."
        ): SendAsyncError,
      )
      .map(_ => TimestampOfSigningKeyCheck)
  }

  private def validateMaxSequencingTime(
      _timestampOfSigningKeyCheck: TimestampOfSigningKeyCheck.type,
      submission: SubmissionRequest,
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncError, Unit] = {
    val estimateSequencingTimestamp = clock.now
    submission.aggregationRule match {
      case Some(_) =>
        for {
          _ <- EitherTUtil.condUnitET[Future](
            submission.maxSequencingTime > estimateSequencingTimestamp,
            SendAsyncError.RequestInvalid(
              s"Max sequencing time ${submission.maxSequencingTime} for submission with id ${submission.messageId} is already past the sequencer clock timestamp $estimateSequencingTimestamp"
            ),
          )
          timestampOfSigningKey <- EitherT.fromOption[Future](
            submission.timestampOfSigningKey,
            ErrorUtil.internalError(
              new IllegalStateException(
                "timestampOfSigningKey is expected to be defined at this point"
              )
            ),
          )
          // We can't easily use snapshot(timestampOfSigningKey), because the effective last snapshot transaction
          // visible in the BlockSequencer can be behind the timestampOfSigningKey and tracking that there's an
          // intermediate topology change is impossible here (will need to communicate with the BlockUpdateGenerator).
          // If timestampOfSigningKey happens to be ahead of current topology's timestamp we grab the latter
          // to prevent a deadlock.
          topologyTimestamp = cryptoApi.approximateTimestamp.min(timestampOfSigningKey)
          snapshot <- EitherT.right(cryptoApi.snapshot(topologyTimestamp))
          domainParameters <- EitherT(
            snapshot.ipsSnapshot.findDynamicDomainParameters()
          )
            .leftMap(error =>
              SendAsyncError.Internal(s"Could not fetch dynamic domain parameters: $error")
            )
          maxSequencingTimeUpperBound = estimateSequencingTimestamp.add(
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
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncError, Unit] = {
    val signedContent = SignedContent(submission, Signature.noSignature, None, protocolVersion)
    sendAsyncSignedInternal(signedContent)
  }

  override protected def sendAsyncSignedInternal(
      signedSubmission: SignedContent[SubmissionRequest]
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncError, Unit] = {
    val submission = signedSubmission.content
    val SubmissionRequest(
      sender,
      _,
      _,
      batch,
      maxSequencingTime,
      _,
      _aggregationRule,
    ) = submission
    logger.debug(
      s"Request to send submission with id ${submission.messageId} with max sequencing time $maxSequencingTime from $sender to ${batch.allRecipients}"
    )

    for {
      timestampOfSigningKeyCheck <- ensureTimestampOfSigningKeyPresentForAggregationRule(submission)
      _ <- validateMaxSequencingTime(timestampOfSigningKeyCheck, submission)
      memberCheck <-
        if (implicitMemberRegistration) {
          EitherT
            .right[SendAsyncError](
              cryptoApi.currentSnapshotApproximation.ipsSnapshot
                .allMembers()
                .map(allMembers =>
                  (member: Member) => allMembers.contains(member) || !member.isAuthenticated
                )
            )
        } else {
          EitherT.rightT[Future, SendAsyncError]((member: Member) =>
            stateManager.isMemberRegistered(member)
          )
        }
      _ <- SequencerValidations
        .checkSenderAndRecipientsAreRegistered(
          submission,
          memberCheck,
        )
        .toEitherT[Future]
      _ = if (parameters.loggingConfig.eventDetails)
        logger.debug(
          s"Invoking send operation on the ledger with the following protobuf message serialized to bytes ${parameters.loggingConfig.api.printer
              .printAdHoc(submission.toProtoVersioned)}"
        )
      _ <-
        EitherT(
          futureSupervisor
            .supervised(
              s"Sending submission request with id ${submission.messageId} from $sender to ${batch.allRecipients}"
            )(
              blockSequencerOps.send(signedSubmission).value
            )
        )
    } yield ()
  }

  override protected def readInternal(member: Member, offset: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CreateSubscriptionError, EventSource] = {
    logger.debug(s"Answering readInternal(member = $member, offset = $offset)")
    if (implicitMemberRegistration) {
      if (!member.isAuthenticated) {
        // allowing unauthenticated members to read events is the same as automatically registering an unauthenticated member
        // and then proceeding with the subscription.
        // optimization: if the member is unauthenticated, we don't need to fetch all members from the snapshot
        EitherT.fromEither[Future](stateManager.readEventsForMember(member, offset))
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
    } else {
      EitherT.fromEither[Future](stateManager.readEventsForMember(member, offset))
    }
  }

  override def isRegistered(
      member: Member
  )(implicit traceContext: TraceContext): Future[Boolean] = {
    if (implicitMemberRegistration) {
      if (!member.isAuthenticated) Future.successful(true)
      else cryptoApi.headSnapshot.ipsSnapshot.isMemberKnown(member)
    } else {
      logger.debug(s"Answering isRegistered(member = $member)")
      Future.successful(stateManager.isMemberRegistered(member))
    }
  }

  override def registerMember(member: Member)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerWriteError[RegisterMemberError], Unit] = {
    if (implicitMemberRegistration) {
      // there is nothing extra to be done for member registration in Canton 3.x
      EitherT.rightT[Future, SequencerWriteError[RegisterMemberError]](())
    } else {
      logger.debug(s"Registering $member at the $name sequencer")
      if (stateManager.isMemberRegistered(member)) {
        val error = OperationError(RegisterMemberError.AlreadyRegisteredError(member))
        EitherT.leftT[Future, Unit](error)
      } else {
        val actuallyRegisteredF =
          futureSupervisor.supervised(s"Waiting for member $member to be exist")(
            stateManager.waitForMemberToExist(member)
          )
        for {
          _ <- blockSequencerOps.register(member)
          // wait for the blockchain to reflect that the member is registered
          _ <- EitherT[Future, SequencerWriteError[
            RegisterMemberError
          ], CantonTimestamp](
            actuallyRegisteredF.map(Right(_)).recover { case NonFatal(throwable) =>
              logger.error("Failed waiting for the member registration event", throwable)
              Left(
                OperationError(
                  RegisterMemberError
                    .UnexpectedError(member, "Failed waiting for the member registration event")
                )
              )
            }
          )
        } yield ()
      }
    }
  }

  /** Important: currently both the disable member and the prune functionality on the block sequencer are
    * purely local operations that do not affect other block sequencers that share the same source of
    * events.
    */
  override def disableMember(member: Member)(implicit traceContext: TraceContext): Future[Unit] = {
    if (!stateManager.isMemberRegistered(member)) {
      logger.warn(s"disableMember attempted to use member [$member] but they are not registered")
      Future.unit
    } else if (!stateManager.isMemberEnabled(member)) {
      logger.debug(s"disableMember attempted to use member [$member] but they are already disabled")
      Future.unit
    } else {
      val disabledF =
        futureSupervisor.supervised(s"Waiting for member $member to be disabled")(
          stateManager.waitForMemberToBeDisabled(member)
        )
      for {
        _ <- placeLocalEvent(BlockSequencer.DisableMember(member))
        _ <- disabledF
      } yield ()
    }
  }

  override def acknowledge(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    logger.debug("Block sequencers only support signed acknowledgements")
    Future.unit
  }

  override protected def acknowledgeSignedInternal(
      signedAcknowledgeRequest: SignedContent[AcknowledgeRequest]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val req = signedAcknowledgeRequest.content
    logger.debug(s"Request for member ${req.member} to acknowledge timestamp ${req.timestamp}")
    val waitForAcknowledgementF =
      stateManager.waitForAcknowledgementToComplete(req.member, req.timestamp)
    for {
      _ <- blockSequencerOps.acknowledge(signedAcknowledgeRequest)
      _ <- waitForAcknowledgementF
    } yield ()
  }

  override def snapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[Future, String, SequencerSnapshot] =
    // TODO(#12676) Make sure that we don't request a snapshot for a state that was already pruned
    store
      .readStateForBlockContainingTimestamp(timestamp)
      .map(_.toSequencerSnapshot(protocolVersion))
      .leftMap(_ => s"Provided timestamp $timestamp is not linked to a block")

  override def isLedgerIdentityRegistered(identity: LedgerIdentity)(implicit
      traceContext: TraceContext
  ): Future[Boolean] = Future.successful(stateManager.isAuthorized(identity))

  override def authorizeLedgerIdentity(
      identity: LedgerIdentity
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    // Functionality was only implemented by the ethereum sequencer and has been removed for now.
    // Might be reintroduced once a sequencer governance api is defined.
    EitherT.leftT(s"authorizeLedgerIdentity is not implemented for $name sequencers")
  }

  override def pruningStatus(implicit traceContext: TraceContext): Future[SequencerPruningStatus] =
    store.pruningStatus().map(_.toSequencerPruningStatus(clock.now))

  /** Important: currently both the disable member and the prune functionality on the block sequencer are
    * purely local operations that do not affect other block sequencers that share the same source of
    * events.
    */
  override def prune(requestedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, PruningError, String] = {

    val pruningF = futureSupervisor.supervised(
      s"Waiting for local pruning operation at $requestedTimestamp to complete"
    )(stateManager.waitForPruningToComplete(requestedTimestamp))

    for {
      status <- EitherT.right[PruningError](this.pruningStatus)
      _ <- condUnitET[Future](
        requestedTimestamp <= status.safePruningTimestamp,
        UnsafePruningPoint(requestedTimestamp, status.safePruningTimestamp): PruningError,
      )
      _ <- EitherT.right(placeLocalEvent(BlockSequencer.Prune(requestedTimestamp)))
      msg <- EitherT.right(pruningF)
    } yield msg
  }

  private def placeLocalEvent(event: BlockSequencer.LocalEvent)(implicit
      traceContext: TraceContext
  ): Future[Unit] = localEventsQueue.offer(event).flatMap {
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

  override def pruningSchedulerBuilder: Option[Storage => PruningScheduler] = None

  override def pruningScheduler: Option[PruningScheduler] = None

  override protected def healthInternal(implicit
      traceContext: TraceContext
  ): Future[SequencerHealthStatus] =
    for {
      ledgerStatus <- blockSequencerOps.health
      isStorageActive = storage.isActive
      _ = logger.debug(s"Storage active: ${storage.isActive}")
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
      SyncCloseable("localEventsQueue.complete", localEventsQueue.complete()),
      AsyncCloseable(
        "localEventsQueue.watchCompletion",
        localEventsQueue.watchCompletion(),
        timeouts.shutdownProcessing,
      ),
      SyncCloseable("blockSequencerOps.close()", blockSequencerOps.close()),
      // The kill switch ensures that we don't process the remaining contents of the queue buffer
      SyncCloseable("killSwitch.shutdown()", killSwitch.shutdown()),
      AsyncCloseable("done", done, timeouts.shutdownProcessing),
      SyncCloseable("stateManager.close()", stateManager.close()),
    )
  }

  override def trafficStates: Future[Map[Member, TrafficState]] = {
    import TraceContext.Implicits.Empty.emptyTraceContext
    val state = stateManager.getHeadState.chunk.ephemeral.trafficState
    // Here the config is not as important since it's only used to compute the initial base rate amount which
    // will be removed from the final response anyway below in `getTrafficStatusFor`. So using the head snapshot is fine
    OptionUtil.zipWithFDefaultValue(
      rateLimitManager,
      cryptoApi.headSnapshot.ipsSnapshot.trafficControlParameters(protocolVersion),
      state,
    ) { case (rlm, parameters) =>
      rlm.updateTrafficStates(
        state,
        clock.now,
        parameters,
      )
    }
  }

  override def trafficStatus(requestedMembers: Seq[Member])(implicit
      traceContext: TraceContext
  ): Future[SequencerTrafficStatus] = {
    OptionUtil.zipWithFDefaultValue(
      rateLimitManager,
      cryptoApi.headSnapshot.ipsSnapshot.trafficControlParameters(protocolVersion),
      SequencerTrafficStatus(Seq.empty),
    ) { case (rlm, parameters) =>
      val headEphemeral = stateManager.getHeadState.chunk.ephemeral
      val requestedMembersSet = requestedMembers.toSet

      def filter(member: Member) = member match {
        case m @ (_: ParticipantId | _: MediatorId) =>
          m.isAuthenticated && Option
            .when(requestedMembersSet.nonEmpty)(requestedMembersSet)
            .forall(_.contains(m))
        case _ => false
      }

      val headSnapshot = cryptoApi.headSnapshot.ipsSnapshot
      val headTrafficSnapshot =
        headSnapshot.trafficControlStatus(requestedMembers)

      headEphemeral.status.members.toList
        .parFlatTraverse {
          case memberStatus if filter(memberStatus.member) =>
            headEphemeral.trafficState
              .get(memberStatus.member)
              .map { status =>
                headTrafficSnapshot
                  .map {
                    // Try to find the total extra traffic limit from the head snapshot
                    _.get(memberStatus.member).flatten.map(_.totalExtraTrafficLimit)
                  }
                  .map { topologyTrafficLimitOpt =>
                    List(
                      memberStatus.member ->
                        // If there's one, update the traffic status with it
                        topologyTrafficLimitOpt
                          .map(_.toNonNegative)
                          .map(status.update(_, headSnapshot.timestamp).valueOr { err =>
                            logger.info(
                              " Could not update traffic state with head topology snapshot",
                              err,
                            )
                            status
                          })
                          .getOrElse(status)
                    )
                  }
              }
              // If a member was previously registered but has never received a message and isn't in the sequencer initial state,
              // it won't be in the trafficStatus map, so we create a new state for it
              .getOrElse(
                rlm
                  .createNewTrafficStateAt(
                    memberStatus.member,
                    memberStatus.registeredAt,
                    parameters,
                  )
                  .map(memberStatus.member -> _)
                  .map(List(_))
              )
          case _ => Future.successful(List.empty)
        }
        .map(_.toMap)
        .flatMap(rlm.getTrafficStatusFor)
        .map(SequencerTrafficStatus)
    }
  }
}

object BlockSequencer {
  private case object CounterDiscriminator

  private type CounterDiscriminator = CounterDiscriminator.type
  private type BlockCounter = Counter[CounterDiscriminator]
  private val BlockCounter = Counter[CounterDiscriminator]

  private def ensureBlocksAreOrdered(
      initialBlockHeight: Option[Long]
  ): () => RawLedgerBlock => List[RawLedgerBlock] = { () =>
    // Using a peano queue to make sure that blocks are sourced in order even if they are added to the queue out of
    // order once in a while. If receiving from multiple peers, repeated blocks are ignored.
    val queue =
      new AtomicReference[Option[PeanoTreeQueue[CounterDiscriminator, RawLedgerBlock]]](None)
    (block: RawLedgerBlock) => {
      queue.compareAndSet(
        None,
        Some(
          PeanoTreeQueue[CounterDiscriminator, RawLedgerBlock](
            BlockCounter(initialBlockHeight.getOrElse(block.blockHeight))
          )
        ),
      )
      queue.get().fold[List[RawLedgerBlock]](List()) { q =>
        q.get(BlockCounter(block.blockHeight)) match {
          case NotInserted(_, _) => q.insert(BlockCounter(block.blockHeight), block): Unit
          case _ => ()
        }

        @tailrec
        def go(blocks: List[RawLedgerBlock]): List[RawLedgerBlock] =
          q.poll() match {
            case None => blocks
            case Some((_, block)) => go(blocks ++ List(block))
          }

        go(Nil)
      }
    }
  }

  sealed trait LocalEvent
  final case class DisableMember(member: Member) extends LocalEvent
  final case class Prune(timestamp: CantonTimestamp) extends LocalEvent
}
