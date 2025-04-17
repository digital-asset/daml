// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.crypto.{HashPurpose, SynchronizerCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.{ConfigTransform, ConfigTransforms}
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.scheduler.PruningScheduler
import com.digitalasset.canton.sequencing.client.SequencerClientSend
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficControlErrors
import com.digitalasset.canton.synchronizer.sequencer.Sequencer.RegisterError
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig.External
import com.digitalasset.canton.synchronizer.sequencer.admin.data.SequencerAdminStatus
import com.digitalasset.canton.synchronizer.sequencer.errors.{
  CreateSubscriptionError,
  SequencerAdministrationError,
  SequencerError,
}
import com.digitalasset.canton.synchronizer.sequencer.traffic.TimestampSelector.TimestampSelector
import com.digitalasset.canton.synchronizer.sequencer.traffic.{
  SequencerRateLimitError,
  SequencerRateLimitManager,
  SequencerTrafficStatus,
}
import com.digitalasset.canton.time.{Clock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{FutureUtil, MonadUtil}
import com.digitalasset.canton.{BaseTest, SequencerCounter}
import monocle.macros.syntax.lens.*
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.{Keep, Source}

import java.time.Duration
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}

/** Offers a hook into the [[Sequencer.send]] method to control the scheduling of messages in tests.
  * The hooks are identified by the `environmentId` and the `synchronizerAlias` and can be accessed
  * using [[ProgrammableSequencer.sequencers]].
  */
class ProgrammableSequencer(
    environmentId: String,
    instanceName: String,
    baseSequencer: Sequencer,
    clock: Clock,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends Sequencer
    with NamedLogging
    with FlagCloseable {
  import ProgrammableSequencer.QueuedSubmission

  override protected val timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing

  private[this] val sendPolicy: AtomicReference[SendPolicy] =
    new AtomicReference[SendPolicy](SendPolicy.default)

  private[this] val blockedMemberReads: AtomicReference[Map[Member, Promise[Unit]]] =
    new AtomicReference[Map[Member, Promise[Unit]]](Map.empty)

  /** The list of [[SubmissionRequest]]s on hold until a predicate Access to this field is
    * synchronized via [[semaphore]].
    */
  private[this] val queuedSubmissions: java.util.List[QueuedSubmission] =
    new java.util.LinkedList[QueuedSubmission]()

  /** Synchronizes access to [[queuedSubmissions]] */
  private[this] val semaphore: Semaphore = new Semaphore(1)

  override val rateLimitManager: Option[SequencerRateLimitManager] =
    baseSequencer.rateLimitManager

  override def trafficStatus(members: Seq[Member], selector: TimestampSelector)(implicit
      traceContext: com.digitalasset.canton.tracing.TraceContext
  ): FutureUnlessShutdown[
    SequencerTrafficStatus
  ] = baseSequencer.trafficStatus(members, selector)

  /** Run body with a given policy.
    *
    * Points to consider:
    *
    *   1. don't trigger console commands in your policies, this can cause a deadlock during
    *      shutdown. E.g., don't call mediator(synchronizer) in the policy, but precompute the
    *      mediator identity upfront and just refer to it in the policy
    *
    * 2. the ACS commitment processor blocks on sends. If you block its messages, you'll cause
    * shutdown issues.
    */
  def withSendPolicy[A](name: String, policy: SendPolicy)(body: => A): A = {
    var isSync = true
    val previous = sendPolicy.get()
    def setPreviousPolicy(): Unit = setPolicy("resetting previous policy")(previous)
    try {
      setPolicy(name)(policy)

      body match {
        case asyncResult: Future[_] =>
          isSync = false
          asyncResult.thereafter(_ => setPreviousPolicy()).asInstanceOf[A]

        case EitherT(value: Future[Either[_, _]] @unchecked) =>
          isSync = false
          EitherT(value.thereafter(_ => setPreviousPolicy())).asInstanceOf[A]

        case OptionT(value: Future[Option[_]] @unchecked) =>
          isSync = false
          OptionT(value.thereafter(_ => setPreviousPolicy())).asInstanceOf[A]

        case syncResult => syncResult
      }
    } finally if (isSync) setPreviousPolicy()
  }

  /** Variant of `withSendPolicy` with a send policy that does not take a trace context. */
  def withSendPolicy_[A](name: String, policy: SendPolicyWithoutTraceContext)(body: => A): A =
    withSendPolicy(name, _ => policy)(body)

  /** Reprogram with a new policy.
    *
    * Points to consider:
    *
    *   1. don't trigger console commands in your policies, this can cause a deadlock during
    *      shutdown. E.g., don't call mediator(synchronizer) in the policy, but precompute the
    *      mediator identity upfront and just refer to it in the policy
    *
    * 2. the ACS commitment processor blocks on sends. If you block its messages, you'll cause
    * shutdown issues.
    */
  def setPolicy(name: String)(newSendPolicy: SendPolicy): Unit = {
    import TraceContext.Implicits.Empty.*
    logger.debug(s"""Set a new policy "$name" for the programmable sequencer""")
    sendPolicy.set(newSendPolicy)
  }

  def setPolicy_(name: String)(
      newSendPolicyWithoutTraceContext: SendPolicyWithoutTraceContext
  ) = setPolicy(name)(_ => newSendPolicyWithoutTraceContext)

  def resetPolicy(): Unit = setPolicy("reset to default policy")(SendPolicy.default)

  /** All new subscriptions for these members will be not producing a single event. This needs to be
    * set-up BEFORE connecting to the synchronizer to take effect.
    */
  def blockFutureMemberRead(member: Member): Unit =
    blockedMemberReads
      .updateAndGet(previousBlockedMembers =>
        previousBlockedMembers.get(member) match {
          case Some(_) => throw new IllegalStateException("member already blocked")
          case None => previousBlockedMembers.+(member -> Promise())
        }
      )
      .discard

  def unBlockMemberRead(member: Member): Unit =
    blockedMemberReads.updateAndGet(previousBlockedMembers =>
      previousBlockedMembers.get(member) match {
        case Some(promise) =>
          promise.trySuccess(())
          previousBlockedMembers.-(member)

        case None =>
          throw new IllegalStateException("member not blocked")
      }
    )

  override def isRegistered(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    baseSequencer.isRegistered(member)

  override private[sequencer] def registerMemberInternal(
      member: Member,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RegisterError, Unit] =
    baseSequencer.registerMemberInternal(member, timestamp)

  override def acknowledgeSigned(
      signedAcknowledgeRequest: SignedContent[AcknowledgeRequest]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    baseSequencer.acknowledgeSigned(signedAcknowledgeRequest)

  override def prune(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, PruningError, String] =
    baseSequencer.prune(timestamp)

  override def locatePruningTimestamp(index: PositiveInt)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, PruningSupportError, Option[CantonTimestamp]] =
    baseSequencer.locatePruningTimestamp(index)

  override def reportMaxEventAgeMetric(
      oldestEventTimestamp: Option[CantonTimestamp]
  ): Either[PruningSupportError, Unit] =
    baseSequencer.reportMaxEventAgeMetric(oldestEventTimestamp)

  override def pruningSchedulerBuilder: Option[Storage => PruningScheduler] =
    baseSequencer.pruningSchedulerBuilder

  override def pruningScheduler: Option[PruningScheduler] = baseSequencer.pruningScheduler

  override def disableMember(member: Member)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerAdministrationError, Unit] =
    baseSequencer.disableMember(member)

  private def sendAsyncInternal(signedSubmission: SignedContent[SubmissionRequest])(
      send: SignedContent[SubmissionRequest] => EitherT[
        FutureUnlessShutdown,
        SequencerDeliverError,
        Unit,
      ]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] = {
    val submission = signedSubmission.content

    def scheduleAt(
        at: CantonTimestamp
    ): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] = {
      val promise = PromiseUnlessShutdown.unsupervised[Either[SequencerDeliverError, Unit]]()

      def run(ts: CantonTimestamp): Unit = {
        logger.debug(s"Processing delayed message ${submission.messageId} at $ts")
        val _ = promise.completeWithUS(sendAndCheck(signedSubmission).value)
      }

      FutureUtil.doNotAwait(
        clock.scheduleAt(run, at).unwrap,
        s"Programmable sequencer scheduled for message ID ${submission.messageId} at $at",
      )
      EitherT(promise.futureUS)
    }

    def sendAndCheck(
        submission: SignedContent[SubmissionRequest]
    ): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] =
      send(submission).map(_ => checkQueued(submission.content))

    blocking(semaphore.acquire())

    sendPolicy.get()(traceContext)(submission) match {
      case SendDecision.Process =>
        semaphore.release()
        logger.debug(s"Immediately allowing message ${submission.messageId}")
        sendAndCheck(signedSubmission)

      case SendDecision.Reject =>
        semaphore.release()
        logger.debug(s"Rejecting message ${submission.messageId}")
        val error = SequencerErrors.InternalTesting("Message rejected by send policy.")
        EitherT.leftT[FutureUnlessShutdown, Unit](error)

      case SendDecision.Drop =>
        semaphore.release()
        logger.debug(s"Dropping message ${submission.messageId}")
        EitherT.pure(())

      case SendDecision.Delay(duration) =>
        semaphore.release()
        val timestamp = clock.now.add(Duration.ofMillis(duration.toMillis))
        logger.debug(s"Delaying message ${submission.messageId} by $duration until $timestamp")
        scheduleAt(timestamp)

      case SendDecision.DelayUntil(timestamp) =>
        semaphore.release()
        logger.debug(s"Delaying message ${submission.messageId} until $timestamp")
        scheduleAt(timestamp)

      case SendDecision.HoldBack(releaseWhenCompleted) =>
        semaphore.release()
        logger.debug(s"Holding back message ${submission.messageId}")
        for {
          _ <- EitherT.right(releaseWhenCompleted).mapK(FutureUnlessShutdown.outcomeK)
          _ = logger.debug(s"Releasing message ${submission.messageId}")
          receipt <- sendAndCheck(signedSubmission)
        } yield receipt

      case SendDecision.OnHoldUntil(releaseAfter) =>
        logger.debug(s"Holding back message ${submission.messageId}")
        val queued = QueuedSubmission(signedSubmission, () => send(signedSubmission), releaseAfter)
        queuedSubmissions.add(queued)
        semaphore.release()
        EitherT(queued.resultPromise.futureUS)

      case SendDecision.Replace(replacement, more*) =>
        semaphore.release()
        logger.debug(s"Replacing message ${submission.messageId} with $replacement")
        MonadUtil.sequentialTraverseMonoid(replacement +: more)(sendAndCheck)
    }
  }

  override def sendAsyncSigned(signedSubmission: SignedContent[SubmissionRequest])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] =
    sendAsyncInternal(signedSubmission) { toSend =>
      baseSequencer.sendAsyncSigned(toSend)
    }

  override def read(member: Member, offset: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CreateSubscriptionError, Sequencer.SequencedEventSource] =
    blockedMemberReads.get.get(member) match {
      case Some(promise) =>
        logger.debug(s"Blocking sequencer source for member $member")
        EitherT.right[CreateSubscriptionError](
          FutureUnlessShutdown.pure {
            Source
              .lazyFutureSource(() =>
                promise.future
                  .flatMap(_ => baseSequencer.read(member, offset).value.unwrap)
                  .map(
                    _.onShutdown(throw new IllegalStateException("Sequencer shutting down")).left
                      .map(err => throw new IllegalStateException(s"Sequencer failed with $err"))
                      .merge
                  )
              )
              .viaMat(KillSwitches.single)(Keep.right)
              .watchTermination()((mat, fd) => (mat, FutureUnlessShutdown.outcomeF(fd)))
          }
        )

      case None =>
        logger.debug(s"Member $member is not blocked, emitting sequencer source")
        baseSequencer.read(member, offset)
    }

  override def readV2(member: Member, timestamp: Option[CantonTimestamp])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CreateSubscriptionError, Sequencer.SequencedEventSource] =
    blockedMemberReads.get.get(member) match {
      case Some(promise) =>
        logger.debug(s"Blocking sequencer source for member $member")
        EitherT.right[CreateSubscriptionError](
          FutureUnlessShutdown.pure {
            Source
              .lazyFutureSource(() =>
                promise.future
                  .flatMap(_ => baseSequencer.readV2(member, timestamp).value.unwrap)
                  .map(
                    _.onShutdown(throw new IllegalStateException("Sequencer shutting down")).left
                      .map(err => throw new IllegalStateException(s"Sequencer failed with $err"))
                      .merge
                  )
              )
              .viaMat(KillSwitches.single)(Keep.right)
              .watchTermination()((mat, fd) => (mat, FutureUnlessShutdown.outcomeF(fd)))
          }
        )

      case None =>
        logger.debug(s"Member $member is not blocked, emitting sequencer source")
        baseSequencer.readV2(member, timestamp)
    }

  override def pruningStatus(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerPruningStatus] =
    baseSequencer.pruningStatus

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerError, SequencerSnapshot] =
    baseSequencer.snapshot(timestamp)

  override def awaitSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerError, SequencerSnapshot] =
    baseSequencer.awaitSnapshot(timestamp)

  override protected def onClosed(): Unit = {
    ProgrammableSequencer.sequencers.remove(environmentId -> instanceName)
    baseSequencer.close()
  }

  @SuppressWarnings(Array("org.wartremover.warts.While"))
  private[this] def checkQueued(sent: SubmissionRequest): Unit = {
    import TraceContext.Implicits.Empty.*
    blocking(semaphore.acquire())
    try {
      val iterator = queuedSubmissions.iterator
      while (iterator.hasNext) {
        val queued = iterator.next()
        if (queued.releaseAfter(sent)) {
          iterator.remove()
          logger.debug(
            s"Releasing message ${queued.submission.content.messageId} upon message ${sent.messageId}"
          )
          queued.resultPromise.completeWithUS(queued.send().value)
        }
      }
    } finally {
      semaphore.release()
    }
  }

  override def setTrafficPurchased(
      member: Member,
      serial: PositiveInt,
      totalTrafficPurchased: NonNegativeLong,
      sequencerClient: SequencerClientSend,
      synchronizerTimeTracker: SynchronizerTimeTracker,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TrafficControlErrors.TrafficControlError,
    Unit,
  ] = baseSequencer.setTrafficPurchased(
    member,
    serial,
    totalTrafficPurchased,
    sequencerClient,
    synchronizerTimeTracker,
  )

  override def getTrafficStateAt(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerRateLimitError.TrafficNotFound, Option[TrafficState]] =
    baseSequencer.getTrafficStateAt(member, timestamp)

  override def adminStatus: SequencerAdminStatus = baseSequencer.adminStatus

  override def isEnabled(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    baseSequencer.isEnabled(member)

  /** Return the last timestamp of the containing block of the provided timestamp. This is needed to
    * determine the effective timestamp to observe in topology processing, required to produce a
    * correct snapshot.
    */
  override def awaitContainingBlockLastTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerError, CantonTimestamp] =
    baseSequencer.awaitContainingBlockLastTimestamp(timestamp)
}

/** Utilities for using the [[ProgrammableSequencer]] from tests */
trait HasProgrammableSequencer {
  this: BaseTest =>

  /** Lookup a [[ProgrammableSequencer]] that has been previously created and registered by a canton
    * node started in an integration test. Fails if instance cannot be found.
    */
  def getProgrammableSequencer(name: String): ProgrammableSequencer = {
    val environmentId = this.getClass.toString
    ProgrammableSequencer
      .sequencer(environmentId, name)
      .getOrElse {
        val availableSequencers = ProgrammableSequencer.allSequencers(environmentId)
        fail(
          show"Programmable sequencer not found. Available sequencers: $availableSequencers"
        )
      }
  }

  // Sign a (modified) submission request
  def signModifiedSubmissionRequest(
      request: SubmissionRequest,
      syncCrypto: SynchronizerCryptoClient,
  )(implicit executionContext: ExecutionContext): SignedContent[SubmissionRequest] =
    SignedContent
      .create(
        syncCrypto.pureCrypto,
        syncCrypto.currentSnapshotApproximation,
        request,
        None,
        HashPurpose.SubmissionRequestSignature,
        testedProtocolVersion,
      )
      .valueOrFailShutdown("sign")
      .futureValue
}

object ProgrammableSequencer {

  private[ProgrammableSequencer] val sequencers
      : concurrent.Map[(String, String), ProgrammableSequencer] =
    new TrieMap[(String, String), ProgrammableSequencer]

  /** Obtain a reference to a programmable sequencer that has been installed via a configuration
    * using [[configOverride]].
    */
  def sequencer(environmentId: String, name: String): Option[ProgrammableSequencer] =
    sequencers.get(environmentId -> name)

  private[sequencer] def allSequencers(environmentId: String): Seq[String] =
    sequencers.keySet.collect { case (`environmentId`, synchronizer) => synchronizer }.toSeq

  /** Changes the [[CantonEnterpriseConfig]] such that all sequencers use a programmable sequencer.
    *
    * @param environmentId
    *   Identifier used to identify the programmable sequencer. Identifiers must not be re-used
    *   across environments, i.e., tests.
    */
  def configOverride(environmentId: String, loggerFactory: NamedLoggerFactory): ConfigTransform = {
    def createAndStoreProgrammableSequencer(
        instanceName: String
    )(clock: Clock)(baseSequencer: Sequencer)(ec: ExecutionContext): ProgrammableSequencer = {
      val programmableSequencer =
        new ProgrammableSequencer(environmentId, instanceName, baseSequencer, clock, loggerFactory)(
          ec
        )
      val previous = sequencers.putIfAbsent(environmentId -> instanceName, programmableSequencer)
      if (previous.isDefined)
        throw new IllegalStateException(
          s"Programmable sequencer for synchronizer $instanceName exists already in $environmentId."
        )
      programmableSequencer
    }

    def intercept(
        instanceName: String
    )(config: SequencerConfig): SequencerConfig =
      config match {
        case database: SequencerConfig.Database =>
          database.copy(
            testingInterceptor = Some(createAndStoreProgrammableSequencer(instanceName))
          )
        case external: External =>
          external
            .focus(_.block.testingInterceptor)
            .replace(Some(createAndStoreProgrammableSequencer(instanceName)))

        case bft: SequencerConfig.BftSequencer =>
          bft
            .focus(_.block.testingInterceptor)
            .replace(
              Some(createAndStoreProgrammableSequencer(instanceName))
            )
      }

    val updateSequencerConfigs = ConfigTransforms.updateAllSequencerConfigs {
      (instanceName, sequencerConfig) =>
        sequencerConfig.focus(_.sequencer).modify(intercept(instanceName))
    }
    updateSequencerConfigs
  }

  private final case class QueuedSubmission(
      submission: SignedContent[SubmissionRequest],
      send: () => EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit],
      releaseAfter: SubmissionRequest => Boolean,
  ) {
    val resultPromise: PromiseUnlessShutdown[Either[SequencerDeliverError, Unit]] =
      PromiseUnlessShutdown.unsupervised()
  }
}
