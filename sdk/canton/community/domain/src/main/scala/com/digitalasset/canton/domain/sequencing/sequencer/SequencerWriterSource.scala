// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.data.{EitherT, Validated}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.option.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError.{
  ExceededMaxSequencingTime,
  PayloadToEventTimeBoundExceeded,
}
import com.digitalasset.canton.domain.sequencing.sequencer.store.*
import com.digitalasset.canton.error.BaseCantonError
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.sequencing.protocol.{
  Deliver,
  DeliverError,
  SendAsyncError,
  SequencerErrors,
  SubmissionRequest,
}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.BatchTracing.withTracedBatch
import com.digitalasset.canton.tracing.{HasTraceContext, TraceContext, Traced}
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.retry.RetryUtil.DbExceptionRetryable
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{Flow, GraphDSL, Keep, Merge, Source}

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContext, Future}

/** A write we want to make to the db */
sealed trait Write
object Write {
  final case class Event(event: Presequenced[StoreEvent[PayloadId]]) extends Write
  case object KeepAlive extends Write
}

/** A write that we've assigned a timestamp to.
  * We drag these over the same clock so we can ensure earlier items have lower timestamps and later items have higher timestamps.
  * This is very helpful, essential you may say, for correctly setting the watermark while ensuring an event with
  * an earlier timestamp will not be written.
  */
sealed trait SequencedWrite extends HasTraceContext {

  /** The sequencing timestamp assigned to the write */
  def timestamp: CantonTimestamp
}

object SequencedWrite {
  final case class Event(event: Sequenced[PayloadId]) extends SequencedWrite {
    override lazy val timestamp: CantonTimestamp = event.timestamp
    override def traceContext: TraceContext = event.traceContext
  }
  final case class KeepAlive(override val timestamp: CantonTimestamp) extends SequencedWrite {
    override def traceContext: TraceContext = TraceContext.empty
  }
}

final case class BatchWritten(notifies: WriteNotification, latestTimestamp: CantonTimestamp)
object BatchWritten {

  /** Assumes events are ordered by timestamp */
  def apply(events: NonEmpty[Seq[Sequenced[_]]]): BatchWritten =
    BatchWritten(
      notifies = WriteNotification(events),
      latestTimestamp = events.last1.timestamp,
    )
}

/** Base class for exceptions intentionally thrown during Pekko stream to flag errors */
sealed abstract class SequencerWriterException(message: String) extends RuntimeException(message)

/** Throw as an error in the pekko stream when we discover that our currently running sequencer writer has been
  * marked as offline.
  */
class SequencerOfflineException(instanceIndex: Int)
    extends SequencerWriterException(
      s"This sequencer (instance:$instanceIndex) has been marked as offline"
    )

/** We intentionally use an unsafe storage method for writing payloads to take advantage of a full connection pool
  * for performance. However this means if a HA Sequencer Writer has lost its instance lock it may still attempt to
  * write payloads while another Sequencer Writer is active with the same instance index. As we use this instance
  * index to generate an (almost) conflict free payload id, in this circumstance there is a slim chance that we
  * may attempt to write conflicting payloads with the same id. If we were using a simple idempotent write approach
  * this could result in the active sequencer writing an event with a payload from the offline writer process (and
  * not the payload it is expecting). This would be a terrible and difficult to diagnose corruption issue.
  *
  * If this exception is raised we currently just halt the writer and run crash recovery. This is slightly suboptimal
  * as in the above scenario we may crash the active writer (if they were second to write a conflicting payload id).
  * However this will be safe. We could optimise this by checking the active lock status and only halting
  * if this is found to be false.
  */
class ConflictingPayloadIdException(payloadId: PayloadId, conflictingInstanceDiscriminator: UUID)
    extends SequencerWriterException(
      s"We attempted to write a payload with an id that already exists [$payloadId] written by instance $conflictingInstanceDiscriminator"
    )

/** A payload that we should have just stored now seems to be missing. */
class PayloadMissingException(payloadId: PayloadId)
    extends SequencerWriterException(s"Payload missing after storing [$payloadId]")

class SequencerWriterQueues private[sequencer] (
    eventGenerator: SendEventGenerator,
    protected val loggerFactory: NamedLoggerFactory,
)(
    @VisibleForTesting
    private[sequencer] val deliverEventQueue: BoundedSourceQueue[Presequenced[StoreEvent[Payload]]],
    keepAliveKillSwitch: UniqueKillSwitch,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {
  private val closing = new AtomicBoolean(false)

  def send(
      submission: SubmissionRequest
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncError, Unit] =
    writeInternal(Left(submission))

  def blockSequencerWrite(
      outcome: SubmissionRequestOutcome
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncError, Unit] =
    writeInternal(Right(outcome))

  /** Accepts both submission requests (DBS flow) as `Left` and submission outcomes (BS flow) as `Right`.
    */
  private def writeInternal(
      submissionOrOutcome: Either[SubmissionRequest, SubmissionRequestOutcome]
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncError, Unit] = {
    for {
      event <- eventGenerator.generate(submissionOrOutcome)
      enqueueResult = deliverEventQueue.offer(event)
      _ <- EitherT.fromEither[Future](enqueueResult match {
        case QueueOfferResult.Enqueued => Right(())
        case QueueOfferResult.Dropped =>
          Left(SendAsyncError.Overloaded("Sequencer event buffer is full"): SendAsyncError)
        case QueueOfferResult.QueueClosed => Left(SendAsyncError.ShuttingDown())
        case other =>
          logger.warn(s"Unexpected result from payload queue offer: $other")
          Right(())
      })
    } yield ()
  }

  def complete(): Unit = {
    implicit val tc: TraceContext = TraceContext.empty
    // the queue completions throw IllegalStateExceptions if you call close more than once
    // so guard to ensure they're only called once
    if (closing.compareAndSet(false, true)) {
      logger.debug(s"Shutting down keep-alive kill switch")
      keepAliveKillSwitch.shutdown()
      logger.debug(s"Completing deliver event queue")
      deliverEventQueue.complete()
    }
  }
}

/** Pekko stream for writing as a Sequencer */
object SequencerWriterSource {
  def apply(
      writerConfig: SequencerWriterConfig,
      totalNodeCount: PositiveInt,
      keepAliveInterval: Option[NonNegativeFiniteDuration],
      store: SequencerWriterStore,
      clock: Clock,
      eventSignaller: EventSignaller,
      loggerFactory: NamedLoggerFactory,
      protocolVersion: ProtocolVersion,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Source[Traced[BatchWritten], SequencerWriterQueues] = {
    val logger = TracedLogger(SequencerWriterSource.getClass, loggerFactory)

    val eventTimestampGenerator =
      new PartitionedTimestampGenerator(clock, store.instanceIndex, totalNodeCount)
    val payloadIdGenerator =
      new PartitionedTimestampGenerator(clock, store.instanceIndex, totalNodeCount)
    // when running an HA sequencer we typically rely on the lock based [[resource.DbStorageMulti]] to ensure that
    // there are no other writers sharing the same instance index concurrently writing. however for performance reasons
    // we in places forgo this and use a non-lock protected based "unsafe" methods. We then separately use a unique
    // instance discriminator to check that our writes are conflict free (currently used solely for payloads).
    val instanceDiscriminator = UUID.randomUUID()

    // log this instance discriminator so in the exceptionally unlikely event that we actually hit conflicts we have
    // an available approach for determining which instances were conflicting
    logger.debug(
      s"Starting sequencer writer stream with index ${store.instanceIndex} of $totalNodeCount and instance discriminator [$instanceDiscriminator]"
    )

    val eventGenerator = new SendEventGenerator(
      store,
      () => PayloadId(payloadIdGenerator.generateNext),
      protocolVersion,
    )

    // Take deliver events with full payloads and first write them before adding them to the events queue
    val deliverEventSource = Source
      .queue[Presequenced[StoreEvent[Payload]]](writerConfig.payloadQueueSize)
      .via(WritePayloadsFlow(writerConfig, store, instanceDiscriminator, loggerFactory))
      .map(Write.Event)

    // push keep alive writes at the specified interval, or never if not set
    val keepAliveSource = keepAliveInterval
      .fold(Source.never[Write.KeepAlive.type]) { frequency =>
        Source.repeat(Write.KeepAlive).throttle(1, frequency.toScala)
      }
      .viaMat(KillSwitches.single)(Keep.right)

    val mkMaterialized = new SequencerWriterQueues(eventGenerator, loggerFactory)(_, _)

    // merge the sources of deliver events and keep-alive writes
    val mergedEventsSource =
      Source.fromGraph(
        GraphDSL.createGraph(deliverEventSource, keepAliveSource)(mkMaterialized) {
          implicit builder => (deliverEventSourceS, keepAliveSourceS) =>
            import GraphDSL.Implicits.*

            val merge = builder.add(Merge[Write](inputPorts = 2))

            deliverEventSourceS ~> merge.in(0)
            keepAliveSourceS ~> merge.in(1)

            SourceShape(merge.out)
        }
      )

    mergedEventsSource
      .via(
        AssertMonotonicBlockSequencerTimestampsFlow(loggerFactory)
      )
      .via(
        SequenceWritesFlow(
          writerConfig,
          store,
          eventTimestampGenerator,
          loggerFactory,
          protocolVersion,
        )
      )
      // Merge watermark updating in case we are running slow here
      .conflate[Traced[BatchWritten]] { case (tracedLeft, tracedRight) =>
        tracedLeft.withTraceContext { _ => left =>
          tracedRight.map(right =>
            BatchWritten(
              left.notifies.union(right.notifies),
              left.latestTimestamp.max(right.latestTimestamp),
            )
          )
        }
      }
      .via(UpdateWatermarkFlow(store, logger))
      .via(NotifyEventSignallerFlow(eventSignaller))
  }
}

class SendEventGenerator(
    store: SequencerWriterStore,
    payloadIdGenerator: () => PayloadId,
    protocolVersion: ProtocolVersion,
)(implicit
    executionContext: ExecutionContext
) {
  def generate(
      submissionOrOutcome: Either[SubmissionRequest, SubmissionRequestOutcome]
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncError, Presequenced[StoreEvent[Payload]]] = {
    val submission = submissionOrOutcome.map(_.submission).merge
    val trafficStateO = submissionOrOutcome.fold(
      _ => None,
      _.memberTrafficState(submission.sender),
    )
    def lookupSender: EitherT[Future, SendAsyncError, SequencerMemberId] = EitherT(
      store
        .lookupMember(submission.sender)
        .map(
          _.map(_.memberId)
            .toRight(
              SendAsyncError
                .SenderUnknown(s"sender [${submission.sender}] is unknown"): SendAsyncError
            )
        )
    )

    def validateRecipient(member: Member): Future[Validated[Member, SequencerMemberId]] =
      for {
        registeredMember <- store.lookupMember(member)
        memberIdO = registeredMember.map(_.memberId)
      } yield memberIdO.toRight(member).toValidated

    def validateRecipients(
        recipients: Set[Member]
    ): Future[Validated[NonEmpty[Seq[Member]], Set[SequencerMemberId]]] =
      for {
        // TODO(#12363) Support group addresses in the DB Sequencer
        validatedSeq <- recipients.toSeq
          .parTraverse(validateRecipient)
        validated = validatedSeq.traverse(_.leftMap(NonEmpty(Seq, _)))
      } yield validated.map(_.toSet)

    def validateAndGenerateEvent(
        senderId: SequencerMemberId
    ): Future[StoreEvent[Payload]] = {
      def unknownRecipientsDeliverError(
          unknownRecipients: NonEmpty[Seq[Member]]
      ): DeliverErrorStoreEvent = {
        val error = SequencerErrors.UnknownRecipients(unknownRecipients)

        DeliverErrorStoreEvent(
          senderId,
          submission.messageId,
          error.rpcStatusWithoutLoggingContext(),
          protocolVersion,
          traceContext,
          trafficStateO,
        )
      }

      def deliver(recipientIds: Set[SequencerMemberId]): StoreEvent[Payload] = {
        val payload =
          Payload(
            payloadIdGenerator(),
            submissionOrOutcome
              .fold(
                _.batch,
                _.aggregatedBatchO.getOrElse(submission.batch),
              )
              .toByteString,
          )
        DeliverStoreEvent.ensureSenderReceivesEvent(
          senderId,
          submission.messageId,
          recipientIds,
          payload,
          submission.topologyTimestamp,
          trafficStateO,
        )
      }

      val recipients = submissionOrOutcome.fold(
        _.batch.allMembers,
        _.deliverToMembers,
      )
      for {
        validatedRecipients <- validateRecipients(recipients)
      } yield validatedRecipients.fold(unknownRecipientsDeliverError, deliver)
    }

    val extDeliverErrorO = submissionOrOutcome.fold(
      _ => None,
      _.receiptOrErrorO, // TODO(#18395): Refactor this into the separate receipt and error outcomes
    )

    for {
      senderId <- lookupSender
      event <- {
        EitherT.right[SendAsyncError](
          extDeliverErrorO match {
            case Some(extDeliverError) =>
              Future.successful(
                extDeliverError match {
                  // This only happens for deliver receipts with an empty batch
                  case event: Deliver[_] =>
                    require(event.batch.envelopes.isEmpty, s"Expected an empty batch for: $event")
                    val payload =
                      Payload(
                        // TODO(#18405): Consider using the event timestamp also for payloads for the BS/US flow
                        payloadIdGenerator(),
                        event.batch.toByteString,
                      )
                    DeliverStoreEvent.ensureSenderReceivesEvent(
                      senderId,
                      submission.messageId,
                      Set(senderId),
                      payload,
                      submission.topologyTimestamp,
                      trafficStateO,
                    ): StoreEvent[Payload]
                  case err: DeliverError =>
                    DeliverErrorStoreEvent(
                      senderId,
                      submission.messageId,
                      err.reason,
                      protocolVersion,
                      traceContext,
                      trafficStateO,
                    ): StoreEvent[Payload]
                }
              )
            case None => validateAndGenerateEvent(senderId)
          }
        )
      }
    } yield Presequenced.withMaxSequencingTime(
      event,
      submission.maxSequencingTime,
      blockSequencerTimestampO = submissionOrOutcome
        .fold(
          _ => None,
          outcome => Some(outcome.sequencingTime),
        ),
    )
  }
}

// Akka flow that asserts that event timestamp is monotonically increasing
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object AssertMonotonicBlockSequencerTimestampsFlow {
  def apply(
      loggerFactory: NamedLoggerFactory
  )(implicit
      traceContext: TraceContext
  ): Flow[Write, Write, NotUsed] = {
    val logger = TracedLogger(WritePayloadsFlow.getClass, loggerFactory)

    Flow[Write]
      .statefulMapConcat { () =>
        var lastTimestamp: Option[CantonTimestamp] = None
        write =>
          {
            val timestampO = write match {
              case Write.Event(event) =>
                event.blockSequencerTimestampO
              case Write.KeepAlive =>
                None
            }

            timestampO match {
              case Some(blockSequencerTimestamp) =>
                if (lastTimestamp.exists(_ > blockSequencerTimestamp)) {
                  logger.warn(
                    s"Block sequencer timestamp is not monotonically increasing: " +
                      s"lastTimestamp=${lastTimestamp}, blockSequencerTimestamp=$blockSequencerTimestamp"
                  )
                }
                lastTimestamp = Some(blockSequencerTimestamp)
              case None =>
            }
          }

          Seq(write)
      }
  }
}

object SequenceWritesFlow {
  def apply(
      writerConfig: SequencerWriterConfig,
      store: SequencerWriterStore,
      eventTimestampGenerator: PartitionedTimestampGenerator,
      loggerFactory: NamedLoggerFactory,
      protocolVersion: ProtocolVersion,
  )(implicit executionContext: ExecutionContext): Flow[Write, Traced[BatchWritten], NotUsed] = {
    val logger = TracedLogger(WritePayloadsFlow.getClass, loggerFactory)

    def sequenceWritesAndStoreEvents(writes: Seq[Write]): Future[Traced[Option[BatchWritten]]] =
      NonEmpty
        .from(writes.map(sequenceWrite))
        // due to the groupedWithin we should likely always have items
        .fold(Future.successful[Traced[Option[BatchWritten]]](Traced.empty(None))) { writes =>
          withTracedBatch(logger, writes) { implicit traceContext => writes =>
            val events: Option[NonEmpty[Seq[Sequenced[PayloadId]]]] =
              NonEmpty.from(writes.collect { case SequencedWrite.Event(event) =>
                event
              })
            val notifies =
              events.fold[WriteNotification](WriteNotification.None)(WriteNotification(_))
            for {
              // if this write batch had any events then save them
              _ <- events.fold(Future.unit)(store.saveEvents)
            } yield Traced(BatchWritten(notifies, writes.last1.timestamp).some)
          }
        }

    def sequenceWrite(write: Write): SequencedWrite = {
      val timestamp = eventTimestampGenerator.generateNext

      write match {
        case Write.KeepAlive => SequencedWrite.KeepAlive(timestamp)
        // if we opt not to write the event as we're past the max-sequencing-time, just replace with a keep alive as we're still alive
        case Write.Event(event) =>
          // TODO(#18401): Explode if a unified sequencer flow has an empty blockSequencerTimestampO
          val sequencingTimestamp = event.blockSequencerTimestampO match {
            case Some(blockSequencerTimestamp) => blockSequencerTimestamp
            case None => timestamp
          }

          sequenceEvent(sequencingTimestamp, event)
            .map(SequencedWrite.Event)
            .getOrElse[SequencedWrite](SequencedWrite.KeepAlive(sequencingTimestamp))
      }
    }

    /* Performs checks and validations that require knowing the sequencing timestamp of the event.
     * May transform the event into an error (if the requested signing timestamp is out of bounds).
     * May drop the event entirely if the max sequencing time has been exceeded.
     */
    def sequenceEvent(
        timestamp: CantonTimestamp,
        presequencedEvent: Presequenced[StoreEvent[PayloadId]],
    ): Option[Sequenced[PayloadId]] = {
      def checkMaxSequencingTime(
          event: Presequenced[StoreEvent[PayloadId]]
      ): Either[BaseCantonError, Presequenced[StoreEvent[PayloadId]]] =
        event.maxSequencingTimeO
          .toLeft(event)
          .leftFlatMap { maxSequencingTime =>
            Either.cond(
              timestamp <= maxSequencingTime,
              event,
              ExceededMaxSequencingTime.Error(timestamp, maxSequencingTime, event.event.description),
            )
          }

      def checkTopologyTimestamp(
          event: Presequenced[StoreEvent[PayloadId]]
      ): Presequenced[StoreEvent[PayloadId]] =
        event.map {
          // we only do this validation for deliver events that specify a signing timestamp
          case deliver @ DeliverStoreEvent(
                sender,
                messageId,
                _,
                _,
                Some(topologyTimestamp),
                _,
                trafficStateO,
              ) =>
            // We only check that the signing timestamp is at most the assigned timestamp.
            // The lower bound will be checked only when reading the event
            // because only then we know the topology state at the signing timestamp,
            // which we need to determine the dynamic domain parameter sequencerSigningTolerance.
            //
            // Sequencer clients should set the signing timestamp only to timestamps that they have read from the
            // domain. In a setting with multiple sequencers, the SequencerReader delivers only events up to
            // the lowest watermark of all sequencers. So even if the sequencer client sends a follow-up submission request
            // to a different sequencer, this sequencer will assign a higher timestamp than the requested topology timestamp.
            // So this check should only fail if the sequencer client violates this policy.
            if (topologyTimestamp <= timestamp) deliver
            else {
              val reason = SequencerErrors
                .TopologyTimestampAfterSequencingTimestamp(topologyTimestamp, timestamp)

              DeliverErrorStoreEvent(
                sender,
                messageId,
                reason.rpcStatusWithoutLoggingContext(),
                protocolVersion,
                event.traceContext,
                trafficStateO,
              )
            }
          case other => other
        }

      def checkPayloadToEventMargin(
          presequencedEvent: Presequenced[StoreEvent[PayloadId]]
      ): Either[BaseCantonError, Presequenced[StoreEvent[PayloadId]]] =
        presequencedEvent match {
          // we only need to check deliver events for payloads
          // the only reason why
          case presequencedDeliver @ Presequenced(deliver: DeliverStoreEvent[PayloadId], _, _) =>
            val payloadTs = deliver.payload.unwrap
            val bound = writerConfig.payloadToEventMargin
            val maxAllowableEventTime = payloadTs.add(bound.asJava)
            Either
              .cond(
                timestamp <= maxAllowableEventTime,
                presequencedDeliver,
                PayloadToEventTimeBoundExceeded.Error(
                  bound.duration,
                  payloadTs,
                  sequencedTs = timestamp,
                  messageId = deliver.messageId,
                ),
              )
          case other =>
            Right(other)
        }

      val resultE = for {
        event <- checkPayloadToEventMargin(presequencedEvent)
        event <- checkMaxSequencingTime(event)
      } yield event

      resultE match {
        case Left(error) =>
          // log here as we don't have the trace context in the error itself
          implicit val errorLoggingContext =
            ErrorLoggingContext(logger, loggerFactory.properties, presequencedEvent.traceContext)
          error.log()
          None
        case Right(event) =>
          val checkedEvent = checkTopologyTimestamp(event)
          Some(Sequenced(timestamp, checkedEvent.event))
      }
    }

    Flow[Write]
      .groupedWithin(
        writerConfig.eventWriteBatchMaxSize,
        writerConfig.eventWriteBatchMaxDuration.underlying,
      )
      .mapAsync(1)(sequenceWritesAndStoreEvents)
      .collect { case tew @ Traced(Some(ew)) => tew.map(_ => ew) }
      .named("sequenceAndWriteEvents")
  }
}

/** Extract the payloads of events and write them in batches to the payloads table.
  * As order does not matter at this point allow writing batches concurrently up to
  * the concurrency specified by [[SequencerWriterConfig.payloadWriteMaxConcurrency]].
  * Pass on the events with the payloads dropped and replaced by their payload ids.
  */
object WritePayloadsFlow {
  def apply(
      writerConfig: SequencerWriterConfig,
      store: SequencerWriterStore,
      instanceDiscriminator: UUID,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): Flow[Presequenced[StoreEvent[Payload]], Presequenced[StoreEvent[PayloadId]], NotUsed] = {
    val logger = TracedLogger(WritePayloadsFlow.getClass, loggerFactory)

    def writePayloads(
        events: Seq[Presequenced[StoreEvent[Payload]]]
    ): Future[Seq[Presequenced[StoreEvent[PayloadId]]]] = {
      if (events.isEmpty) Future.successful(Seq.empty[Presequenced[StoreEvent[PayloadId]]])
      else {
        implicit val traceContext: TraceContext = TraceContext.ofBatch(events)(logger)
        // extract the payloads themselves for storing
        val payloads = events.map(_.event).flatMap(extractPayload(_).toList)

        // strip out the payloads and replace with their id as the content itself is not needed downstream
        val eventsWithPayloadId = events.map(_.map(e => dropPayloadContent(e)))
        logger.debug(s"Writing ${payloads.size} payloads from batch of ${events.size}")

        // save the payloads if there are any
        EitherTUtil.toFuture {
          NonEmpty
            .from(payloads)
            .traverse_(store.savePayloads(_, instanceDiscriminator))
            .leftMap {
              case SavePayloadsError.ConflictingPayloadId(id, conflictingInstance) =>
                new ConflictingPayloadIdException(id, conflictingInstance)
              case SavePayloadsError.PayloadMissing(id) => new PayloadMissingException(id)
            }
            .map((_: Unit) => eventsWithPayloadId)
        }
      }
    }

    def extractPayload(event: StoreEvent[Payload]): Option[Payload] = event match {
      case DeliverStoreEvent(_, _, _, payload, _, _, _) => payload.some
      case _other => None
    }

    def dropPayloadContent(event: StoreEvent[Payload]): StoreEvent[PayloadId] = event match {
      case deliver: DeliverStoreEvent[Payload] => deliver.mapPayload(_.id)
      case error: DeliverErrorStoreEvent => error
    }

    Flow[Presequenced[StoreEvent[Payload]]]
      .groupedWithin(
        writerConfig.payloadWriteBatchMaxSize,
        writerConfig.payloadWriteBatchMaxDuration.underlying,
      )
      .mapAsync(writerConfig.payloadWriteMaxConcurrency)(
        writePayloads(_)
      ) // TODO(#18394): make a switch with the flag for .mapAsyncUnordered, if this affects performance
      .mapConcat(identity)
      .named("writePayloads")
  }

}

object UpdateWatermarkFlow {
  def apply(store: SequencerWriterStore, logger: TracedLogger)(implicit
      executionContext: ExecutionContext
  ): Flow[Traced[BatchWritten], Traced[BatchWritten], NotUsed] = {
    Flow[Traced[BatchWritten]]
      .mapAsync(1)(_.withTraceContext { implicit traceContext => written =>
        for {
          _ <- store
            .saveWatermark(written.latestTimestamp)
            .value
            .map {
              case Left(SaveWatermarkError.WatermarkFlaggedOffline) =>
                // intentionally throwing exception that will bubble up through the pekko stream and handled by the
                // recovery process in SequencerWriter
                throw new SequencerOfflineException(store.instanceIndex)
              case _ => ()
            }
            // This is a workaround to avoid failing during shutdown that can be removed once saveWatermark returns
            // a FutureUnlessShutdown
            .recover {
              case exception if DbExceptionRetryable.retryOKForever(exception, logger) =>
                // The exception itself is already being logged above by retryOKForever. Only logging here additional
                // context
                logger.info(
                  "Saving watermark failed with a retryable error. This can happen during shutdown." +
                    " The error will be ignored to allow the shutdown to proceed."
                )
            }
        } yield Traced(written)
      })
      .named("updateWatermark")
  }
}

object NotifyEventSignallerFlow {
  def apply(eventSignaller: EventSignaller)(implicit
      executionContext: ExecutionContext
  ): Flow[Traced[BatchWritten], Traced[BatchWritten], NotUsed] =
    Flow[Traced[BatchWritten]]
      .mapAsync(1)(_.withTraceContext { implicit traceContext => batchWritten =>
        eventSignaller.notifyOfLocalWrite(batchWritten.notifies) map { _ =>
          Traced(batchWritten)
        }
      })
}
