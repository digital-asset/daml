// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import cats.instances.option.*
import cats.syntax.bifunctor.*
import cats.syntax.functor.*
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.protocol.{
  SequencerDeliverError,
  SequencerErrors,
  SubmissionRequest,
}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.SequencerWriter.*
import com.digitalasset.canton.synchronizer.sequencer.WriterStartupError.FailedToInitializeFromSnapshot
import com.digitalasset.canton.synchronizer.sequencer.admin.data.SequencerHealthStatus
import com.digitalasset.canton.synchronizer.sequencer.store.{SequencerStore, SequencerWriterStore}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration, SimClock}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.retry.{AllExceptionRetryPolicy, Pause}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{Keep, Sink}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

final case class OnlineSequencerCheckConfig(
    onlineCheckInterval: config.NonNegativeFiniteDuration =
      config.NonNegativeFiniteDuration.ofSeconds(5L),
    offlineDuration: config.NonNegativeFiniteDuration =
      config.NonNegativeFiniteDuration.ofSeconds(8L),
)

object TotalNodeCountValues {
  val SingleSequencerTotalNodeCount: PositiveInt = PositiveInt.tryCreate(1)

  /** We need to allocate a range of available DbLockCounters so need to specify a maximum number of
    * sequencer writers that can concurrently exist.
    */
  val MaxNodeCount = 32
}

/** Create instances for a [[sequencer.store.SequencerWriterStore]] and a predicate to know whether
  * we can recreate a sequencer writer on failures encountered potentially during storage.
  * Implements `AutoClosable` so implementations can use [[lifecycle.FlagCloseable]] to short
  * circuit retry attempts.
  */
trait SequencerWriterStoreFactory extends AutoCloseable {
  def create(storage: Storage, generalStore: SequencerStore)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, WriterStartupError, SequencerWriterStore]

  /** When the sequencer goes offline Exceptions may be thrown by the
    * [[sequencer.store.SequencerStore]] and [[sequencer.SequencerWriterSource]]. This allows
    * callers to check whether the captured exception is expected when offline and indicates that
    * the [[sequencer.SequencerWriter]] can still be recreated.
    */
  def expectedOfflineException(error: Throwable): Boolean = false
}

object SequencerWriterStoreFactory {
  def singleInstance(implicit executionContext: ExecutionContext): SequencerWriterStoreFactory =
    new SequencerWriterStoreFactory {
      override def create(storage: Storage, generalStore: SequencerStore)(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, WriterStartupError, SequencerWriterStore] =
        EitherT.pure(SequencerWriterStore.singleInstance(generalStore))
      override def close(): Unit = ()
    }
}

/** The Writer component is in practice a little state machine that will run crash recovery on
  * startup then create a running [[SequencerWriterSource]]. If this materialized Sequencer flow
  * then crashes with an exception that can be recovered by running crash recovery it will then go
  * through this process and attempt to restart the flow.
  *
  * Callers must call [[start]] to start the writer and will likely have to wait for this completing
  * before accepting calls for the sequencer to direct at [[send]]. Note that the crash recovery
  * steps may take a long duration to run:
  *   - we delete invalid events previously written by the sequencer and then attempt to insert a
  *     new online watermark, and these database queries may simply take a long time to run.
  *   - the [[SequencerWriter]] will wait until our clock has reached the new online watermark
  *     timestamp before starting the writer to ensure that no events before this timestamp are
  *     written. If this online watermark is significantly ahead of the current clock value it will
  *     just wait until this is reached. In practice assuming all sequencers in the local topology
  *     are kept in sync through NTP or similar, this duration should be very small (<1s).
  */
class SequencerWriter(
    writerStoreFactory: SequencerWriterStoreFactory,
    writerFlowFactory: SequencerWriterFlowFactory,
    storage: Storage,
    val generalStore: SequencerStore,
    clock: Clock,
    expectedCommitMode: Option[CommitMode],
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseableAsync
    with HasCloseContext {

  private case class RunningWriter(flow: RunningSequencerWriterFlow, store: SequencerWriterStore) {

    def healthStatus(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[SequencerHealthStatus] =
      // Small positive value for maxRetries, so that
      // - a short period of unavailability does not make the sequencer inactive
      // - the future terminates "timely"
      store
        .fetchWatermark(maxRetries = 10)
        .transform {
          case Failure(ex) =>
            logger.info(s"Unable to fetch watermark during health monitoring.", ex)

            // If we fail to fetch the watermark, we want still want to return a health status.
            Success(
              UnlessShutdown.Outcome(
                SequencerHealthStatus(
                  isActive = false,
                  Some("writer: N/A"),
                )
              )
            )

          case Success(watermarkO) =>
            val watermarkStatus = watermarkO match {
              case UnlessShutdown.Outcome(Some(watermark)) =>
                if (watermark.online) "online" else "offline"
              case UnlessShutdown.Outcome(None) => "initializing"
              case AbortedDueToShutdown => "Aborted due to shutdown"
            }

            // no watermark -> writer offline
            val writerOnline = watermarkO.onShutdown(None).exists(_.online)
            Success(
              UnlessShutdown.Outcome(
                SequencerHealthStatus(
                  isActive = writerOnline,
                  Some(s"writer: $watermarkStatus"),
                )
              )
            )
        }

    /** Ensures that all resources for the writer flow are halted and cleaned up. The store should
      * not be used after calling this operation (the HA implementation will close its exclusive
      * storage instance).
      */
    def close()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
      logger.debug(s"Completing writer flow")
      val future = for {
        _ <- FutureUnlessShutdown.outcomeF(flow.complete())
        // in the HA sequencer there's a chance the writer store may have already lost its writer lock,
        // in which case this will throw a PassiveInstanceException
        _ = logger.debug(s"Taking store offline")
        _ <- goOffline(store).recover {
          case throwable if writerStoreFactory.expectedOfflineException(throwable) =>
            logger.debug(
              s"Exception was thrown while setting the sequencer as offline but this is expected if already offline so suppressing",
              throwable,
            )
            UnlessShutdown.unit
        }
      } yield ()
      future.thereafter { _ =>
        logger.debug(s"Closing store")
        store.close()
      }
    }
  }

  private val runningWriterRef = new AtomicReference[Option[RunningWriter]](None)

  @VisibleForTesting
  private[sequencer] def isRunning: Boolean = runningWriterRef.get().isDefined

  private[sequencer] def healthStatus(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerHealthStatus] =
    runningWriterRef.get() match {
      case Some(runningWriter) => runningWriter.healthStatus
      case None =>
        FutureUnlessShutdown.pure(
          SequencerHealthStatus(isActive = false, Some("sequencer writer not running"))
        )
    }

  private def sequencerQueues: Option[SequencerWriterQueues] =
    runningWriterRef.get().map(_.flow.queues)

  /** The startup of a [[SequencerWriter]] can fail at runtime. Currently if this occurs we will log
    * a message at error level but as we have no ability to forcibly crash the node we will likely
    * continue running in an unhealthy state. This will however be visible to anyone calling the
    * status operation and could be used by a process monitor to externally restart the sequencer.
    */
  def startOrLogError(
      initialSnapshot: Option[SequencerInitialState],
      resetWatermarkTo: => ResetWatermark,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    start(initialSnapshot, resetWatermarkTo).fold(
      err => logger.error(s"Failed to startup sequencer writer: $err"),
      identity,
    )

  def start(
      // TODO(#18401): Move initialization from snapshot into the sequencer factory
      initialSnapshot: Option[SequencerInitialState] = None,
      resetWatermarkTo: => ResetWatermark,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, WriterStartupError, Unit] =
    performUnlessClosingEitherUSF[WriterStartupError, Unit](functionFullName) {
      def createStoreAndRunCrashRecovery()
          : EitherT[FutureUnlessShutdown, WriterStartupError, SequencerWriterStore] = {
        // only retry errors that are flagged as retryable
        implicit val success: retry.Success[Either[WriterStartupError, SequencerWriterStore]] =
          retry.Success {
            case Left(error) => !error.retryable
            case Right(_) => true
          }

        // continuously attempt to start the writer as we can't meaningfully proactively shutdown or crash
        // when this fails
        EitherT {
          Pause(logger, this, retry.Forever, 100.millis, "start-sequencer-writer").unlessShutdown(
            {
              logger.debug("Starting sequencer writer")
              for {
                writerStore <- writerStoreFactory.create(storage, generalStore)
                _ <- EitherTUtil
                  .onErrorOrFailureUnlessShutdown[WriterStartupError, Unit](
                    _ => writerStore.close(),
                    () => writerStore.close(),
                  ) {
                    for {
                      _ <- initialSnapshot
                        .fold[EitherT[FutureUnlessShutdown, WriterStartupError, Unit]](
                          EitherT.rightT(())
                        )(snapshot =>
                          generalStore
                            .initializeFromSnapshot(snapshot)
                            .leftMap(FailedToInitializeFromSnapshot.apply)
                        )
                      // validate that the datastore has an appropriate commit mode set in order to run the writer
                      _ <- expectedCommitMode
                        .fold(EitherTUtil.unitUS[String])(writerStore.validateCommitMode)
                        .leftMap(WriterStartupError.BadCommitMode.apply)
                      resetWatermarkToValue = resetWatermarkTo
                      _ <- {
                        (resetWatermarkToValue match {
                          case SequencerWriter.ResetWatermarkToClockNow |
                              SequencerWriter.DoNotResetWatermark =>
                            EitherT.pure[FutureUnlessShutdown, String](())
                          case SequencerWriter.ResetWatermarkToTimestamp(timestamp) =>
                            logger.debug(
                              s"Resetting the watermark to the externally passed timestamp of $timestamp"
                            )
                            writerStore.resetWatermark(timestamp).leftMap(_.toString)
                        }).leftMap(WriterStartupError.WatermarkResetError.apply)
                      }
                      onlineTimestamp <- EitherT.right[WriterStartupError](
                        runRecovery(writerStore, resetWatermarkToValue)
                      )
                      _ <- EitherT.right(generalStore.resetAndPreloadBuffer())
                      _ <- EitherT
                        .right[WriterStartupError](waitForOnline(onlineTimestamp))
                        .mapK(FutureUnlessShutdown.outcomeK)
                    } yield ()
                  }
              } yield writerStore
            }.value,
            AllExceptionRetryPolicy,
          )
        }
      }

      createStoreAndRunCrashRecovery()
        .map(startWriter)

    }

  def send(
      submission: SubmissionRequest
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] = {
    lazy val sendET = sequencerQueues
      .fold(
        EitherT
          .leftT[FutureUnlessShutdown, Unit](SequencerErrors.Unavailable("Unavailable"))
          .leftWiden[SequencerDeliverError]
      )(_.send(submission))

    EitherT(
      performUnlessClosingUSF(functionFullName)(sendET.value)
    )
  }

  def blockSequencerWrite(
      outcome: DeliverableSubmissionOutcome
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] = {
    lazy val sendET = sequencerQueues
      .fold(
        EitherT
          .leftT[FutureUnlessShutdown, Unit](
            SequencerErrors.Unavailable("Unavailable: sequencer is not running")
          )
          .leftWiden[SequencerDeliverError]
      )(_.blockSequencerWrite(outcome))
    EitherT(performUnlessClosingUSF(functionFullName)(sendET.value))
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def runRecovery(
      store: SequencerWriterStore,
      resetWatermarkTo: ResetWatermark,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[CantonTimestamp] =
    for {
      pastWatermarkO <- store.deleteEventsPastWatermark()
      goOnlineAt = resetWatermarkTo match {
        case SequencerWriter.ResetWatermarkToClockNow =>
          clock.now
        case SequencerWriter.ResetWatermarkToTimestamp(timestamp) =>
          timestamp
        case SequencerWriter.DoNotResetWatermark =>
          // Used in unified sequencer mode only, do not jump to now
          // the default value ensures that we are not ahead of BlockSequencer's timestamps, when rehydrating
          pastWatermarkO.getOrElse(CantonTimestamp.MinValue)
      }
      onlineTimestamp <- store.goOnline(
        goOnlineAt
      ) // actual online timestamp depends on other instances
      _ = if (clock.isSimClock && clock.now < onlineTimestamp) {
        logger.debug(s"The sequencer will not start unless sim clock moves to $onlineTimestamp")
        logger.debug(
          s"In order to prevent deadlocking in tests the clock's timestamp will now be advanced to $onlineTimestamp"
        )
        clock.asInstanceOf[SimClock].advanceTo(onlineTimestamp)
      }
    } yield onlineTimestamp

  /** When we go online we're given the value of the new watermark that is inserted for this
    * sequencer. We cannot start the writer before this point to ensure that no events before this
    * point are inserted. It may have already be surpassed in which case we can immediately start.
    * Otherwise we wait until this point has been reached.
    */
  private def waitForOnline(
      onlineTimestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] =
    if (clock.now.isBefore(onlineTimestamp)) {
      val durationToWait =
        java.time.Duration.between(clock.now.toInstant, onlineTimestamp.toInstant)
      logger.debug(
        s"Delaying sequencer writer start for $durationToWait to reach our online watermark"
      )

      val onlineP = Promise[Unit]()
      FutureUtil.doNotAwait(
        clock.scheduleAt(_ => onlineP.success(()), onlineTimestamp).unwrap,
        s"wait for becoming online at $onlineTimestamp",
      )
      onlineP.future
    } else Future.unit

  private def startWriter(
      store: SequencerWriterStore
  )(implicit traceContext: TraceContext): Unit =
    // if these actions fail we want to ensure that the store is closed
    try {
      val writerFlow = writerFlowFactory.create(store)
      setupWriterRecovery(writerFlow.done)
      runningWriterRef.set(RunningWriter(writerFlow, store).some)
    } catch {
      case NonFatal(ex) =>
        store.close()
        throw ex
    }

  private def setupWriterRecovery(doneF: Future[Unit]): Unit =
    doneF.onComplete { result =>
      withNewTraceContext { implicit traceContext =>
        performUnlessClosing(functionFullName) { // close will take care of shutting down a running writer if close is invoked
          // close the running writer and reset the reference
          val closed = runningWriterRef
            .getAndSet(None)
            .parTraverse_(_.close())
            .recover { case NonFatal(e) =>
              logger.debug("Running writer will be recovered, due to non-fatal error:", e)
              UnlessShutdown.unit
            }

          // determine whether we can run recovery or not
          val shouldRecover = result match {
            case Failure(_writerException: SequencerWriterException) => true
            case Failure(exception) => writerStoreFactory.expectedOfflineException(exception)
            case _other => false
          }

          if (shouldRecover) {
            // include the exception in our info log message if it was the cause of restarting the writer
            val message = "Running Sequencer recovery process"
            result.fold(ex => logger.info(message, ex), _ => logger.info(message))

            FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
              // Wait for the writer store to be closed before re-starting, otherwise we might end up with
              // concurrent write stores trying to connect to the DB within the same sequencer node
              closed.flatMap(_ =>
                startOrLogError(None, SequencerWriter.DoNotResetWatermark)(traceContext)
              ),
              "SequencerWriter recovery",
            )
          } else {
            // if we encountered an exception and have opted not to recover log a warning
            result.fold(
              ex => logger.warn(s"Sequencer writer has completed with an unrecoverable error", ex),
              _ => logger.debug("Sequencer writer has successfully completed"),
            )
          }
        }
      }
    }

  private def goOffline(
      store: SequencerWriterStore
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    logger.debug("Going offline so marking our sequencer as offline")
    store.goOffline()
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = withNewTraceContext {
    implicit traceContext =>
      logger.debug("Shutting down sequencer writer")
      val sequencerFlow = runningWriterRef.get()
      Seq(
        SyncCloseable("sequencerWriterStoreFactory", writerStoreFactory.close()),
        AsyncCloseable(
          "sequencingFlow",
          sequencerFlow.map(_.close()).getOrElse(FutureUnlessShutdown.unit).unwrap,
          // Use timeouts.closing.duration (as opposed to `shutdownShort`) as closing the sequencerFlow can be slow
          timeouts.closing,
        ),
      )
  }
}

object SequencerWriter {

  @VisibleForTesting
  private[sequencer] class RunningSequencerWriterFlow(
      val queues: SequencerWriterQueues,
      doneF: Future[Unit],
  )(implicit executionContext: ExecutionContext) {
    private val completed = new AtomicBoolean(false)

    /** Future for when the underlying stream has completed. We intentionally hand out a transformed
      * future that ensures our completed flag is set first. This is to in most cases avoiding the
      * race where we may call `queues.complete` on an already completed stream which will cause a
      * `IllegalStateException`. However as we can't actually synchronize the pekko stream
      * completing due to an error with close being called there is likely still a short window when
      * this situation could occur, however at this point it should only result in an unclean
      * shutdown.
      */
    val done: Future[Unit] = doneF.thereafter(_ => completed.set(true))

    def complete(): Future[Unit] = {
      if (!completed.get()) {
        queues.complete()
      }
      done
    }
  }

  def apply(
      writerConfig: SequencerWriterConfig,
      writerStorageFactory: SequencerWriterStoreFactory,
      totalNodeCount: PositiveInt,
      keepAliveInterval: Option[NonNegativeFiniteDuration],
      processingTimeout: ProcessingTimeout,
      storage: Storage,
      sequencerStore: SequencerStore,
      clock: Clock,
      eventSignaller: EventSignaller,
      protocolVersion: ProtocolVersion,
      loggerFactory: NamedLoggerFactory,
      blockSequencerMode: Boolean,
      metrics: SequencerMetrics,
  )(implicit materializer: Materializer, executionContext: ExecutionContext): SequencerWriter = {
    implicit val loggingContext: ErrorLoggingContext = ErrorLoggingContext(
      loggerFactory.getTracedLogger(SequencerWriter.getClass),
      loggerFactory.properties,
      TraceContext.empty,
    )

    object DefaultSequencerWriterFlowFactory extends SequencerWriterFlowFactory {
      override def create(
          store: SequencerWriterStore
      )(implicit traceContext: TraceContext): RunningSequencerWriterFlow = {
        val runnableGraph = SequencerWriterSource(
          writerConfig,
          totalNodeCount,
          keepAliveInterval,
          store,
          clock,
          eventSignaller,
          loggerFactory,
          protocolVersion,
          metrics,
          blockSequencerMode,
        )
          .toMat(Sink.ignore)(Keep.both)
          .mapMaterializedValue(m => new RunningSequencerWriterFlow(m._1, m._2.void))

        PekkoUtil.runSupervised(
          runnableGraph,
          errorLogMessagePrefix = "Sequencer writer flow error",
          isDone = (runningFlow: RunningSequencerWriterFlow) => runningFlow.done.isCompleted,
        )
      }
    }

    new SequencerWriter(
      writerStorageFactory,
      DefaultSequencerWriterFlowFactory,
      storage,
      sequencerStore,
      clock,
      writerConfig.commitModeValidation,
      processingTimeout,
      loggerFactory,
    )
  }

  sealed trait ResetWatermark
  final case object DoNotResetWatermark extends ResetWatermark
  final case object ResetWatermarkToClockNow extends ResetWatermark
  final case class ResetWatermarkToTimestamp(timestamp: CantonTimestamp) extends ResetWatermark

  trait SequencerWriterFlowFactory {
    def create(store: SequencerWriterStore)(implicit
        traceContext: TraceContext
    ): RunningSequencerWriterFlow
  }
}
