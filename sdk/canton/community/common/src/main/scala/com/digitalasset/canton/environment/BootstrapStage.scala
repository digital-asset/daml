// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.data.EitherT
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  Lifecycle,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.retry.RetryUtil.NoExnRetryable
import com.digitalasset.canton.util.retry.Success
import com.digitalasset.canton.util.{EitherTUtil, SimpleExecutionQueue, retry}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

sealed trait BootstrapStageOrLeaf[T <: CantonNode]
    extends FlagCloseable
    with NamedLogging
    with HasCloseContext {

  def start()(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit]
  def getNode: Option[T]
  protected def bootstrap: BootstrapStage.Callback

  protected def timeouts: ProcessingTimeout = bootstrap.timeouts
  protected def loggerFactory: NamedLoggerFactory = bootstrap.loggerFactory

}

class RunningNode[T <: CantonNode](
    val bootstrap: BootstrapStage.Callback,
    val node: T,
)(implicit ec: ExecutionContext)
    extends BootstrapStageOrLeaf[T] {

  def description: String = "Node up and running"
  override def getNode: Option[T] = Some(node)

  override def start()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = EitherT.rightT[FutureUnlessShutdown, String](())

}

abstract class BootstrapStage[T <: CantonNode, StageResult <: BootstrapStageOrLeaf[T]](
    val description: String,
    val bootstrap: BootstrapStage.Callback,
)(implicit executionContext: ExecutionContext)
    extends BootstrapStageOrLeaf[T] {

  private val closeables = new AtomicReference[Seq[AutoCloseable]](Seq.empty)
  protected val stageResult = new AtomicReference[Option[StageResult]](None)

  /** can be used to track closeables created with this class that should be cleaned up after this stage */
  protected def addCloseable[C <: AutoCloseable](item: C): Unit = {
    closeables.updateAndGet(_ :+ item).discard
  }

  /** main handler to implement where we attempt to init this stage
    * if we return None, then the init was okay but stopped at this level (waiting for
    *   further input)
    */
  protected def attempt()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Option[StageResult]]

  /** the attempt and store handler that runs sequential on the init queue
    * it will attempt to create the next stage and store it in the current
    *   atomic reference
    */
  protected def attemptAndStore()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Option[StageResult]] = {
    bootstrap.queue.executeEUS(
      stageResult.get() match {
        case Some(previous) => EitherT.rightT(Some(previous))
        case None =>
          EitherT(
            performUnlessClosingUSF(description)(
              (for {
                result <- attempt()
                  .leftMap { err =>
                    logger.error(s"Startup of ${description} failed with $err")
                    bootstrap.abortThisNodeOnStartupFailure()
                    err
                  }
              } yield {
                stageResult.set(result)
                result
              }).value
            )
          )
      },
      description,
    )
  }

  /** iterative start handler which will attempt to start the stages until
    * we are either up and running or awaiting some init action by the user
    */
  def start()(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    if (stageResult.get().isDefined) {
      logger.error(s"Duplicate init at ${description}")
    }
    logger.debug(s"Attempting startup stage: $description")
    for {
      result <- attemptAndStore()
      _ <- result match {
        case None =>
          logger.info(s"Startup succeeded up to stage ${description}, waiting for external input")
          EitherT.rightT[FutureUnlessShutdown, String](())
        case Some(stage) =>
          logger.debug(s"Succeeded startup stage: $description")
          def closeOnFailure() = {
            Lifecycle.close(this)(logger)
            bootstrap.abortThisNodeOnStartupFailure()
          }
          stage
            .start()
            .thereafter {
              case scala.util.Success(UnlessShutdown.Outcome(Right(_))) =>
              // do nothing on right success
              case scala.util.Success(UnlessShutdown.Outcome(Left(err))) =>
                logger.info(s"Closing due to error ${err}")
                closeOnFailure()
              case scala.util.Success(AbortedDueToShutdown) =>
              // should be okay as if the child is shutdown, then the parent will be shutdown soon too
              case scala.util.Failure(ex) =>
                logger.error(".start() failed with exception!", ex)
                closeOnFailure()
            }
      }
    } yield ()
  }

  def next: Option[StageResult] = stageResult.get()
  def getNode: Option[T] = next.flatMap(_.getNode)

  override protected def onClosed(): Unit = {
    super.onClosed()
    // first close subsequent stage and then close this stage
    // synchronisation with attemptAndStore happens through performUnlessClosing
    stageResult.getAndSet(None).foreach { res =>
      Lifecycle.close(res)(logger)
    }
    Lifecycle.close(closeables.getAndSet(Seq.empty).reverse *)(logger)
  }

}

/** Bootstrap stage which does auto-init / write operations and might eventually be passive */
abstract class BootstrapStageWithStorage[
    T <: CantonNode,
    StageResult <: BootstrapStageOrLeaf[T],
    M,
](
    description: String,
    bootstrap: BootstrapStage.Callback,
    storage: Storage,
    autoInit: Boolean,
)(implicit executionContext: ExecutionContext)
    extends BootstrapStage[T, StageResult](description, bootstrap) {

  private val backgroundStarted = new AtomicBoolean(false)

  /** if a passive node hits a manual init step, it will return "start" is succeeded
    * and wait in the background for the active node to finish the startup sequence
    */
  protected def toBackgroundForPassiveNode()(implicit traceContext: TraceContext): Unit = {
    if (!backgroundStarted.getAndSet(true)) {
      logger.debug(s"As passive instance, I await $description or becoming active")
      // retry here as long as we don't get a result or we get a serious failure
      // so we retry here if the result is Right(None)
      val success = Success[Either[String, Option[StageResult]]](_ != Right(None))
      EitherTUtil.doNotAwait(
        EitherT(
          retry
            .Backoff(
              logger,
              this,
              retry.Forever - 1,
              initialDelay = 10.millis,
              maxDelay = 5.seconds,
              s"waitForInit-${description}",
            )
            // on shutdown, the retry loop will return the last value so if
            // we get None back, we know that the retry loop was aborted due to a shutdown
            .unlessShutdown(attemptAndStore().value, NoExnRetryable)(
              success,
              executionContext,
              traceContext,
            )
        ).flatMap {
          case Some(result) =>
            logger.info(
              s"Initialization stage $description completed in the background, proceeding."
            )
            performUnlessClosingEitherU(description)(result.start().onShutdown(Right(())))
          case None => // was aborted due to shutdown, so we just pass
            EitherT.rightT[FutureUnlessShutdown, String](())
        }.onShutdown {
          logger.debug(s"Initialization of ${description} aborted due to shutdown")
          Right(())
        },
        s"Background startup failed at $description",
      )
    }
  }

  /** test whether the stage is completed already through a previous init. if so, return result */
  protected def stageCompleted(implicit
      traceContext: TraceContext
  ): Future[Option[M]]

  /** given the result of this stage, create the next stage */
  protected def buildNextStage(result: M): StageResult

  /** if the stage didn't complete yet, the node is active and auto-init is set to true, perform
    * the steps necessary. note, this invocation will be thread safe and only run once
    * however, any implementation needs to be crash resilient, if it didn't complete fully, it
    * might be called again.
    * if this stage does not support auto-init, you must return None
    * the method may throw "PassiveInstanceException" if it becomes passive
    */
  protected def autoCompleteStage(): EitherT[FutureUnlessShutdown, String, Option[M]]

  /** if external input is necessary */
  protected def completeWithExternal(
      storeAndPassResult: => EitherT[Future, String, M]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    completeWithExternalUS(storeAndPassResult.mapK(FutureUnlessShutdown.outcomeK))

  protected def completeWithExternalUS(
      storeAndPassResult: => EitherT[FutureUnlessShutdown, String, M]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    bootstrap.queue
      .executeEUS(
        if (stageResult.get().nonEmpty) {
          EitherT.leftT[FutureUnlessShutdown, StageResult](s"Already initialised ${description}")
        } else
          {
            for {
              current <- performUnlessClosingEitherU(s"check-already-init-$description")(
                EitherT.right[String](stageCompleted)
              )
              _ <- EitherT.cond[FutureUnlessShutdown](
                current.isEmpty,
                (),
                s"Node is already initialised with ${current}",
              )
              _ <- EitherT.cond[FutureUnlessShutdown](storage.isActive, (), "Node is passive")
              item <- performUnlessClosingEitherUSF(s"complete-grab-result-$description")(
                storeAndPassResult
              ): EitherT[FutureUnlessShutdown, String, M]
              stage <- EitherT.right(
                FutureUnlessShutdown.lift(performUnlessClosing(s"store-stage-$description") {
                  val stage = buildNextStage(item)
                  stageResult.set(Some(stage))
                  stage
                })
              )
            } yield stage
          }: EitherT[FutureUnlessShutdown, String, StageResult],
        s"complete-with-external-$description",
      )
      .flatMap { stage =>
        // run start outside here to avoid blocking on the sequential queue. note that
        // shutdown / sequential processing is handled within the start
        stage.start().leftMap { err =>
          logger.error(s"Failed to startup at $description with $err")
          err
        }
      }

  final override def attempt()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Option[StageResult]] =
    performUnlessClosingEitherUSF(description) {
      for {
        result <- EitherT.right(stageCompleted).mapK(FutureUnlessShutdown.outcomeK)
        stageO <- result match {
          case Some(result) =>
            EitherT.rightT[FutureUnlessShutdown, String](Some(buildNextStage(result)))
          case None =>
            // if stage is not completed, but we can auto-init this stage, proceed
            val isActive = storage.isActive
            if (autoInit && isActive) {
              EitherT(
                autoCompleteStage()
                  .map { res =>
                    if (res.isEmpty) {
                      logger.info(
                        s"Waiting for external action on $description to complete startup"
                      )
                    }
                    res
                  }
                  .value
                  .recover { case _: PassiveInstanceException =>
                    logger.info(s"Stage ${description} failed as node became passive")
                    // if we became passive during auto-complete stage, we complete the
                    // start procedure and move the waiting to the back
                    UnlessShutdown.Outcome(Right(None))
                  }
              ).map(_.map(buildNextStage))
            } else {
              // otherwise, finish the startup here and wait for an external trigger
              EitherT.rightT[FutureUnlessShutdown, String](None)
            }
        }
      } yield stageO
    }

  override def start()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    super.start().map { res =>
      // if start did not complete, move start
      if (!storage.isActive && next.isEmpty) {
        toBackgroundForPassiveNode()
      }
      res
    }
  }
}

object BootstrapStage {
  trait Callback {
    def loggerFactory: NamedLoggerFactory
    def timeouts: ProcessingTimeout
    def abortThisNodeOnStartupFailure(): Unit
    def queue: SimpleExecutionQueue
    def ec: ExecutionContext
  }

}
