// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko

import cats.{Applicative, Traverse}
import com.daml.metrics.api.MetricHandle.Timer
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.error.FatalError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.parallelApplicativeFutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.PekkoP2PGrpcNetworking.PekkoP2PGrpcNetworkManager
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.ModuleControl.{
  NoOp,
  Send,
  SetBehavior,
  Stop,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.{
  ModuleControl,
  SystemInitializationResult,
  SystemInitializer,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import com.digitalasset.canton.tracing.{HasTraceContext, TraceContext}
import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.{BootstrapSetup, Cancellable}

import java.time.Instant
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.collection.mutable
import scala.concurrent.*
import scala.concurrent.duration.*
import scala.util.Try

object PekkoModuleSystem {

  // Should be a few millis, but giving it a good margin to be safe.
  private val PekkoActorSystemStartupMaxDuration = 5.seconds

  private val BlockingOperationTimeout = 30.seconds

  private def pekkoBehavior[MessageT](
      moduleSystem: PekkoModuleSystem,
      exitOnFatalFailures: Boolean,
      isOrdererHealthy: AtomicBoolean,
      outstandingMessages: AtomicInteger,
      moduleName: ModuleName,
      moduleNameForMetrics: String,
      loggerFactory: NamedLoggerFactory,
  ): Behavior[ModuleControl[PekkoEnv, MessageT]] = {

    def emitQueuePullMetrics(
        metricsContext: MetricsContext,
        maybeSendInstant: Option[Instant],
        maybeDelay: Option[FiniteDuration],
    ): Unit = {
      moduleSystem.metrics.performance.orderingStageLatency
        .emitModuleQueueSize(
          moduleNameForMetrics,
          outstandingMessages.decrementAndGet(),
        )(metricsContext)
      maybeSendInstant.foreach(
        moduleSystem.metrics.performance.orderingStageLatency
          .emitModuleQueueLatency(
            moduleNameForMetrics,
            _,
            maybeDelay,
          )(metricsContext)
      )
    }

    Behaviors.setup { context =>
      val pekkoContext: PekkoActorContext[MessageT] =
        PekkoActorContext(
          moduleSystem,
          context,
          moduleNameForMetrics,
          exitOnFatalFailures,
          isOrdererHealthy,
          outstandingMessages,
          loggerFactory,
        )
      val logger = loggerFactory.getLogger(getClass)
      @SuppressWarnings(Array("org.wartremover.warts.Var"))
      var maybeModule: Option[framework.Module[PekkoEnv, MessageT]] = None
      val sendsAwaitingAModule = mutable.Queue[Send[PekkoEnv, MessageT]]()
      Behaviors
        .supervise(
          Behaviors
            .receiveMessage[ModuleControl[PekkoEnv, MessageT]] {
              case SetBehavior(m: framework.Module[PekkoEnv, MessageT], ready) =>
                maybeModule.foreach(_.close())
                maybeModule = Some(m)
                if (ready)
                  m.ready(pekkoContext.self)
                // Emit queue stats for postponed messages that were awaiting a module
                sendsAwaitingAModule.foreach {
                  case Send(
                        message,
                        traceContext,
                        metricsContext,
                        maybeSendInstant,
                        maybeDelay,
                      ) =>
                    emitQueuePullMetrics(metricsContext, maybeSendInstant, maybeDelay)
                    m.receive(message)(pekkoContext, traceContext)
                }
                sendsAwaitingAModule.clear()
                Behaviors.same
              case send @ Send(
                    message,
                    traceContext,
                    metricsContext,
                    maybeSendInstant,
                    maybeDelay,
                  ) =>
                maybeModule match {
                  case Some(module) =>
                    emitQueuePullMetrics(metricsContext, maybeSendInstant, maybeDelay)
                    module.receive(message)(pekkoContext, traceContext)
                  case _ =>
                    sendsAwaitingAModule.enqueue(send)
                }
                Behaviors.same
              case NoOp() =>
                Behaviors.same
              case Stop(onStop) =>
                logger.debug(s"Stopping Pekko actor for module '$moduleName' as requested")
                onStop()
                Behaviors.stopped
            }
            .receiveSignal { case (_, Terminated(actorRef)) =>
              // after calling `context.stop()` we must handle the Terminated signal, otherwise an exception is thrown and the actor system stops
              logger.debug(
                s"$moduleName received Terminated signal for Pekko actor '${actorRef.path.name}' as requested"
              )
              Behaviors.same
            }
        )
        .onFailure(SupervisorStrategy.stop)
    }
  }

  private[bftordering] final case class PekkoModuleRef[AcceptedMessageT](
      moduleSystem: PekkoModuleSystem,
      ref: ActorRef[ModuleControl[PekkoEnv, AcceptedMessageT]],
      moduleNameForMetrics: String,
      outstandingMessages: AtomicInteger,
  ) extends ModuleRef[AcceptedMessageT] {
    override def asyncSend(message: AcceptedMessageT)(implicit
        traceContext: TraceContext,
        metricsContext: MetricsContext,
    ): Unit = {
      moduleSystem.metrics.performance.orderingStageLatency
        .emitModuleQueueSize(
          moduleNameForMetrics,
          outstandingMessages.incrementAndGet(),
        )
      ref ! Send(message, traceContext, metricsContext, maybeSendInstant = Some(Instant.now))
    }
  }

  private final case class PekkoCancellableEvent(
      cancellable: Cancellable,
      moduleSystem: PekkoModuleSystem,
      moduleNameForMetrics: String,
      outstandingMessages: AtomicInteger,
  ) extends CancellableEvent {

    override def cancel()(implicit metricsContext: MetricsContext): Boolean = {
      val cancelledSuccessfully = cancellable.cancel()
      if (cancelledSuccessfully)
        moduleSystem.metrics.performance.orderingStageLatency
          .emitModuleQueueSize(
            moduleNameForMetrics,
            outstandingMessages.decrementAndGet(),
          )
      cancelledSuccessfully
    }
  }

  private[bftordering] final case class PekkoActorContext[MessageT](
      moduleSystem: PekkoModuleSystem,
      underlying: ActorContext[ModuleControl[PekkoEnv, MessageT]],
      moduleNameForMetrics: String,
      exitOnFatalFailures: Boolean,
      isOrdererHealthy: AtomicBoolean,
      outstandingMessages: AtomicInteger,
      override val loggerFactory: NamedLoggerFactory,
  ) extends ModuleContext[PekkoEnv, MessageT] {

    override val self: PekkoModuleRef[MessageT] =
      PekkoModuleRef(moduleSystem, underlying.self, moduleNameForMetrics, outstandingMessages)

    override def delayedEvent(delay: FiniteDuration, message: MessageT)(implicit
        traceContext: TraceContext,
        metricsContext: MetricsContext,
    ): CancellableEvent = {
      moduleSystem.metrics.performance.orderingStageLatency
        .emitModuleQueueSize(
          moduleNameForMetrics,
          outstandingMessages.incrementAndGet(),
        )
      PekkoCancellableEvent(
        underlying.scheduleOnce(
          delay,
          underlying.self,
          Send[PekkoEnv, MessageT](
            message,
            traceContext,
            metricsContext,
            maybeSendInstant = Some(Instant.now),
            maybeDelay = Some(delay),
          ),
        ),
        moduleSystem,
        moduleNameForMetrics,
        outstandingMessages,
      )
    }

    override def newModuleRef[NewModuleMessageT](
        moduleName: ModuleName
    )(moduleNameForMetrics: String = moduleName.name): PekkoModuleRef[NewModuleMessageT] =
      moduleSystem.newModuleRefImpl(moduleName, moduleNameForMetrics, underlying)

    override def setModule[OtherModuleMessageT](
        moduleRef: PekkoModuleRef[OtherModuleMessageT],
        module: framework.Module[PekkoEnv, OtherModuleMessageT],
    ): Unit =
      moduleSystem.setModule(moduleRef, module)

    override protected def pipeToSelfInternal[X](
        futureUnlessShutdown: PekkoFutureUnlessShutdown[X]
    )(
        fun: Try[X] => Option[MessageT]
    )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit =
      futureUnlessShutdown.orderingStage.fold(pipeToSelfImpl(futureUnlessShutdown, fun)) { stage =>
        pipeToSelfImpl(
          moduleSystem.metrics.performance.orderingStageLatency
            .timeFuture[PekkoEnv, X](this, futureUnlessShutdown, stage),
          fun,
        )
      }

    private def pipeToSelfImpl[X](
        futureUnlessShutdown: PekkoFutureUnlessShutdown[X],
        fun: Try[X] => Option[MessageT],
    )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit =
      underlying.pipeToSelf(
        toFuture(
          futureUnlessShutdown.action,
          futureUnlessShutdown.futureUnlessShutdown(),
          underlying,
        )
      ) { result =>
        fun(result) match {
          case Some(msg) =>
            moduleSystem.metrics.performance.orderingStageLatency
              .emitModuleQueueSize(
                moduleNameForMetrics,
                outstandingMessages.incrementAndGet(),
              )
            Send(msg, traceContext, metricsContext, maybeSendInstant = Some(Instant.now))
          case None => NoOp()
        }
      }

    override def blockingAwait[X](actionAndFuture: PekkoFutureUnlessShutdown[X]): X =
      blockingAwait(actionAndFuture, BlockingOperationTimeout)

    override def blockingAwait[X](
        actionAndFuture: PekkoFutureUnlessShutdown[X],
        duration: FiniteDuration,
    ): X = blocking {
      Await.result(
        toFuture(actionAndFuture.action, actionAndFuture.futureUnlessShutdown(), underlying),
        atMost = duration,
      )
    }

    override def abort(failure: Throwable): Nothing = {
      markOrdererAsUnhealthy()
      if (exitOnFatalFailures) {
        FatalError.exitOnFatalError(failure.getMessage, failure, logger)(TraceContext.empty)
      } else {
        throw failure
      }
    }

    override def abort(msg: String): Nothing = {
      markOrdererAsUnhealthy()
      if (exitOnFatalFailures) {
        FatalError.exitOnFatalError(msg, logger)(TraceContext.empty)
      } else {
        sys.error(msg)
      }
    }

    override def abort(): Nothing = {
      markOrdererAsUnhealthy()
      val msg = "Aborted"
      if (exitOnFatalFailures) {
        FatalError.exitOnFatalError(msg, logger)(TraceContext.empty)
      } else {
        sys.error(msg)
      }
    }

    private def markOrdererAsUnhealthy(): Unit = {
      logger.error("Marking orderer as unhealthy")(TraceContext.empty)
      isOrdererHealthy.set(false)
    }

    override def become(module: framework.Module[PekkoEnv, MessageT]): Unit =
      underlying.self ! SetBehavior(module, ready = true)

    // Note that further messages sent to stopped actors land in the dead letters. Pekko is configured to log them.
    override def stop(onStop: () => Unit): Unit =
      underlying.self ! Stop(onStop)

    private def toFuture[X](
        action: String,
        futureUnlessShutdown: FutureUnlessShutdown[X],
        underlying: ActorContext[ModuleControl[PekkoEnv, MessageT]],
    ): Future[X] =
      futureUnlessShutdown.failOnShutdownToAbortException(action)(underlying.executionContext)

    override def withNewTraceContext[A](fn: TraceContext => A): A =
      TraceContext.withNewTraceContext("pekko_actor")(fn)

    override def traceContextOfBatch(
        items: IterableOnce[HasTraceContext]
    ): TraceContext =
      TraceContext.ofBatch("pekko_actor")(items)(logger)

    override def futureContext: FutureContext[PekkoEnv] = PekkoFutureContext(
      underlying.executionContext
    )
  }

  final case class PekkoFutureUnlessShutdown[MessageT](
      action: String,
      futureUnlessShutdown: () => FutureUnlessShutdown[MessageT],
      orderingStage: Option[String] = None,
  ) {
    def map[B](f: MessageT => B, mapOrderingStage: Option[String] = None)(implicit
        ec: ExecutionContext
    ): PekkoFutureUnlessShutdown[B] =
      PekkoFutureUnlessShutdown(
        action,
        () => futureUnlessShutdown().map(f),
        mapOrderingStage,
      )

    def flatMap[B](
        f: MessageT => PekkoFutureUnlessShutdown[B],
        flatMapOrderingStage: Option[String] = None,
    )(implicit ec: ExecutionContext): PekkoFutureUnlessShutdown[B] =
      PekkoFutureUnlessShutdown(
        s"$action.flatmap(...)",
        () => futureUnlessShutdown().flatMap(x => f(x).futureUnlessShutdown()),
        flatMapOrderingStage,
      )

    def failed(implicit ec: ExecutionContext): PekkoFutureUnlessShutdown[Throwable] =
      PekkoFutureUnlessShutdown(action, () => futureUnlessShutdown().failed)

    def zip[Y](future2: PekkoFutureUnlessShutdown[Y], zipOrderingStage: Option[String] = None)(
        implicit ec: ExecutionContext
    ): PekkoFutureUnlessShutdown[(MessageT, Y)] =
      PekkoFutureUnlessShutdown(
        s"$action.zip(${future2.action})",
        () =>
          Applicative[FutureUnlessShutdown](parallelApplicativeFutureUnlessShutdown)
            .product(futureUnlessShutdown(), future2.futureUnlessShutdown()),
        zipOrderingStage,
      )
  }

  object PekkoFutureUnlessShutdown {

    def sequence[A, F[_]](
        futures: F[PekkoFutureUnlessShutdown[A]],
        orderingStage: Option[String] = None,
    )(implicit
        ev: Traverse[F],
        ec: ExecutionContext,
    ): PekkoFutureUnlessShutdown[F[A]] =
      PekkoFutureUnlessShutdown(
        s"sequence(${ev.map(futures)(_.action)})",
        () => ev.sequence(ev.map(futures)(_.futureUnlessShutdown())),
        orderingStage,
      )

    def pure[X](x: X): PekkoFutureUnlessShutdown[X] =
      PekkoFutureUnlessShutdown("", () => FutureUnlessShutdown.pure(x))
  }

  private[bftordering] final case class PekkoFutureContext(executionContext: ExecutionContext)
      extends FutureContext[PekkoEnv] {

    override def timeFuture[X](timer: Timer, futureUnlessShutdown: => PekkoFutureUnlessShutdown[X])(
        implicit mc: MetricsContext
    ): PekkoFutureUnlessShutdown[X] =
      PekkoFutureUnlessShutdown(
        futureUnlessShutdown.action,
        () =>
          FutureUnlessShutdown(timer.timeFuture(futureUnlessShutdown.futureUnlessShutdown().unwrap)),
      )

    override def zipFuture[X, Y](
        future1: PekkoFutureUnlessShutdown[X],
        future2: PekkoFutureUnlessShutdown[Y],
        orderingStage: Option[String] = None,
    ): PekkoFutureUnlessShutdown[(X, Y)] =
      future1.zip(future2, orderingStage)(executionContext)

    override def zipFuture3[X, Y, Z](
        future1: PekkoFutureUnlessShutdown[X],
        future2: PekkoFutureUnlessShutdown[Y],
        future3: PekkoFutureUnlessShutdown[Z],
        orderingStage: Option[String] = None,
    ): PekkoFutureUnlessShutdown[(X, Y, Z)] =
      future1
        .zip(future2, orderingStage)(executionContext)
        .zip(future3, orderingStage)(executionContext)
        .map(
          { case ((x, y), z) =>
            (x, y, z)
          },
          orderingStage,
        )(executionContext)

    override def sequenceFuture[A, F[_]](
        futures: F[PekkoFutureUnlessShutdown[A]],
        orderingStage: Option[String] = None,
    )(implicit
        ev: Traverse[F]
    ): PekkoFutureUnlessShutdown[F[A]] =
      PekkoFutureUnlessShutdown.sequence(futures, orderingStage)(ev, executionContext)

    override def mapFuture[X, Y](
        future: PekkoFutureUnlessShutdown[X]
    )(fun: PureFun[X, Y], orderingStage: Option[String] = None): PekkoFutureUnlessShutdown[Y] =
      future.map(fun, orderingStage)(executionContext)

    override def pureFuture[X](x: X): PekkoFutureUnlessShutdown[X] =
      PekkoFutureUnlessShutdown.pure(x)

    override def flatMapFuture[R1, R2](
        future1: PekkoFutureUnlessShutdown[R1],
        future2: PureFun[R1, PekkoFutureUnlessShutdown[R2]],
        orderingStage: Option[String] = None,
    ): PekkoFutureUnlessShutdown[R2] =
      future1.flatMap(future2, orderingStage)(executionContext)
  }

  private[bftordering] final class PekkoEnv extends Env[PekkoEnv] {
    override type ActorContextT[MessageT] = PekkoActorContext[MessageT]
    override type ModuleRefT[AcceptedMessageT] = PekkoModuleRef[AcceptedMessageT]
    override type FutureUnlessShutdownT[MessageT] = PekkoFutureUnlessShutdown[MessageT]
  }

  /** The result of initializing an Pekko module system. Since Pekko actors initialization happens
    * in a delayed fashion when actors are actually started by Pekko, any initialization results can
    * only be provided asynchronously; however, since Pekko initialization is expected to run
    * quickly and system construction time is not performance-critical, we wait until Pekko is
    * started and return the initialization results synchronously.
    *
    * @param actorSystem
    *   The Pekko typed actor system used
    * @param initResult
    *   The initialization result
    * @tparam InputMessageT
    *   The type of input messages, i.e., messages sent by client to the input module
    */
  final case class PekkoModuleSystemInitResult[InputMessageT](
      actorSystem: ActorSystem[ModuleControl[PekkoEnv, Unit]],
      initResult: SystemInitializationResult[
        PekkoEnv,
        PekkoP2PGrpcNetworkManager,
        BftOrderingMessage,
        InputMessageT,
      ],
  )

  private[bftordering] final class PekkoModuleSystem(
      underlyingRootActorContext: ActorContext[ModuleControl[PekkoEnv, Unit]],
      exitOnFatalFailures: Boolean,
      isOrdererHealthy: AtomicBoolean,
      val metrics: BftOrderingMetrics,
      loggerFactory: NamedLoggerFactory,
  ) extends ModuleSystem[PekkoEnv] {

    override def rootActorContext: PekkoActorContext[?] =
      PekkoActorContext(
        this,
        underlyingRootActorContext,
        "rootActorContext",
        exitOnFatalFailures,
        isOrdererHealthy,
        new AtomicInteger(), // Unused
        loggerFactory,
      )

    override def futureContext: FutureContext[PekkoEnv] = PekkoFutureContext(
      underlyingRootActorContext.executionContext
    )

    override def newModuleRef[AcceptedMessageT](
        moduleName: ModuleName
    )(moduleNameForMetrics: String = moduleName.name): PekkoModuleRef[AcceptedMessageT] =
      newModuleRefImpl(moduleName, moduleNameForMetrics, rootActorContext.underlying)

    protected[pekko] def newModuleRefImpl[AcceptedMessageT, ContextMessageT](
        moduleName: ModuleName,
        moduleNameForMetrics: String,
        actorContext: ActorContext[ModuleControl[PekkoEnv, ContextMessageT]],
    ): PekkoModuleRef[AcceptedMessageT] = {
      val outstandingMessages = new AtomicInteger()
      val actorRef =
        actorContext.spawn(
          pekkoBehavior[AcceptedMessageT](
            this,
            exitOnFatalFailures,
            isOrdererHealthy,
            outstandingMessages,
            moduleName,
            moduleNameForMetrics,
            loggerFactory,
          ),
          moduleName.name,
          MailboxSelector.fromConfig("bft-ordering.control-mailbox"),
        )
      actorContext.watch(actorRef)
      PekkoModuleRef(this, actorRef, moduleNameForMetrics, outstandingMessages)
    }

    override def setModule[AcceptedMessageT](
        moduleRef: PekkoModuleRef[AcceptedMessageT],
        module: framework.Module[PekkoEnv, AcceptedMessageT],
    ): Unit =
      moduleRef.ref ! SetBehavior(module, ready = false)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
  def tryCreate[InputMessageT](
      name: String,
      systemInitializer: SystemInitializer[
        PekkoEnv,
        PekkoP2PGrpcNetworkManager,
        BftOrderingMessage,
        InputMessageT,
      ],
      createP2PNetworkManager: (
          P2PConnectionEventListener,
          ModuleRef[BftOrderingMessage],
      ) => PekkoP2PGrpcNetworkManager,
      exitOnFatalFailures: Boolean,
      isOrdererHealthy: AtomicBoolean,
      metrics: BftOrderingMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): PekkoModuleSystemInitResult[InputMessageT] = {
    val resultPromise =
      Promise[SystemInitializationResult[
        PekkoEnv,
        PekkoP2PGrpcNetworkManager,
        BftOrderingMessage,
        InputMessageT,
      ]]()

    val actorSystem = {
      val systemBehavior =
        Behaviors
          .supervise {
            Behaviors.setup[ModuleControl[PekkoEnv, Unit]] { actorContext =>
              val logger = loggerFactory.getLogger(getClass)
              val moduleSystem =
                new PekkoModuleSystem(
                  actorContext,
                  exitOnFatalFailures,
                  isOrdererHealthy,
                  metrics,
                  loggerFactory,
                )
              resultPromise.success(
                systemInitializer.initialize(moduleSystem, createP2PNetworkManager)
              )
              Behaviors.receiveSignal { case (_, Terminated(actorRef)) =>
                logger.debug(
                  s"Pekko module system behavior received 'Terminated($actorRef)' signal"
                )
                Behaviors.same
              }
            }
          }
          .onFailure(SupervisorStrategy.stop)
      ActorSystem(
        systemBehavior,
        name,
        bootstrapSetup = BootstrapSetup().withDefaultExecutionContext(executionContext),
      )
    }
    // The code within Behaviors.setup will be run as soon as the ActorSystem is created, so
    // the future below will not take more than a few millis. In this case, waiting is more sensible than
    // propagating Future values.
    val result = blocking(Await.result(resultPromise.future, PekkoActorSystemStartupMaxDuration))
    PekkoModuleSystemInitResult(
      actorSystem,
      result,
    )
  }
}
