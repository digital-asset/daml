// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko

import cats.{Applicative, Traverse}
import com.daml.metrics.api.MetricHandle.Timer
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.parallelApplicativeFutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
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
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.actor.typed.*
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.{BootstrapSetup, Cancellable}

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
      moduleName: ModuleName,
      loggerFactory: NamedLoggerFactory,
  ): Behavior[ModuleControl[PekkoEnv, MessageT]] =
    Behaviors.setup { context =>
      val pekkoContext: PekkoActorContext[MessageT] =
        PekkoActorContext(moduleSystem, context, loggerFactory)
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
                sendsAwaitingAModule.foreach { case Send(message, traceContext) =>
                  m.receive(message)(pekkoContext, traceContext)
                }
                sendsAwaitingAModule.clear()
                Behaviors.same
              case send @ Send(message, traceContext) =>
                maybeModule match {
                  case Some(module) =>
                    module.receive(message)(pekkoContext, traceContext)
                  case _ =>
                    sendsAwaitingAModule.enqueue(send)
                }
                Behaviors.same
              case NoOp() =>
                Behaviors.same
              case Stop(onStop) =>
                logger.info(s"Stopping Pekko actor for module '$moduleName' as requested")
                onStop()
                Behaviors.stopped
            }
            .receiveSignal { case (_, Terminated(actorRef)) =>
              // after calling `context.stop()` we must handle the Terminated signal, otherwise an exception is thrown and the actor system stops
              logger.info(
                s"$moduleName received Terminated signal for Pekko actor '${actorRef.path.name}' as requested"
              )
              Behaviors.same
            }
        )
        .onFailure(SupervisorStrategy.stop)
    }

  private[bftordering] final case class PekkoModuleRef[AcceptedMessageT](
      ref: ActorRef[ModuleControl[PekkoEnv, AcceptedMessageT]]
  ) extends ModuleRef[AcceptedMessageT] {
    override def asyncSendTraced(message: AcceptedMessageT)(implicit
        traceContext: TraceContext
    ): Unit =
      ref ! Send(message, traceContext)
  }

  private final case class PekkoCancellableEvent(cancellable: Cancellable)
      extends CancellableEvent {

    override def cancel(): Boolean = cancellable.cancel()
  }

  private[bftordering] final case class PekkoActorContext[MessageT](
      moduleSystem: PekkoModuleSystem,
      underlying: ActorContext[ModuleControl[PekkoEnv, MessageT]],
      override val loggerFactory: NamedLoggerFactory,
  ) extends ModuleContext[PekkoEnv, MessageT] {

    override val self: PekkoModuleRef[MessageT] = PekkoModuleRef(underlying.self)

    override def delayedEventTraced(delay: FiniteDuration, message: MessageT)(implicit
        traceContext: TraceContext
    ): CancellableEvent =
      PekkoCancellableEvent(
        underlying.scheduleOnce(
          delay,
          underlying.self,
          Send[PekkoEnv, MessageT](message, traceContext),
        )
      )

    override def newModuleRef[NewModuleMessageT](
        moduleName: ModuleName
    ): PekkoModuleRef[NewModuleMessageT] =
      moduleSystem.newModuleRefImpl(moduleName, underlying)

    override def setModule[OtherModuleMessageT](
        moduleRef: PekkoModuleRef[OtherModuleMessageT],
        module: framework.Module[PekkoEnv, OtherModuleMessageT],
    ): Unit =
      moduleSystem.setModule(moduleRef, module)

    override def pipeToSelfInternal[X](
        futureUnlessShutdown: PekkoFutureUnlessShutdown[X]
    )(fun: Try[X] => Option[MessageT])(implicit traceContext: TraceContext): Unit =
      underlying.pipeToSelf(
        toFuture(
          futureUnlessShutdown.action,
          futureUnlessShutdown.futureUnlessShutdown(),
          underlying,
        )
      ) { result =>
        fun(result) match {
          case Some(msg) => Send(msg, traceContext)
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

    override def abort(failure: Throwable): Nothing =
      throw failure

    override def abort(msg: String): Nothing =
      sys.error(msg)

    override def abort(): Nothing =
      sys.error("Aborted")

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
      TraceContext.withNewTraceContext(fn)

    override def futureContext: FutureContext[PekkoEnv] = PekkoFutureContext(
      underlying.executionContext
    )
  }

  final case class PekkoFutureUnlessShutdown[MessageT](
      action: String,
      futureUnlessShutdown: () => FutureUnlessShutdown[MessageT],
  ) {
    def map[B](f: MessageT => B)(implicit ec: ExecutionContext): PekkoFutureUnlessShutdown[B] =
      PekkoFutureUnlessShutdown(action, () => futureUnlessShutdown().map(f))

    def flatMap[B](
        f: MessageT => PekkoFutureUnlessShutdown[B]
    )(implicit ec: ExecutionContext): PekkoFutureUnlessShutdown[B] =
      PekkoFutureUnlessShutdown(
        action,
        () => futureUnlessShutdown().flatMap(x => f(x).futureUnlessShutdown()),
      )

    def failed(implicit ec: ExecutionContext): PekkoFutureUnlessShutdown[Throwable] =
      PekkoFutureUnlessShutdown(action, () => futureUnlessShutdown().failed)

    def zip[Y](future2: PekkoFutureUnlessShutdown[Y])(implicit
        ec: ExecutionContext
    ): PekkoFutureUnlessShutdown[(MessageT, Y)] =
      PekkoFutureUnlessShutdown(
        s"$action.zip(${future2.action})",
        () =>
          Applicative[FutureUnlessShutdown](parallelApplicativeFutureUnlessShutdown)
            .product(futureUnlessShutdown(), future2.futureUnlessShutdown()),
      )
  }

  object PekkoFutureUnlessShutdown {

    def sequence[A, F[_]](futures: F[PekkoFutureUnlessShutdown[A]])(implicit
        ev: Traverse[F],
        ec: ExecutionContext,
    ): PekkoFutureUnlessShutdown[F[A]] = PekkoFutureUnlessShutdown(
      s"sequence(${ev.map(futures)(_.action)})",
      () => ev.sequence(ev.map(futures)(_.futureUnlessShutdown())),
    )

    def pure[X](x: X): PekkoFutureUnlessShutdown[X] =
      PekkoFutureUnlessShutdown("", () => FutureUnlessShutdown.pure(x))

  }

  private[bftordering] final case class PekkoFutureContext(executionContext: ExecutionContext)
      extends FutureContext[PekkoEnv] {

    override def timeFuture[X](timer: Timer, futureUnlessShutdown: => PekkoFutureUnlessShutdown[X])(
        implicit mc: MetricsContext
    ): PekkoFutureUnlessShutdown[X] = {
      val handle = timer.startAsync()
      futureUnlessShutdown.map { x =>
        handle.stop()
        x
      }(executionContext)
    }

    override def zipFuture[X, Y](
        future1: PekkoFutureUnlessShutdown[X],
        future2: PekkoFutureUnlessShutdown[Y],
    ): PekkoFutureUnlessShutdown[(X, Y)] =
      future1.zip(future2)(executionContext)

    override def zipFuture[X, Y, Z](
        future1: PekkoFutureUnlessShutdown[X],
        future2: PekkoFutureUnlessShutdown[Y],
        future3: PekkoFutureUnlessShutdown[Z],
    ): PekkoFutureUnlessShutdown[(X, Y, Z)] =
      future1
        .zip(future2)(executionContext)
        .zip(future3)(executionContext)
        .map { case ((x, y), z) =>
          (x, y, z)
        }(executionContext)

    override def sequenceFuture[A, F[_]](
        futures: F[PekkoFutureUnlessShutdown[A]]
    )(implicit
        ev: Traverse[F]
    ): PekkoFutureUnlessShutdown[F[A]] =
      PekkoFutureUnlessShutdown.sequence(futures)(ev, executionContext)

    override def mapFuture[X, Y](future: PekkoFutureUnlessShutdown[X])(
        fun: PureFun[X, Y]
    ): PekkoFutureUnlessShutdown[Y] =
      future.map(fun)(executionContext)

    override def pureFuture[X](x: X): PekkoFutureUnlessShutdown[X] =
      PekkoFutureUnlessShutdown.pure(x)

    override def flatMapFuture[R1, R2](
        future1: PekkoFutureUnlessShutdown[R1],
        future2: PureFun[R1, PekkoFutureUnlessShutdown[R2]],
    ): PekkoFutureUnlessShutdown[R2] =
      future1.flatMap(future2)(executionContext)
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
    * @tparam P2PMessageT
    *   The type of P2P messages
    * @tparam InputMessageT
    *   The type of input messages, i.e., messages sent by client to the input module
    */
  final case class PekkoModuleSystemInitResult[P2PMessageT, InputMessageT](
      actorSystem: ActorSystem[ModuleControl[PekkoEnv, Unit]],
      initResult: SystemInitializationResult[P2PMessageT, InputMessageT],
  )

  private[bftordering] final class PekkoModuleSystem(
      underlyingRootActorContext: ActorContext[ModuleControl[PekkoEnv, Unit]],
      loggerFactory: NamedLoggerFactory,
  ) extends ModuleSystem[PekkoEnv] {

    override def rootActorContext: PekkoActorContext[?] =
      PekkoActorContext(this, underlyingRootActorContext, loggerFactory)

    override def futureContext: FutureContext[PekkoEnv] = PekkoFutureContext(
      underlyingRootActorContext.executionContext
    )

    override def newModuleRef[AcceptedMessageT](
        moduleName: ModuleName
    ): PekkoModuleRef[AcceptedMessageT] = newModuleRefImpl(moduleName, rootActorContext.underlying)

    protected[pekko] def newModuleRefImpl[AcceptedMessageT, ContextMessageT](
        moduleName: ModuleName,
        actorContext: ActorContext[ModuleControl[PekkoEnv, ContextMessageT]],
    ): PekkoModuleRef[AcceptedMessageT] = {
      val actorRef = actorContext.spawn(
        pekkoBehavior[AcceptedMessageT](this, moduleName, loggerFactory),
        moduleName.name,
        MailboxSelector.fromConfig("bft-ordering.control-mailbox"),
      )
      actorContext.watch(actorRef)
      PekkoModuleRef(actorRef)
    }

    override def setModule[AcceptedMessageT](
        moduleRef: PekkoModuleRef[AcceptedMessageT],
        module: framework.Module[PekkoEnv, AcceptedMessageT],
    ): Unit =
      moduleRef.ref ! SetBehavior(module, ready = false)

  }

  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
  def tryCreate[P2PMessageT, InputMessageT](
      name: String,
      systemInitializer: SystemInitializer[PekkoEnv, P2PMessageT, InputMessageT],
      p2pManager: ClientP2PNetworkManager[PekkoEnv, P2PMessageT],
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): PekkoModuleSystemInitResult[P2PMessageT, InputMessageT] = {
    val resultPromise =
      Promise[SystemInitializationResult[P2PMessageT, InputMessageT]]()

    val actorSystem = {
      val systemBehavior =
        Behaviors
          .supervise {
            Behaviors.setup[ModuleControl[PekkoEnv, Unit]] { actorContext =>
              val moduleSystem = new PekkoModuleSystem(actorContext, loggerFactory)
              resultPromise.success(systemInitializer.initialize(moduleSystem, p2pManager))
              Behaviors.same
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
