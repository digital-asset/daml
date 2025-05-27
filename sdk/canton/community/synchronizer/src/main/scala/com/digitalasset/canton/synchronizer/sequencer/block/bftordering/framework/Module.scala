// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework

import cats.Traverse
import com.daml.metrics.api.MetricHandle.Timer
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.DoNotDiscardLikeFuture
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{FlagCloseable, UnlessShutdown}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Consensus,
  Output,
  P2PNetworkOut,
}
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.dispatch.ControlMessage

import java.time.Instant
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

final case class ModuleName(name: String)

/** Modules abstract actors away from the concrete actor framework, mainly so that their logic can
  * be more easily deterministically simulation-tested.
  *
  * @tparam E
  *   An environment corresponding to the actor framework, such as Pekko or the deterministic
  *   simulation testing framework.
  * @tparam MessageT
  *   The root message type understood by the actor.
  */
trait Module[E <: Env[E], MessageT] extends NamedLogging with FlagCloseable {

  /** The module's message handler.
    *
    * @param context
    *   Environment-specific information, such as the representation of the actor's state.
    */
  final def receive(
      message: MessageT
  )(implicit
      context: E#ActorContextT[MessageT],
      traceContext: TraceContext,
  ): Unit =
    try {
      performUnlessClosing("receive")(receiveInternal(message))
        .onShutdown {
          logger.info(s"Received $message but won't process because we're shutting down")
        }
    } catch {
      case t: Throwable =>
        logger.error(
          s"Internal: unexpected exception thrown in module while processing $message",
          t,
        )
        context.abort(t)
    }

  /** Called by the system construction logic when the system is functional and, in particular, the
    * module can send messages to references (including itself).
    *
    * It is also called by the module system when the module changes behavior.
    */
  def ready(self: ModuleRef[MessageT]): Unit = ()

  protected def receiveInternal(
      message: MessageT
  )(implicit
      context: E#ActorContextT[MessageT],
      traceContext: TraceContext,
  ): Unit

  // Modules are designed to enable simulation testing, so while we need the functionality
  //  made available by FlagCloseable, they must not hold resources and don't need to be closed.
  override protected def onClosed(): Unit = ()

  final def pipeToSelf[X](futureUnlessShutdown: E#FutureUnlessShutdownT[X])(
      fun: Try[X] => MessageT
  )(implicit
      context: E#ActorContextT[MessageT],
      traceContext: TraceContext,
      merticsContext: MetricsContext,
  ): Unit =
    context.pipeToSelf(futureUnlessShutdown)(fun.andThen(Some(_)))

  final def pipeToSelf[X](futureUnlessShutdown: E#FutureUnlessShutdownT[X], timer: Timer)(
      fun: Try[X] => MessageT
  )(implicit
      context: E#ActorContextT[MessageT],
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): Unit =
    context.pipeToSelf(context.timeFuture(timer, futureUnlessShutdown))(fun.andThen(Some(_)))

  final protected def abort(
      msg: String
  )(implicit
      context: E#ActorContextT[MessageT],
      traceContext: TraceContext,
  ): Nothing = {
    // Ensure to log the failure, as exceptions may be swallowed by the actor framework
    logError(msg)
    context.abort(msg)
  }

  final protected def abort(
      msg: String,
      failure: Throwable,
  )(implicit
      context: E#ActorContextT[MessageT],
      traceContext: TraceContext,
  ): Nothing = {
    // Ensure that the log contains the failure, as exceptions may be swallowed by the actor framework
    logError(msg, failure)
    context.abort(failure)
  }

  // Aborting initialization in Canton shouldn't kill the whole process, as it may contain several nodes,
  //  but rather only the module/sequencer.
  final protected def abortInit(
      msg: String
  ): Nothing = {
    // Ensure that the log contains the failure, as exceptions may be swallowed by the actor framework
    logError(msg)(TraceContext.empty)
    sys.error(msg)
  }

  final protected def abortInit(
      msg: String,
      failure: Throwable,
  ): Nothing = {
    // Ensure that the log contains the failure, as exceptions may be swallowed by the actor framework
    logError(msg, failure)(TraceContext.empty)
    throw failure
  }

  private def logError(msg: String, failure: Throwable)(implicit
      traceContext: TraceContext
  ): Unit =
    logger.error(msg, failure)

  private def logError(msg: String)(implicit traceContext: TraceContext): Unit =
    logger.error(msg)
}

/** Modules abstract actor references away from the concrete actor framework, mainly so that their
  * logic can be more easily deterministically simulation-tested.
  *
  * @tparam AcceptedMessageT
  *   The root message type understood by the actor.
  */
trait ModuleRef[-AcceptedMessageT] {

  /** The module reference's asynchronous send operation.
    */
  def asyncSend(
      msg: AcceptedMessageT
  )(implicit metricsContext: MetricsContext): Unit =
    asyncSendTraced(msg)(TraceContext.empty, metricsContext)

  /** Send operation that is also providing the current TraceContext
    */
  def asyncSendTraced(
      msg: AcceptedMessageT
  )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit
}

/** An abstraction of the network for deterministic simulation testing purposes.
  */
trait P2PNetworkRef[-P2PMessageT] extends FlagCloseable {
  def asyncP2PSend(createMessage: Option[Instant] => P2PMessageT)(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): Unit
}

/** An abstraction of the P2P network manager for deterministic simulation testing purposes.
  */
trait ClientP2PNetworkManager[E <: Env[E], -P2PMessageT] {

  def createNetworkRef[ActorContextT](
      context: E#ActorContextT[ActorContextT],
      endpoint: P2PEndpoint,
  )(
      onSequencerId: (P2PEndpoint.Id, BftNodeId) => Unit
  ): P2PNetworkRef[P2PMessageT]
}

/** An abstraction of cancelable delayedEvent for deterministic simulation testing purposes.
  */
trait CancellableEvent {

  /** @return
    *   True if the cancellation was successful.
    */
  def cancel(): Boolean
}

/** FutureContext contains functions for creating and combining E#FutureUnlessShutdown that will be
  * safe to use in pipeToSelf.
  */
trait FutureContext[E <: Env[E]] {

  def timeFuture[X](timer: Timer, futureUnlessShutdown: => E#FutureUnlessShutdownT[X])(implicit
      mc: MetricsContext
  ): E#FutureUnlessShutdownT[X]

  def pureFuture[X](x: X): E#FutureUnlessShutdownT[X]

  /** [[mapFuture]] requires a [[PureFun]] instead of a normal [[scala.Function1]] since we need to
    * be careful not to mutate state of the modules in the [[Env#FutureUnlessShutdownT]], as this
    * would violate the assumptions we use when writing [[Module]]s.
    */
  def mapFuture[X, Y](future: E#FutureUnlessShutdownT[X])(
      fun: PureFun[X, Y]
  ): E#FutureUnlessShutdownT[Y]

  def zipFuture[X, Y](
      future1: E#FutureUnlessShutdownT[X],
      future2: E#FutureUnlessShutdownT[Y],
  ): E#FutureUnlessShutdownT[(X, Y)]

  def zipFuture[X, Y, Z](
      future1: E#FutureUnlessShutdownT[X],
      future2: E#FutureUnlessShutdownT[Y],
      future3: E#FutureUnlessShutdownT[Z],
  ): E#FutureUnlessShutdownT[(X, Y, Z)]

  def sequenceFuture[A, F[_]](futures: F[E#FutureUnlessShutdownT[A]])(implicit
      ev: Traverse[F]
  ): E#FutureUnlessShutdownT[F[A]]

  /** [[flatMapFuture]] requires a [[PureFun]] instead of a normal [[scala.Function1]] for similar
    * reason as [[mapFuture]]
    */
  def flatMapFuture[R1, R2](
      future1: E#FutureUnlessShutdownT[R1],
      future2: PureFun[R1, E#FutureUnlessShutdownT[R2]],
  ): E#FutureUnlessShutdownT[R2]
}

/** An abstraction of actor contexts for deterministic simulation testing purposes.
  */
trait ModuleContext[E <: Env[E], MessageT] extends NamedLogging with FutureContext[E] {

  // Client API, used by system construction logic

  def newModuleRef[NewModuleMessageT](
      moduleName: ModuleName
  ): E#ModuleRefT[NewModuleMessageT]

  /** Spawns a new module. The `module` handler object must not be spawned more than once, lest it
    * potentially cause a violation of the actor model, as its state could be accessed concurrently.
    */
  def setModule[OtherModuleMessageT](
      moduleRef: E#ModuleRefT[OtherModuleMessageT],
      module: Module[E, OtherModuleMessageT],
  ): Unit

  // Handler API, used by module implementations

  def self: E#ModuleRefT[MessageT]

  def delayedEvent(delay: FiniteDuration, message: MessageT)(implicit
      metricsContext: MetricsContext
  ): CancellableEvent =
    delayedEventTraced(delay, message)(TraceContext.empty, metricsContext)

  def delayedEventTraced(delay: FiniteDuration, messageT: MessageT)(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): CancellableEvent

  /** Similar to TraceContext.withNewTraceContext but can be deterministically simulated
    */
  def withNewTraceContext[A](fn: TraceContext => A): A

  def futureContext: FutureContext[E]

  final override def timeFuture[X](
      timer: Timer,
      futureUnlessShutdown: => E#FutureUnlessShutdownT[X],
  )(implicit
      metricsContext: MetricsContext
  ): E#FutureUnlessShutdownT[X] =
    futureContext.timeFuture(timer, futureUnlessShutdown)

  final override def pureFuture[X](x: X): E#FutureUnlessShutdownT[X] = futureContext.pureFuture(x)

  final override def mapFuture[X, Y](future: E#FutureUnlessShutdownT[X])(
      fun: PureFun[X, Y]
  ): E#FutureUnlessShutdownT[Y] = futureContext.mapFuture(future)(fun)

  final override def zipFuture[X, Y](
      future1: E#FutureUnlessShutdownT[X],
      future2: E#FutureUnlessShutdownT[Y],
  ): E#FutureUnlessShutdownT[(X, Y)] = futureContext.zipFuture(future1, future2)

  final override def zipFuture[X, Y, Z](
      future1: E#FutureUnlessShutdownT[X],
      future2: E#FutureUnlessShutdownT[Y],
      future3: E#FutureUnlessShutdownT[Z],
  ): E#FutureUnlessShutdownT[(X, Y, Z)] = futureContext.zipFuture(future1, future2, future3)

  final override def sequenceFuture[A, F[_]](futures: F[E#FutureUnlessShutdownT[A]])(implicit
      ev: Traverse[F]
  ): E#FutureUnlessShutdownT[F[A]] = futureContext.sequenceFuture(futures)

  final override def flatMapFuture[R1, R2](
      future1: E#FutureUnlessShutdownT[R1],
      future2: PureFun[R1, E#FutureUnlessShutdownT[R2]],
  ): E#FutureUnlessShutdownT[R2] = futureContext.flatMapFuture(future1, future2)

  def pipeToSelf[X](futureUnlessShutdown: E#FutureUnlessShutdownT[X])(
      fun: Try[X] => Option[MessageT]
  )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit =
    pipeToSelfInternal(futureUnlessShutdown) {
      UnlessShutdown.recoverFromAbortException(_) match {
        case Success(Outcome(x)) => fun(Success(x))
        case Success(AbortedDueToShutdown) =>
          logger.info("Can't complete future, shutting down")
          stop()
          None
        case Failure(ex) => fun(Failure(ex))
      }
    }

  protected def pipeToSelfInternal[X](futureUnlessShutdown: E#FutureUnlessShutdownT[X])(
      fun: Try[X] => Option[MessageT]
  )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit

  def blockingAwait[X](future: E#FutureUnlessShutdownT[X]): X

  def blockingAwait[X](future: E#FutureUnlessShutdownT[X], duration: FiniteDuration): X

  def become(module: Module[E, MessageT]): Unit

  def stop(onStop: () => Unit = () => ()): Unit

  // Aborting in Canton shouldn't kill the whole process, as it may contain several nodes,
  //  but rather only the module/sequencer.

  def abort(): Nothing

  def abort(msg: String): Nothing

  def abort(failure: Throwable): Nothing
}

/** An environment defines the concrete actor context, reference and timer times for a specific
  * actor framework, such as Pekko or the deterministic simulation testing framework.
  *
  * A bit of theory: This type utilizes F-bounded polymorphism, meaning that [[Env]] is
  * parameterized over its own subtypes. This enables passing the implementing type as an argument
  * to the superclass, facilitating the use of more specific argument and return types where
  * subtypes of [[Env]] are present. Another commonly used pattern for this use case is the
  * type-class pattern (also known as ad-hoc polymorphism), you can read more about both here:
  * https://stackoverflow.com/questions/59813323/advantages-of-f-bounded-polymorphism-over-typeclass-for-return-current-type-prob
  */
trait Env[E <: Env[E]] {
  type ActorContextT[MessageT] <: ModuleContext[E, MessageT]
  type ModuleRefT[AcceptedMessageT] <: ModuleRef[AcceptedMessageT]
  @DoNotDiscardLikeFuture
  type FutureUnlessShutdownT[_]
}

/** A module system abstracts how modules are constructed away from the concrete actors framework,
  * such as Pekko or the deterministic simulation testing framework.
  *
  * Note that modules, unlike actors, are arranged in a static topology at bootstrap.
  */
trait ModuleSystem[E <: Env[E]] {

  def rootActorContext: E#ActorContextT[?]

  def futureContext: FutureContext[E]

  def newModuleRef[AcceptedMessageT](
      moduleName: ModuleName
  ): E#ModuleRefT[AcceptedMessageT]

  def setModule[AcceptedMessageT](
      moduleRef: E#ModuleRefT[AcceptedMessageT],
      module: Module[E, AcceptedMessageT],
  ): Unit
}

object Module {

  protected[framework] sealed trait ModuleControl[E <: Env[E], AcceptedMessageT] extends Product
  protected[framework] object ModuleControl {
    final case class Send[E <: Env[E], AcceptedMessageT](
        message: AcceptedMessageT,
        traceContext: TraceContext,
        metricsContext: MetricsContext,
        // The following fields are only used for metrics
        maybeSendInstant: Option[Instant] = None,
        maybeDelay: Option[FiniteDuration] = None,
    ) extends ModuleControl[E, AcceptedMessageT]

    final case class SetBehavior[E <: Env[E], AcceptedMessageT](
        module: Module[E, AcceptedMessageT],
        ready: Boolean,
    ) extends ModuleControl[E, AcceptedMessageT]
        with ControlMessage

    final case class NoOp[E <: Env[E], AcceptedMessageT]()
        extends ModuleControl[E, AcceptedMessageT]

    final case class Stop[E <: Env[E], AcceptedMessageT](onStop: () => Unit)
        extends ModuleControl[E, AcceptedMessageT]
        with ControlMessage
  }

  /** A system initializer defines how a specific modular distributed system is built independently
    * of the concrete actors framework, such as Pekko or the simulation testing framework, as to
    * further reduce the gap between what is run and what is deterministically simulation-tested.
    *
    * Inputs are a module system and a network manager; the latter defines how nodes connect.
    */
  trait SystemInitializer[E <: Env[E], P2PMessageT, InputMessageT] {
    def initialize(
        moduleSystem: ModuleSystem[E],
        networkManager: ClientP2PNetworkManager[E, P2PMessageT],
    ): SystemInitializationResult[P2PMessageT, InputMessageT]
  }

  /** The result of initializing a module system independent of the actor framework, to be used
    * during the actor framework-specific initialization.
    */
  final case class SystemInitializationResult[P2PMessageT, InputMessageT](
      inputModuleRef: ModuleRef[InputMessageT],
      p2pNetworkInModuleRef: ModuleRef[P2PMessageT],
      p2pNetworkOutAdminModuleRef: ModuleRef[P2PNetworkOut.Admin],
      consensusAdminModuleRef: ModuleRef[Consensus.Admin],
      outputModuleRef: ModuleRef[Output.SequencerSnapshotMessage],
  )
}
