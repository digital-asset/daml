// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.unit.modules

import cats.Traverse
import cats.syntax.either.*
import com.daml.metrics.api.MetricHandle.Timer
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{
  Hash,
  HashPurpose,
  Signature,
  SignatureCheckError,
  SigningKeyUsage,
  SyncCryptoError,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressingLogger}
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.{
  CancellableEvent,
  Env,
  Module,
  ModuleContext,
  ModuleName,
  ModuleRef,
  PureFun,
}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.Assertions.fail

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

import UnitTestContext.DelayCount

/** Convenience unit test [[Env]] with ignored operations.
  */
class UnitTestEnv extends Env[UnitTestEnv] {
  override type ActorContextT[MessageT] = UnitTestContext[UnitTestEnv, MessageT]
  override type FutureUnlessShutdownT[_] = Unit
}

class UnitTestContext[E <: Env[E], MessageT] extends ModuleContext[E, MessageT] {

  override def loggerFactory: NamedLoggerFactory = SuppressingLogger(getClass)

  override def self: E#ModuleRefT[MessageT] =
    unsupported()

  override def newModuleRef[NewModuleMessageT](
      moduleName: ModuleName
  ): E#ModuleRefT[NewModuleMessageT] = unsupported()

  override def setModule[NewModuleMessageT](
      moduleRef: E#ModuleRefT[NewModuleMessageT],
      module: Module[E, NewModuleMessageT],
  ): Unit =
    unsupported()

  override def delayedEvent(delay: FiniteDuration, message: MessageT): CancellableEvent =
    unsupported()

  // Metrics are not produced in unit tests
  override def timeFuture[X](timer: Timer, futureUnlessShutdown: => E#FutureUnlessShutdownT[X])(
      implicit mc: MetricsContext
  ): E#FutureUnlessShutdownT[X] =
    unsupported()

  override def zipFuture[X, Y](
      future1: E#FutureUnlessShutdownT[X],
      future2: E#FutureUnlessShutdownT[Y],
  ): E#FutureUnlessShutdownT[(X, Y)] = unsupported()

  override def sequenceFuture[A, F[_]](futures: F[E#FutureUnlessShutdownT[A]])(implicit
      ev: Traverse[F]
  ): E#FutureUnlessShutdownT[F[A]] = unsupported()

  override def mapFuture[X, Y](future: E#FutureUnlessShutdownT[X])(
      fun: PureFun[X, Y]
  ): E#FutureUnlessShutdownT[Y] = unsupported()

  override def pureFuture[X](x: X): E#FutureUnlessShutdownT[X] = unsupported()

  override def pipeToSelfInternal[X](
      futureUnlessShutdown: E#FutureUnlessShutdownT[X]
  )(fun: Try[X] => Option[MessageT]): Unit =
    unsupported()

  override def blockingAwait[X](future: E#FutureUnlessShutdownT[X]): X = unsupported()
  override def blockingAwait[X](future: E#FutureUnlessShutdownT[X], duration: FiniteDuration): X =
    unsupported()

  override def abort(): Nothing = fail()

  override def abort(msg: String): Nothing = fail(msg)

  override def abort(failure: Throwable): Nothing = fail(failure)

  override def become(module: Module[E, MessageT]): Unit = unsupported()

  override def stop(onStop: () => Unit): Unit = unsupported()

  private def unsupported() =
    fail("Unsupported by unit tests")
}

object UnitTestContext {
  def apply[MessageT](): UnitTestContext[UnitTestEnv, MessageT] =
    new UnitTestContext[UnitTestEnv, MessageT]

  type DelayCount = Int
}

class SelfEnv extends Env[SelfEnv] {
  override type ActorContextT[MessageT] = SelfContext[MessageT]
  override type FutureUnlessShutdownT[_] = Unit
  override type ModuleRefT[MessageT] = ModuleRef[MessageT]
}

class SelfContext[MessageT] extends UnitTestContext[SelfEnv, MessageT] {

  var selfRef: Option[ModuleRef[MessageT]] = None

  override def self: ModuleRef[MessageT] =
    selfRef.getOrElse(fail("selfRef not set"))
}

object SelfContext {
  def apply[MessageT](): SelfContext[MessageT] = new SelfContext[MessageT]
}

/** Convenience unit test [[Env]] ignoring messages.
  */
abstract class BaseIgnoringUnitTestEnv[E <: BaseIgnoringUnitTestEnv[E]] extends Env[E] {
  override type FutureUnlessShutdownT[X] = () => X
  // override type ModuleRefT[-MessageT] = ModuleRef[MessageT]
}

class IgnoringUnitTestEnv extends BaseIgnoringUnitTestEnv[IgnoringUnitTestEnv] {
  override type ActorContextT[MessageT] = IgnoringUnitTestContext[MessageT]
  override type ModuleRefT[-MessageT] = IgnoringModuleRef[MessageT]
}

final case class IgnoringUnitTestContext[MessageT]()
    extends UnitTestContext[IgnoringUnitTestEnv, MessageT] {
  override def self: IgnoringModuleRef[MessageT] = new IgnoringModuleRef()

  override def delayedEvent(delay: FiniteDuration, message: MessageT): CancellableEvent =
    fakeCancellableEventExpectingSilence

  override def pipeToSelfInternal[X](futureUnlessShutdown: () => X)(
      fun: Try[X] => Option[MessageT]
  ): Unit = ()

  override def blockingAwait[X](future: () => X): X = future()
  override def blockingAwait[X](future: () => X, duration: FiniteDuration): X = future()
}

final class IgnoringModuleRef[-MessageT] extends ModuleRef[MessageT] {
  override def asyncSend(msg: MessageT): Unit = ()
}

/** Convenience unit test [[Env]] storing delayed messages, with support for cancellation.
  */
class FakeTimerCellUnitTestEnv extends BaseIgnoringUnitTestEnv[FakeTimerCellUnitTestEnv] {
  override type ActorContextT[MessageT] = FakeTimerCellUnitTestContext[MessageT]
  override type ModuleRefT[MessageT] = ModuleRef[MessageT]
}

final case class FakeTimerCellUnitTestContext[MessageT](
    cell: AtomicReference[Option[(DelayCount, MessageT)]]
) extends UnitTestContext[FakeTimerCellUnitTestEnv, MessageT] {
  private var delayCount: DelayCount = 0

  def reset(): Unit = {
    delayCount = 0
    cell.set(None)
  }

  override def self: IgnoringModuleRef[MessageT] = new IgnoringModuleRef()

  override def delayedEvent(delay: FiniteDuration, message: MessageT): CancellableEvent = {
    delayCount += 1
    val newDelayCount = delayCount
    cell.set(Some(newDelayCount -> message))
    () => true
  }

  override def pipeToSelfInternal[X](future: () => X)(fun: Try[X] => Option[MessageT]): Unit = ()

  override def blockingAwait[X](future: () => X): X = future()
  override def blockingAwait[X](future: () => X, duration: FiniteDuration): X = future()
}

/** Convenience unit test [[Env]] storing single pipeToSelf message.
  */
class FakePipeToSelfCellUnitTestEnv extends BaseIgnoringUnitTestEnv[FakePipeToSelfCellUnitTestEnv] {
  override type ActorContextT[MessageT] = FakePipeToSelfCellUnitTestContext[MessageT]
  override type ModuleRefT[MessageT] = ModuleRef[MessageT]
}

final case class FakePipeToSelfCellUnitTestContext[MessageT](
    cell: AtomicReference[Option[() => Option[MessageT]]]
) extends UnitTestContext[FakePipeToSelfCellUnitTestEnv, MessageT] {
  override def self: ModuleRef[MessageT] = new IgnoringModuleRef()

  override def timeFuture[X](timer: Timer, futureUnlessShutdown: => () => X)(implicit
      mc: MetricsContext
  ): () => X =
    futureUnlessShutdown

  override def sequenceFuture[A, F[_]](futures: F[() => A])(implicit
      ev: Traverse[F]
  ): () => F[A] =
    ev.sequence(futures)

  override def zipFuture[X, Y](future1: () => X, future2: () => Y): () => (X, Y) = () =>
    (future1(), future2())

  override def pipeToSelfInternal[X](futureUnlessShutdown: () => X)(
      fun: Try[X] => Option[MessageT]
  ): Unit =
    cell.set(Some(() => fun(Try(futureUnlessShutdown()))))

  override def blockingAwait[X](future: () => X): X = future()
  override def blockingAwait[X](future: () => X, duration: FiniteDuration): X = future()

  override def pureFuture[X](x: X): () => X = () => x

  override def delayedEvent(delay: FiniteDuration, message: MessageT): CancellableEvent = () => true
}

/** Convenience unit test [[Env]] storing queue of pipeToSelf messages.
  */
class FakePipeToSelfQueueUnitTestEnv
    extends BaseIgnoringUnitTestEnv[FakePipeToSelfQueueUnitTestEnv] {
  override type ActorContextT[MessageT] = FakePipeToSelfQueueUnitTestContext[MessageT]
  override type ModuleRefT[MessageT] = ModuleRef[MessageT]
}

final case class FakePipeToSelfQueueUnitTestContext[MessageT](
    queue: mutable.Queue[() => Option[MessageT]]
) extends UnitTestContext[FakePipeToSelfQueueUnitTestEnv, MessageT] {
  override def self: ModuleRef[MessageT] = new IgnoringModuleRef()

  override def timeFuture[X](timer: Timer, futureUnlessShutdown: => () => X)(implicit
      mc: MetricsContext
  ): () => X =
    futureUnlessShutdown

  override def pipeToSelfInternal[X](future: () => X)(fun: Try[X] => Option[MessageT]): Unit =
    queue.addOne(() => fun(Try(future())))
}

class ProgrammableUnitTestEnv extends BaseIgnoringUnitTestEnv[ProgrammableUnitTestEnv] {
  override type ActorContextT[X] = ProgrammableUnitTestContext[X]
  override type FutureUnlessShutdownT[X] = () => X
  override type ModuleRefT[X] = ModuleRef[X]
}

object ProgrammableUnitTestEnv {
  private[unit] case object noSignatureCryptoProvider
      extends CryptoProvider[ProgrammableUnitTestEnv] {
    override def sign(hash: Hash, usage: NonEmpty[Set[SigningKeyUsage]])(implicit
        traceContext: TraceContext
    ): () => Either[SyncCryptoError, Signature] = () => Right(Signature.noSignature)

    override def signMessage[MessageT <: ProtocolVersionedMemoizedEvidence & MessageFrom](
        message: MessageT,
        hashPurpose: HashPurpose,
        usage: NonEmpty[Set[SigningKeyUsage]],
    )(implicit traceContext: TraceContext): () => Either[SyncCryptoError, SignedMessage[MessageT]] =
      () => Right(SignedMessage(message, Signature.noSignature))

    override def verifySignature(hash: Hash, member: SequencerId, signature: Signature)(implicit
        traceContext: TraceContext
    ): () => Either[SignatureCheckError, Unit] = () => Either.unit
  }
}

final class ProgrammableUnitTestContext[MessageT](resolveAwaits: Boolean = false)
    extends UnitTestContext[ProgrammableUnitTestEnv, MessageT] {
  private val pipedQueue = mutable.Queue.empty[() => Option[MessageT]]
  private val delayedQueue = mutable.Queue.empty[MessageT]
  private var lastCancelledEventCell: Option[(Int, MessageT)] = None
  private val selfQueue = mutable.Queue.empty[MessageT]
  private val becomesQueue = mutable.Queue.empty[Module[ProgrammableUnitTestEnv, MessageT]]
  private var closeActionCell: Option[() => Unit] = None

  override def self: ModuleRef[MessageT] = (msg: MessageT) => selfQueue.addOne(msg)

  override def delayedEvent(delay: FiniteDuration, message: MessageT): CancellableEvent = {
    delayedQueue.addOne(message)
    val delayCount = delayedQueue.size
    () => {
      lastCancelledEventCell = Some(delayCount -> message)
      true
    }
  }

  override def pipeToSelfInternal[X](future: () => X)(fun: Try[X] => Option[MessageT]): Unit =
    pipedQueue.addOne(() => fun(Try(future())))

  def runPipedMessagesAndReceiveOnModule(
      module: Module[ProgrammableUnitTestEnv, MessageT]
  )(implicit traceContext: TraceContext): Unit =
    runPipedMessagesThenVerifyAndReceiveOnModule(module)(_ => ())

  def runPipedMessagesThenVerifyAndReceiveOnModule(
      module: Module[ProgrammableUnitTestEnv, MessageT]
  )(verify: MessageT => Unit)(implicit traceContext: TraceContext): Unit =
    runPipedMessages().foreach { message =>
      verify(message)
      module.receive(message)(this, traceContext)
    }

  def runPipedMessages(): Seq[MessageT] = {
    val actions = pipedQueue.toSeq
    pipedQueue.clear()
    actions.flatMap(_())
  }

  def selfMessages: Seq[MessageT] = selfQueue.toSeq

  def extractSelfMessages(): Seq[MessageT] = {
    val actions = selfQueue.toSeq
    selfQueue.clear()
    actions
  }

  def delayedMessages: Seq[MessageT] = delayedQueue.toSeq

  /** @return the count of scheduled events plus the last scheduled event. None if no events have been scheduled.
    */
  def lastDelayedMessage: Option[(Int, MessageT)] =
    delayedQueue.lastOption.map(msg => (delayedQueue.size, msg))

  /** @return the last scheduled event plus its count corresponding to the number
    *         of scheduled events when that event was initially scheduled.
    *         None if no events have been cancelled.
    */
  def lastCancelledEvent: Option[(Int, MessageT)] = lastCancelledEventCell

  def sizeOfPipedMessages: Int = pipedQueue.size

  override def blockingAwait[X](future: () => X): X =
    if (resolveAwaits)
      future()
    else
      super.blockingAwait(future)

  override def blockingAwait[X](future: () => X, duration: FiniteDuration): X =
    blockingAwait(future)

  override def timeFuture[X](timer: Timer, futureUnlessShutdown: => () => X)(implicit
      mc: MetricsContext
  ): () => X = futureUnlessShutdown

  override def zipFuture[X, Y](future1: () => X, future2: () => Y): () => (X, Y) =
    () => (future1(), future2())

  override def sequenceFuture[A, F[_]](futures: F[() => A])(implicit ev: Traverse[F]): () => F[A] =
    ev.sequence(futures)

  override def become(module: Module[ProgrammableUnitTestEnv, MessageT]): Unit =
    becomesQueue.enqueue(module)

  def extractBecomes(): Seq[Module[ProgrammableUnitTestEnv, MessageT]] = {
    val becomes = becomesQueue.toSeq
    becomesQueue.clear()
    becomes
  }

  override def stop(onStop: () => Unit): Unit = closeActionCell = Some(onStop)

  def runCloseAction(): Unit = closeActionCell.getOrElse(abort("No close action defined"))()
}
