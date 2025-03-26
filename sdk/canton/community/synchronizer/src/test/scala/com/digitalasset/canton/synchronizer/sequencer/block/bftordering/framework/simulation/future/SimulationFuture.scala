// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future

import cats.Traverse
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.PureFun

import scala.util.Try

sealed trait SimulationFuture[T] {
  def resolveValue(): Try[T]

  def schedule(timeGenerator: () => CantonTimestamp): RunningFuture[T]
}

object SimulationFuture {
  final class Pure[T](name: => String, fun: () => Try[T]) extends SimulationFuture[T] {
    override def resolveValue(): Try[T] = fun()

    override def schedule(timeGenerator: () => CantonTimestamp): RunningFuture[T] =
      new RunningFuture.Pure(name, RunningFuture.Scheduled(timeGenerator(), () => resolveValue()))
  }

  final case class Zip[X, Y](fut1: SimulationFuture[X], fut2: SimulationFuture[Y])
      extends SimulationFuture[(X, Y)] {
    override def resolveValue(): Try[(X, Y)] =
      fut1.resolveValue().flatMap(x => fut2.resolveValue().map(y => (x, y)))

    override def schedule(timeGenerator: () => CantonTimestamp): RunningFuture[(X, Y)] =
      RunningFuture.Zip(fut1.schedule(timeGenerator), fut2.schedule(timeGenerator))
  }

  final case class Zip3[X, Y, Z](
      fut1: SimulationFuture[X],
      fut2: SimulationFuture[Y],
      fut3: SimulationFuture[Z],
  ) extends SimulationFuture[(X, Y, Z)] {
    override def resolveValue(): Try[(X, Y, Z)] =
      for {
        f1 <- fut1.resolveValue()
        f2 <- fut2.resolveValue()
        f3 <- fut3.resolveValue()
      } yield (f1, f2, f3)

    override def schedule(timeGenerator: () => CantonTimestamp): RunningFuture[(X, Y, Z)] =
      RunningFuture.Zip3(
        fut1.schedule(timeGenerator),
        fut2.schedule(timeGenerator),
        fut3.schedule(timeGenerator),
      )
  }

  final case class Sequence[A, F[_]](in: F[SimulationFuture[A]])(implicit ev: Traverse[F])
      extends SimulationFuture[F[A]] {
    override def resolveValue(): Try[F[A]] = ev.sequence(ev.map(in)(_.resolveValue()))

    override def schedule(timeGenerator: () => CantonTimestamp): RunningFuture[F[A]] =
      RunningFuture.Sequence(ev.map(in)(_.schedule(timeGenerator)), ev)
  }

  final case class Map[X, Y](future: SimulationFuture[X], fun: X => Y) extends SimulationFuture[Y] {
    override def resolveValue(): Try[Y] = future.resolveValue().map(fun)

    override def schedule(timeGenerator: () => CantonTimestamp): RunningFuture[Y] =
      RunningFuture.Map(future.schedule(timeGenerator), fun)
  }

  final case class FlatMap[R1, R2](
      fut1: SimulationFuture[R1],
      fut2: PureFun[R1, SimulationFuture[R2]],
  ) extends SimulationFuture[R2] {
    override def resolveValue(): Try[R2] =
      fut1.resolveValue().map(fut2).flatMap(_.resolveValue())

    // TODO(#23754): support finer-grained simulation of `FlatMap` futures
    override def schedule(timeGenerator: () => CantonTimestamp): RunningFuture[R2] =
      new RunningFuture.Pure(
        "flatMap",
        RunningFuture.Scheduled(timeGenerator(), () => resolveValue()),
      )
  }

  def apply[T](name: => String)(resolveValue: () => Try[T]): SimulationFuture[T] =
    new Pure(name, resolveValue)
}
