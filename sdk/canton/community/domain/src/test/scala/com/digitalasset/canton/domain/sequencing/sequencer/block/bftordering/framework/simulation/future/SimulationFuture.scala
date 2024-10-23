// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.future

import cats.Traverse
import com.digitalasset.canton.data.CantonTimestamp

import scala.util.Try

sealed trait SimulationFuture[T] {
  def resolveValue(): Try[T]

  def schedule(timeGenerator: () => CantonTimestamp): RunningFuture[T]
}

object SimulationFuture {
  final case class Pure[T](fun: () => Try[T]) extends SimulationFuture[T] {
    override def resolveValue(): Try[T] = fun()

    override def schedule(timeGenerator: () => CantonTimestamp): RunningFuture[T] =
      RunningFuture.Pure(RunningFuture.Scheduled(timeGenerator(), () => resolveValue()))
  }

  final case class Zip[X, Y](fut1: SimulationFuture[X], fut2: SimulationFuture[Y])
      extends SimulationFuture[(X, Y)] {
    override def resolveValue(): Try[(X, Y)] =
      fut1.resolveValue().flatMap(x => fut2.resolveValue().map(y => (x, y)))

    override def schedule(timeGenerator: () => CantonTimestamp): RunningFuture[(X, Y)] =
      RunningFuture.Zip(fut1.schedule(timeGenerator), fut2.schedule(timeGenerator))
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

  def apply[T](resolveValue: () => Try[T]): SimulationFuture[T] = Pure(resolveValue)
}
