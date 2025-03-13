// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future

import cats.Traverse
import com.digitalasset.canton.data.CantonTimestamp

import scala.util.Try

/** [[RunningFuture]] represents a running future in simulation. Since the future might be built up
  * of smaller futures (coming from either zip or sequence for example), they might either have run
  * or not. So we represent this with the [[RunningFuture.IsResolved]] that can either be
  * [[RunningFuture.Resolved]] for a future that has already finished or [[RunningFuture.Scheduled]]
  * for one that will run in the future.
  *
  * It is important that we can run the futures in any order, so that simulation can faithfully
  * simulate all possible interleaving.
  *
  * @tparam T
  *   type of the future
  */
sealed trait RunningFuture[T] {
  def name: String

  def minimumScheduledTime: Option[CantonTimestamp]

  protected def runIfBelow(time: CantonTimestamp): RunningFuture[T]

  /** Should only call this if [[minimumScheduledTime]] is None */
  protected def resolve: Try[T]

  def resolveAllBelow(time: CantonTimestamp): RunningFuture.IsResolved[RunningFuture[T], T] = {
    val newFut = runIfBelow(time)

    newFut.minimumScheduledTime match {
      case Some(newTime) => RunningFuture.Scheduled(newTime, newFut)
      case None => RunningFuture.Resolved(newFut.resolve)
    }
  }
}

object RunningFuture {

  sealed trait IsResolved[Action, Value]

  final case class Resolved[Action, Value](value: Try[Value]) extends IsResolved[Action, Value]

  final case class Scheduled[Action, Value](when: CantonTimestamp, what: Action)
      extends IsResolved[Action, Value]

  final class Pure[X](theName: => String, isResolved: IsResolved[() => Try[X], X])
      extends RunningFuture[X] {
    override def name: String = theName
    override def minimumScheduledTime: Option[CantonTimestamp] = isResolved match {
      case Resolved(_) => None
      case Scheduled(when, _) => Some(when)
    }

    override def runIfBelow(time: CantonTimestamp): Pure[X] =
      isResolved match {
        case Resolved(_) => this
        case Scheduled(when, what) =>
          if (when <= time) {
            val value = what()
            new Pure(theName, Resolved(value))
          } else {
            this
          }
      }

    override protected def resolve: Try[X] = isResolved match {
      case Resolved(value) => value
      case Scheduled(_, _) => throw new Exception("Can't resolve scheduled future")
    }
  }

  final case class Zip[X, Y](fut1: RunningFuture[X], fut2: RunningFuture[Y])
      extends RunningFuture[(X, Y)] {
    override def name: String = s"(${fut1.name}).zip(${fut2.name})"

    override def minimumScheduledTime: Option[CantonTimestamp] =
      (fut1.minimumScheduledTime.toList ++ fut2.minimumScheduledTime.toList).minOption

    override protected def runIfBelow(time: CantonTimestamp): RunningFuture[(X, Y)] =
      Zip(fut1.runIfBelow(time), fut2.runIfBelow(time))

    override protected def resolve: Try[(X, Y)] =
      for {
        x <- fut1.resolve
        y <- fut2.resolve
      } yield (x, y)
  }

  final case class Sequence[X, F[_]](in: F[RunningFuture[X]], ev: Traverse[F])
      extends RunningFuture[F[X]] {
    override def name: String =
      ev.map(in)(_.name).toString

    override def minimumScheduledTime: Option[CantonTimestamp] =
      ev.toList(in).flatMap(_.minimumScheduledTime).minOption

    override protected def runIfBelow(time: CantonTimestamp): RunningFuture[F[X]] =
      Sequence(ev.map(in)(_.runIfBelow(time)), ev)

    override protected def resolve: Try[F[X]] =
      ev.sequence(ev.map(in)(_.resolve))
  }

  final case class Map[X, Y](future: RunningFuture[X], fun: X => Y) extends RunningFuture[Y] {
    override def name: String = s"(${future.name}).map(...)"
    override def minimumScheduledTime: Option[CantonTimestamp] = future.minimumScheduledTime

    override protected def runIfBelow(time: CantonTimestamp): RunningFuture[Y] =
      Map(future.runIfBelow(time), fun)

    override protected def resolve: Try[Y] = future.resolve.map(fun)
  }
}
