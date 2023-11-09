// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.fetchcontracts.util

import akka.NotUsed
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Partition, SinkQueueWithCancel}
import akka.stream.{FanOutShape2, Graph}
import com.daml.scalautil.Statement.discard
import doobie.free.{connection => fconn}
import scalaz.Order
import scalaz.syntax.order._
import scalaz.syntax.std.option._
import scalaz.{-\/, \/, \/-}

import scala.concurrent.{ExecutionContext, Future}

// Generic utilities for akka-streams and doobie.
private[daml] object AkkaStreamsDoobie {
  def partition[A, B]: Graph[FanOutShape2[A \/ B, A, B], NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val split = b.add(
        Partition[A \/ B](
          2,
          {
            case -\/(_) => 0
            case \/-(_) => 1
          },
        )
      )
      val as = b.add(Flow[A \/ B].collect { case -\/(a) => a })
      val bs = b.add(Flow[A \/ B].collect { case \/-(b) => b })
      discard { split ~> as }
      discard { split ~> bs }
      new FanOutShape2(split.in, as.out, bs.out)
    }

  private[fetchcontracts] def project2[A, B]: Graph[FanOutShape2[(A, B), A, B], NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val split = b add Broadcast[(A, B)](2, eagerCancel = true)
      val left = b add Flow.fromFunction((_: (A, B))._1)
      val right = b add Flow.fromFunction((_: (A, B))._2)
      discard { split ~> left }
      discard { split ~> right }
      new FanOutShape2(split.in, left.out, right.out)
    }

  private[fetchcontracts] def last[A](ifEmpty: A): Flow[A, A, NotUsed] =
    Flow[A].fold(ifEmpty)((_, later) => later)

  private[fetchcontracts] def max[A: Order](ifEmpty: A): Flow[A, A, NotUsed] =
    Flow[A].fold(ifEmpty)(_ max _)

  def sinkCioSequence_[Ign](
      f: SinkQueueWithCancel[doobie.ConnectionIO[Ign]]
  )(implicit ec: ExecutionContext): doobie.ConnectionIO[Unit] = {
    import doobie.ConnectionIO
    import doobie.free.{connection => fconn}
    def go(): ConnectionIO[Unit] = {
      val next = f.pull()
      connectionIOFuture(next)
        .flatMap(_.cata(cio => cio flatMap (_ => go()), fconn.pure(())))
    }
    fconn.handleErrorWith(
      go(),
      t => {
        f.cancel()
        fconn.raiseError(t)
      },
    )
  }

  def connectionIOFuture[A](
      fa: Future[A]
  )(implicit ec: ExecutionContext): doobie.ConnectionIO[A] =
    fconn.async[A](k => fa.onComplete(ta => k(ta.toEither)))
}
