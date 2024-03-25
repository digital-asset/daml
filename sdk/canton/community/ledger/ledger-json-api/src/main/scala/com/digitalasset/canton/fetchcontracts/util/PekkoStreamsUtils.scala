// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.fetchcontracts.util

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Broadcast, Flow, GraphDSL, Partition}
import org.apache.pekko.stream.{FanOutShape2, Graph}
import com.daml.scalautil.Statement.discard
import scalaz.syntax.order.*
import scalaz.{-\/, Order, \/, \/-}

// Generic utilities for pekko-streams and doobie.
 object PekkoStreamsUtils {
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
}
