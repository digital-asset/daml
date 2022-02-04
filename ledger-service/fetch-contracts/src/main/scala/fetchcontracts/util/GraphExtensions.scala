// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.fetchcontracts.util

import akka.NotUsed
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink}
import akka.stream.{FanOutShape2, FlowShape, Graph}
import scalaz.Liskov.<~<

import scala.concurrent.Future

object GraphExtensions {
  implicit final class `Graph FOS2 funs`[A, Y, Z, M](
      private val g: Graph[FanOutShape2[A, Y, Z], M]
  ) extends AnyVal {
    private def divertToMat[N, O](oz: Sink[Z, N])(mat: (M, N) => O): Flow[A, Y, O] =
      Flow fromGraph GraphDSL.createGraph(g, oz)(mat) { implicit b => (gs, zOut) =>
        import GraphDSL.Implicits._
        gs.out1 ~> zOut
        new FlowShape(gs.in, gs.out0)
      }

    /** Several of the graphs here have a second output guaranteed to deliver only one value.
      * This turns such a graph into a flow with the value materialized.
      */
    def divertToHead(implicit noM: M <~< NotUsed): Flow[A, Y, Future[Z]] = {
      type CK[-T] = (T, Future[Z]) => Future[Z]
      divertToMat(Sink.head)(noM.subst[CK](Keep.right[NotUsed, Future[Z]]))
    }
  }
}
