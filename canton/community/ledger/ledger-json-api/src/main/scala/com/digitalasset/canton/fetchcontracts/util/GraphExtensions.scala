// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.fetchcontracts.util

import akka.NotUsed
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink}
import akka.stream.{FanOutShape2, FlowShape, Graph}
import com.daml.logging.LoggingContextOf
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.NoTracing
import scalaz.Liskov.<~<

import scala.concurrent.{ExecutionContext, Future}

object GraphExtensions extends NoTracing {
  implicit final class `Graph FOS2 funs`[A, Y, Z, M](
      private val g: Graph[FanOutShape2[A, Y, Z], M]
  ) extends AnyVal {
    private def divertToMat[N, O](oz: Sink[Z, N])(mat: (M, N) => O): Flow[A, Y, O] =
      Flow fromGraph GraphDSL.createGraph(g, oz)(mat) { implicit b => (gs, zOut) =>
        import GraphDSL.Implicits.*
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

  def logTermination[A](
      logger: TracedLogger,
      extraMessage: String,
  )(implicit ec: ExecutionContext, lc: LoggingContextOf[Any]): Flow[A, A, NotUsed] =
    if (logger.underlying.isTraceEnabled) {
      Flow[A].watchTermination() { (mat, fd) =>
        fd.onComplete(
          _.fold(
            { t =>
              logger.trace(
                s"stream-abort [$extraMessage] trying to abort ${t.getMessage}, ${lc.makeString}"
              )
            },
            { _ =>
              logger.trace(s"stream-stop [$extraMessage] trying to shutdown, ${lc.makeString}")
            },
          )
        )
        mat
      }
    } else
      Flow[A]

}
