// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.fetchcontracts.util

import akka.NotUsed
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink}
import akka.stream.{FanOutShape2, FlowShape, Graph}
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import scalaz.Liskov.<~<

import scala.concurrent.{ExecutionContext, Future}

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

  private[daml] def logTermination[A](
      extraMessage: String
  )(implicit ec: ExecutionContext, lc: LoggingContextOf[Any]): Flow[A, A, NotUsed] =
    if (logger.trace.isEnabled)
      Flow[A].watchTermination() { (mat, fd) =>
        fd.onComplete(
          _.fold(
            { t =>
              logger.trace(s"stream-abort [$extraMessage] trying to abort ${t.getMessage}")
            },
            { _ =>
              logger.trace(s"stream-stop [$extraMessage] trying to shutdown")
            },
          )
        )
        mat
      }
    else
      Flow[A]

  private val logger = ContextualizedLogger.get(getClass)
}
