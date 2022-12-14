// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import akka.NotUsed
import akka.stream.scaladsl.{Flow, FlowOpsMat}
import Logging.InstanceUUID
import com.daml.fetchcontracts.util.GraphExtensions._
import com.daml.logging.LoggingContextOf
import scalaz.{-\/, \/}
import scala.concurrent.ExecutionContext

object FlowUtil {
  def allowOnlyFirstInput[E, A](error: => E): Flow[E \/ A, E \/ A, NotUsed] =
    Flow[E \/ A]
      .scan(Option.empty[E \/ A]) { (s0, x) =>
        s0 match {
          case Some(_) =>
            Some(-\/(error))
          case None =>
            Some(x)
        }
      }
      .collect { case Some(x) =>
        x
      }

  type FlowOpsMatAux[+Out, +Mat, ReprMat0[+_, +_]] = FlowOpsMat[Out, Mat] {
    type ReprMat[+O, +M] = ReprMat0[O, M]
  }

  private[http] implicit final class `Flow flatMapMerge`[In, Out, Mat](
      private val self: Flow[In, Out, Mat]
  ) extends AnyVal {
    import akka.stream.{Graph, SourceShape}
    import akka.stream.scaladsl.Source

    def flatMapMergeCancellable[T, M](
        breadth: Int,
        f: Out => Graph[SourceShape[T], M],
    )(implicit
        ec: ExecutionContext,
        lc: LoggingContextOf[InstanceUUID],
    ): Flow[In, T, Mat] = {
      self
        .flatMapMerge(
          breadth,
          f andThen (gss => (Source fromGraph gss logTermination "fmm-inner")),
        )
        .logTermination("fmm-outer")
    }
  }
}
