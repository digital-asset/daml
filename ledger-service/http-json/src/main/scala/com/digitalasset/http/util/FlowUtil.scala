// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Flow, FlowOpsMat}
import Logging.InstanceUUID
import com.daml.logging.{LoggingContextOf, ContextualizedLogger}
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

  import language.implicitConversions

  private[http] implicit def `flowops logTermination`[O, M](
      self: FlowOpsMat[O, M]
  ): `flowops logTermination`[O, M, self.ReprMat] =
    new `flowops logTermination`[O, M, self.ReprMat](self)

  private[http] final class `flowops logTermination`[O, M, RM[+_, +_]](
      private val self: FlowOpsMatAux[O, M, RM]
  ) extends AnyVal {
    def logTermination(
        extraMessage: String
    )(implicit ec: ExecutionContext, lc: LoggingContextOf[Any]): RM[O, M] =
      self.watchTermination() { (mat, fd) =>
        fd.onComplete(
          _.fold(
            { t =>
              logger.info(s"S11 $extraMessage trying to abort ${t.getMessage}")
            },
            { _ =>
              logger.info(s"S11 $extraMessage trying to shutdown")
            },
          )
        )
        mat
      }
  }

  private[http] implicit final class `Flow flatMapMerge`[In, Out, Mat](
      private val self: Flow[In, Out, Mat]
  ) extends AnyVal {
    import akka.stream.{Graph, SourceShape, KillSwitches}
    import akka.stream.scaladsl.Source

    def flatMapMergeCancellable[T, M](
        breadth: Int,
        f: Out => Graph[SourceShape[T], M],
    )(implicit
        ec: ExecutionContext,
        mat: Materializer,
        lc: LoggingContextOf[InstanceUUID],
    ): Flow[In, T, Mat] = {
      val (ks, inner) = Source
        .never[T]
        .viaMat(Flow fromGraph KillSwitches.single[T])(Keep.right)
        .preMaterialize()
      self
        .flatMapMerge(
          breadth,
          f andThen (gss =>
            (Source fromGraph gss logTermination "fmm-inner").merge(inner, eagerComplete = true)
          ),
        )
        .watchTermination() { (mat, fd) =>
          fd.onComplete(
            _.fold(
              { t =>
                logger.info(s"S11 trying to abort ${t.getMessage}")
                ks.abort(t)
              },
              { _ =>
                logger.info(s"S11 trying to shutdown")
                ks.shutdown()
              },
            )
          )
          mat
        }
    }
  }

  private[http] def showTryDone(t: scala.util.Try[akka.Done]) =
    t.fold(_.getMessage, _ => "Done")

  private val logger = ContextualizedLogger.get(getClass)
}
