// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil.future

import java.util.concurrent.{CompletionException, CompletionStage}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.javaapi.FutureConverters

object FutureConversion {

  /** Converts a java [[CompletionStage]] into a scala [[Future]].
    * Java [[CompletionStage]] wraps any exception that causes failure into a [[CompletionException]].
    * Compared to the standard library conversions found in [[scala.jdk.FutureConverters]]
    * this conversion unwraps any failure and fails the future with the root cause found in the [[CompletionException]].
    */
  def toScalaUnwrapped[T](cs: CompletionStage[T]): Future[T] = {
    FutureConverters
      .asScala(cs)
      .recover {
        case ex: CompletionException if ex.getCause != null =>
          throw ex.getCause
      }(ExecutionContext.parasitic)
  }

  implicit class CompletionStageConversionOps[T](private val cs: CompletionStage[T])
      extends AnyVal {
    def toScalaUnwrapped: Future[T] = FutureConversion.toScalaUnwrapped(cs)
  }

}
