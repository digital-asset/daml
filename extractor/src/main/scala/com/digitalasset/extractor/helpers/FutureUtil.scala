// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.helpers

import scalaz.{NonEmptyList, Show, ValidationNel, \/}
import scalaz.syntax.show._
import scala.concurrent.Future

object FutureUtil {
  def toFuture[E: Show, A](a: E \/ A): Future[A] = {
    a.fold(
      e => Future.failed(new RuntimeException(e.shows)),
      a => Future.successful(a)
    )
  }

  def toFuture[E: Show, A](a: ValidationNel[E, A]): Future[A] =
    a.fold(
      es => Future.failed(new RuntimeException(formatErrors(es))),
      a => Future.successful(a)
    )

  private def formatErrors[E: Show](es: NonEmptyList[E]): String = {
    es.map(e => e.shows).list.toList.mkString(", ")
  }
}
