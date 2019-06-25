// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

import scalaz.{Show, \/}
import scalaz.syntax.show._

import scala.concurrent.Future

object FutureUtil {
  def toFuture[A](o: Option[A]): Future[A] =
    o.fold(Future.failed[A](new IllegalStateException(s"Empty option: $o")))(a =>
      Future.successful(a))

  def toFuture[A: Show, B](a: A \/ B): Future[B] =
    a.fold(e => Future.failed(new IllegalStateException(e.shows)), a => Future.successful(a))
}
