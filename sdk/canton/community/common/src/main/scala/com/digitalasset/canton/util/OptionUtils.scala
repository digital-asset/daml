// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object OptionUtils {
  implicit class OptionExtension[A](val in: Option[A]) extends AnyVal {
    def toFuture(e: => Throwable): Future[A] = in match {
      case Some(v) => Future.successful(v)
      case None => Future.failed(e)
    }
    def toTry(e: => Throwable): Try[A] = in match {
      case Some(value) => Success(value)
      case None => Failure(e)
    }
  }
}
