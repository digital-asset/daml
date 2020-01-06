// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.quickstart.iou
import scala.concurrent.Future

object FutureUtil {
  def toFuture[A](o: Option[A]): Future[A] =
    o.fold(Future.failed[A](new IllegalStateException(s"Empty option: $o")))(a =>
      Future.successful(a))
}
