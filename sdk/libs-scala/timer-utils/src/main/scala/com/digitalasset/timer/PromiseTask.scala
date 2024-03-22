// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timer

import java.util.TimerTask

import scala.concurrent.{Future, Promise}
import scala.util.Try
import scala.util.control.NonFatal

private class PromiseTask[A](value: => Future[A]) extends TimerTask with Promise[A] {

  private val p = Promise[A]()

  override def run(): Unit = {
    p.completeWith {
      try value
      catch { case NonFatal(t) => Future.failed(t) }
    }
  }

  override def future: Future[A] = p.future

  override def isCompleted: Boolean = p.isCompleted

  override def tryComplete(result: Try[A]): Boolean = p.tryComplete(result)

}
