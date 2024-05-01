// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.scenario

import java.util.{Timer, TimerTask}
import scala.concurrent.duration._

private[scenario] class TimeBomb(delayInMillis: Long) {
  @volatile
  private[this] var exploded: Boolean = false

  def start(): () => Boolean = {
    val task = new TimerTask { override def run(): Unit = exploded = true }
    TimeBomb.timer.schedule(task, delayInMillis)
    hasExploded
  }

  val hasExploded: () => Boolean = () => exploded
}

private[scenario] object TimeBomb {
  private val timer = new Timer(true)
  def apply(delayInMillis: Long): TimeBomb = new TimeBomb(delayInMillis)
  def apply(duration: Duration): TimeBomb = apply(duration.toMillis)
}
