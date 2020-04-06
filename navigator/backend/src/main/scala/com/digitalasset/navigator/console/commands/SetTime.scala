// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import com.daml.navigator.console._
import com.daml.navigator.store.Store.AdvanceTime
import com.daml.navigator.time.TimeProviderWithType
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Try

case object SetTime extends SimpleCommand {
  def name: String = "set_time"

  def description: String = "Set the (static) ledger effective time"

  def params: List[Parameter] = List(ParameterString("time", "New (static) ledger effective time"))

  private def advanceTime(state: State, newTime: Instant): Future[TimeProviderWithType] = {
    implicit val actorTimeout: Timeout = Timeout(20, TimeUnit.SECONDS)
    implicit val executionContext: ExecutionContext = state.ec

    (state.store ? AdvanceTime(newTime))
      .mapTo[Try[TimeProviderWithType]]
      .map(t => t.get)
  }

  private def formatTime(t: Instant): String = DateTimeFormatter.ISO_INSTANT.format(t)

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    for {
      arg1 <- args.headOption ~> "Missing <time> argument"
      newTime <- Try(Instant.parse(arg1)) ~> "Failed to parse time"
      future <- Try(advanceTime(state, newTime)) ~> "Failed to advance time"
      confirmedTime <- Try(Await.result(future, 30.seconds)) ~> "Failed to advance time"
      result <- Try(formatTime(confirmedTime.time.getCurrentTime)) ~> "Failed to format time"
    } yield {
      (state, s"New ledger effective time: $result")
    }
  }

}
