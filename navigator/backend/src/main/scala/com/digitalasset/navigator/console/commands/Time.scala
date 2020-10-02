// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import com.daml.navigator.console._
import com.daml.navigator.store.Store.ReportCurrentTime
import com.daml.navigator.time.TimeProviderWithType
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Try

case object Time extends SimpleCommand {
  def name: String = "time"

  def description: String = "Print the ledger effective time"

  def params: List[Parameter] = List.empty

  def getTime(state: State): Future[TimeProviderWithType] = {
    implicit val actorTimeout: Timeout = Timeout(20, TimeUnit.SECONDS)
    implicit val executionContext: ExecutionContext = state.ec

    (state.store ? ReportCurrentTime)
      .mapTo[Try[TimeProviderWithType]]
      .map(t => t.get)
  }

  def formatTime(t: Instant): String = DateTimeFormatter.ISO_INSTANT.format(t)

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    for {
      future <- Try(getTime(state)) ~> "Failed to get time"
      time <- Try(Await.result(future, 30.seconds)) ~> "Failed to get time"
      result <- Try(formatTime(time.time.getCurrentTime)) ~> "Failed to format time"
    } yield {
      (state, result)
    }
  }

}
