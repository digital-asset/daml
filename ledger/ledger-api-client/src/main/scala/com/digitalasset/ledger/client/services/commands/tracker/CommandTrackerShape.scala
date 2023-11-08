// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands.tracker

import org.apache.pekko.stream.{Inlet, Outlet, Shape}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionFailure,
  CompletionSuccess,
}
import com.daml.ledger.client.services.commands.{CommandSubmission, CompletionStreamElement}
import com.daml.util.Ctx
import com.google.protobuf.empty.Empty

import scala.collection.immutable
import scala.util.Try

private[tracker] final case class CommandTrackerShape[Context](
    submitRequestIn: Inlet[Ctx[Context, CommandSubmission]],
    submitRequestOut: Outlet[Ctx[(Context, TrackedCommandKey), CommandSubmission]],
    commandResultIn: Inlet[
      Either[Ctx[(Context, TrackedCommandKey), Try[Empty]], CompletionStreamElement]
    ],
    resultOut: Outlet[Ctx[Context, Either[CompletionFailure, CompletionSuccess]]],
    offsetOut: Outlet[LedgerOffset],
) extends Shape {

  override def inlets: immutable.Seq[Inlet[_]] = Vector(submitRequestIn, commandResultIn)

  override def outlets: immutable.Seq[Outlet[_]] = Vector(submitRequestOut, resultOut, offsetOut)

  override def deepCopy(): Shape =
    CommandTrackerShape[Context](
      submitRequestIn.carbonCopy(),
      submitRequestOut.carbonCopy(),
      commandResultIn.carbonCopy(),
      resultOut.carbonCopy(),
      offsetOut.carbonCopy(),
    )
}
