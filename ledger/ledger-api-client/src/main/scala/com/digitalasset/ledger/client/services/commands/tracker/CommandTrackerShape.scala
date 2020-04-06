// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands.tracker

import akka.stream.{Inlet, Outlet, Shape}
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.services.commands.CompletionStreamElement
import com.daml.util.Ctx
import com.google.protobuf.empty.Empty

import scala.collection.immutable
import scala.util.Try

private[tracker] final case class CommandTrackerShape[Context](
    submitRequestIn: Inlet[Ctx[Context, SubmitRequest]],
    submitRequestOut: Outlet[Ctx[(Context, String), SubmitRequest]],
    commandResultIn: Inlet[Either[Ctx[(Context, String), Try[Empty]], CompletionStreamElement]],
    resultOut: Outlet[Ctx[Context, Completion]],
    offsetOut: Outlet[LedgerOffset])
    extends Shape {

  override def inlets: immutable.Seq[Inlet[_]] = Vector(submitRequestIn, commandResultIn)

  override def outlets: immutable.Seq[Outlet[_]] = Vector(submitRequestOut, resultOut, offsetOut)

  override def deepCopy(): Shape =
    CommandTrackerShape[Context](
      submitRequestIn.carbonCopy(),
      submitRequestOut.carbonCopy(),
      commandResultIn.carbonCopy(),
      resultOut.carbonCopy(),
      offsetOut.carbonCopy())
}
