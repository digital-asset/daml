// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.commands.tracker

import akka.stream.{Inlet, Outlet, Shape}
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.client.services.commands.CompletionStreamElement
import com.digitalasset.util.Ctx
import com.google.protobuf.empty.Empty

import scala.collection.immutable
import scala.util.Try

private[tracker] final case class CommandTrackerShape[Context](
    submitRequestIn: Inlet[Ctx[Context, SubmitRequest]],
    submitRequestOut: Outlet[Ctx[(Context, String), SubmitRequest]],
    commandResultIn: Inlet[Either[Ctx[(Context, String), Try[Empty]], CompletionStreamElement]],
    resultOut: Outlet[Ctx[Context, Completion]],
) extends Shape {

  override def inlets: immutable.Seq[Inlet[_]] = Vector(submitRequestIn, commandResultIn)

  override def outlets: immutable.Seq[Outlet[_]] = Vector(submitRequestOut, resultOut)

  override def deepCopy(): Shape =
    CommandTrackerShape[Context](
      submitRequestIn.carbonCopy(),
      submitRequestOut.carbonCopy(),
      commandResultIn.carbonCopy(),
      resultOut.carbonCopy(),
    )
}
