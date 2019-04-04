// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.refinements

import java.time.{Duration, Instant}

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.ledger.api.refinements.ApiTypes._
import com.digitalasset.ledger.api.v1.commands.Command.Command.Create
import com.digitalasset.ledger.api.v1.commands.{Command, Commands, CreateCommand}
import com.digitalasset.ledger.api.v1.trace_context.TraceContext
import com.digitalasset.ledger.api.v1.value.Identifier
import com.google.protobuf.timestamp.Timestamp
import org.scalatest.{Matchers, WordSpec}

class CompositeCommandAdapterUT extends WordSpec with Matchers {

  CompositeCommandAdapter.getClass.getSimpleName should {

    "translate CompositeCommand to SubmitRequest" in {
      val commands =
        Seq(Command(Create(CreateCommand(Some(Identifier("packageId", "templateId")), None))))

      val submittedTraceContext = Some(TraceContext(1, 2, 3, Some(4L), true))
      val compositeCommand = CompositeCommand(
        commands,
        Party("party"),
        CommandId("commandId"),
        WorkflowId("workflowId"),
        submittedTraceContext
      )

      val timeProvider = TimeProvider.Constant(Instant.ofEpochSecond(60))

      val submitRequest = CompositeCommandAdapter(
        LedgerId("ledgerId"),
        ApplicationId("applicationId"),
        Duration.ofMinutes(1),
        timeProvider
      ).transform(compositeCommand)

      submitRequest.commands shouldBe Some(
        Commands(
          "ledgerId",
          "workflowId",
          "applicationId",
          "commandId",
          "party",
          Some(Timestamp(60, 0)),
          Some(Timestamp(120, 0)),
          commands))

      submitRequest.traceContext shouldBe submittedTraceContext
    }

  }

}
