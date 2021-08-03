// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.refinements

import com.daml.ledger.api.refinements.ApiTypes._
import com.daml.ledger.api.v1.commands.Command.Command.Create
import com.daml.ledger.api.v1.commands.{Command, Commands, CreateCommand}
import com.daml.ledger.api.v1.value.Identifier
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CompositeCommandAdapterUT extends AnyWordSpec with Matchers {

  CompositeCommandAdapter.getClass.getSimpleName should {

    "translate CompositeCommand to SubmitRequest" in {
      val commands =
        Seq(
          Command(
            Create(CreateCommand(Some(Identifier("packageId", "moduleName", "templateId")), None))
          )
        )

      val compositeCommand = CompositeCommand(
        commands,
        Party("party"),
        CommandId("commandId"),
        WorkflowId("workflowId"),
      )

      val submitRequest = CompositeCommandAdapter(
        LedgerId("ledgerId"),
        ApplicationId("applicationId"),
      ).transform(compositeCommand)

      submitRequest.commands shouldBe Some(
        Commands("ledgerId", "workflowId", "applicationId", "commandId", "party", commands)
      )
    }

  }

}
