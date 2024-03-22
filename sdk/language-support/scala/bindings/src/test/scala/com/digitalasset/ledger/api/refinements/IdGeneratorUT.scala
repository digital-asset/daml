// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.refinements

import com.daml.ledger.api.refinements.ApiTypes.{CommandId, CommandIdTag, WorkflowId, WorkflowIdTag}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class IdGeneratorUT extends AnyWordSpec with Matchers {

  "IdGenerator" should {

    "generate WorkflowId based on seed" in {
      new IdGenerator[WorkflowIdTag](1).generateRandom shouldBe WorkflowId("bb1ad57319b89cd8")
    }

    "generate CommandId based on seed" in {
      new IdGenerator[CommandIdTag](1).generateRandom shouldBe CommandId("bb1ad57319b89cd8")
    }

  }

}
