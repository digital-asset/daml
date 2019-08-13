// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.refinements

import com.digitalasset.ledger.api.refinements.ApiTypes.{
  CommandId,
  CommandIdTag,
  WorkflowId,
  WorkflowIdTag
}
import org.scalatest.{Matchers, WordSpec}

class IdGeneratorUT extends WordSpec with Matchers {

  "IdGenerator" should {

    "generate WorkflowId based on seed" in {
      new IdGenerator[WorkflowIdTag](1).generateRandom shouldBe WorkflowId("bb1ad57319b89cd8")
    }

    "generate CommandId based on seed" in {
      new IdGenerator[CommandIdTag](1).generateRandom shouldBe CommandId("bb1ad57319b89cd8")
    }

  }

}
