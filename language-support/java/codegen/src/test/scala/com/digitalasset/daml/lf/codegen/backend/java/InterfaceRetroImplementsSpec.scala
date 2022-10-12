// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java

import com.daml.ledger.javaapi.data.ExerciseCommand
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ut.retro.InterfaceRetro
import ut.retro.TemplateRetro

final class InterfaceRetroImplementsSpec extends AnyWordSpec with Matchers {

  "TemplateRetro.ContractId where `TemplateRetro` is implementing `InterfaceRetro` retroactively" should {
    "be able to convert to a interface id of `InterfaceRetro`" in {
      val contractId = new TemplateRetro.ContractId("SomeID")
      val contractViaInterface: InterfaceRetro.ContractId =
        contractId.toInterface(InterfaceRetro.INTERFACE)
      val update = contractViaInterface.exerciseTransfer("newOwner")
      val cmd = update.command().asInstanceOf[ExerciseCommand]
      cmd.getContractId shouldEqual contractId.contractId
      cmd.getTemplateId shouldEqual InterfaceRetro.TEMPLATE_ID
    }
  }
}
