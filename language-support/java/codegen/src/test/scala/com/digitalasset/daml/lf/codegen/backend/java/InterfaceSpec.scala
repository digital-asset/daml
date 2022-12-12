// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ut.retro.InterfaceRetro
import ut.retro.TemplateRetro

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOptional

final class InterfaceSpec extends AnyWordSpec with Matchers {

  "TemplateRetro.ContractId where `TemplateRetro` is implementing `InterfaceRetro` retroactively" should {
    "be able to convert to a interface id of `InterfaceRetro`" in {
      val contractId = new TemplateRetro.ContractId("SomeID")
      val contractViaInterface: InterfaceRetro.ContractId =
        contractId.toInterface(InterfaceRetro.INTERFACE)
      val update = contractViaInterface.exerciseTransfer("newOwner")
      val cmd = update.commands().get(0).asExerciseCommand().toScala.get
      cmd.getContractId shouldEqual contractId.contractId
      cmd.getTemplateId shouldEqual InterfaceRetro.TEMPLATE_ID
    }
  }

  "decoded contracts" should {
    import java.util.Collections.{emptyList, emptyMap}
    import java.util.Optional
    import com.daml.ledger.javaapi.data.CreatedEvent
    import com.daml.ledger.javaapi.data.codegen.{Contract, DamlRecord, InterfaceCompanion}
    import ut.retro.TokenView

    "roundtrip through CreatedEvent" in {
      def roundtrip[Id, View](
          data: DamlRecord[View],
          ic: InterfaceCompanion[_, Id, View],
      ): Contract[Id, View] =
        ic.fromCreatedEvent(
          new CreatedEvent(
            emptyList,
            "e",
            TemplateRetro.TEMPLATE_ID,
            "c",
            new TemplateRetro("", "", 0).toValue,
            Map(ic.TEMPLATE_ID -> data.toValue).asJava,
            emptyMap,
            Optional.empty,
            Optional.empty,
            emptyList,
            emptyList,
          )
        )
      val data = new TokenView("foobar", 12345)
      val contract = roundtrip(data, InterfaceRetro.INTERFACE)
      contract.data should ===(data)
      contract.getContractTypeId should ===(InterfaceRetro.TEMPLATE_ID)
    }
  }
}
