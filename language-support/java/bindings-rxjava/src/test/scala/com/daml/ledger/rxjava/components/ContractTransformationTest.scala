// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.components

import com.daml.ledger.javaapi.data.{Identifier, Party, DamlRecord, Text}
import com.daml.ledger.rxjava.components.helpers.{
  CreatedContract,
  CreatedContractContext,
  TemplateUtils,
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import templateutils.{TemplateA, TemplateB, TemplateC}

class TestContext extends CreatedContractContext {
  @Override
  def getWorkflowId(): String = ???
}

class ContractTransformationTest extends AnyFlatSpec with Matchers {

  private def createCreatedContract(identifier: Identifier, argument: String): CreatedContract = {
    val arguments = new DamlRecord(
      new DamlRecord.Field("argument", new Text(argument)),
      new DamlRecord.Field("owner", new Party("party")),
    )
    new CreatedContract(identifier, arguments, new TestContext())
  }

  val transformer = TemplateUtils.contractTransformer(classOf[TemplateA], classOf[TemplateB])

  it should "transform allowed template A" in {
    val argumentA = "argument for A"
    val createdContractA = createCreatedContract(TemplateA.TEMPLATE_ID, argumentA)
    transformer.apply(createdContractA) match {
      case tA: TemplateA =>
        tA.argument shouldBe argumentA
    }
  }

  it should "transform allowed template B" in {
    val argumentB = "argument for B"
    val createdContractB = createCreatedContract(TemplateB.TEMPLATE_ID, argumentB)
    transformer.apply(createdContractB) match {
      case tB: TemplateB =>
        tB.argument shouldBe argumentB
    }
  }

  it should "throw when transforming disallowed template C" in {
    val argumentC = "argument for C"
    val createdContractC = createCreatedContract(TemplateC.TEMPLATE_ID, argumentC)
    intercept[java.lang.IllegalStateException] {
      transformer.apply(createdContractC)
    }
  }

}
