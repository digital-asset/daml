// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  Participant,
  Participants,
  SingleParty,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v2.value.Value
import com.daml.ledger.javaapi.data.codegen.PrimitiveValueDecoders
import com.daml.ledger.test.java.semantic.damlvalues.WithTextMap

import scala.jdk.CollectionConverters.{MapHasAsJava, MapHasAsScala}

final class DamlValuesIT extends LedgerTestSuite {
  test(
    "DVTextMap",
    "Ledger API should support text maps in Daml values",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice))) =>
    for {
      contract: WithTextMap.ContractId <- ledger
        .create(alice, new WithTextMap(alice, Map("foo" -> "bar").asJava))(
          WithTextMap.COMPANION
        )
      resultTxTree <- ledger
        .exercise(alice, contract.exerciseWithTextMap_Expand(Map("marco" -> "polo").asJava))
    } yield {
      val result = resultTxTree.events.head.getExercised.exerciseResult
        .getOrElse(fail("Expected return value"))

      val actualItems = PrimitiveValueDecoders
        .fromTextMap(PrimitiveValueDecoders.fromText)
        .decode(com.daml.ledger.javaapi.data.Value.fromProto(Value.toJavaProto(result)))
        .asScala

      val expectedItems = Map("foo" -> "bar", "marco" -> "polo")
      assertSameElements(actualItems, expectedItems, "items returned by WithTextMap_Expand")
    }
  })
}
