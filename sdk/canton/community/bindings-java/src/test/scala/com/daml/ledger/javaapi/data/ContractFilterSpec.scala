// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.codegen.json.TestHelpers
import com.daml.ledger.javaapi.data.codegen.json.TestHelpers.Tmpl.{IfaceCompanion, TmplCompanion}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.Collections
import scala.jdk.CollectionConverters.{MapHasAsJava, SetHasAsJava}
import scala.util.chaining.scalaUtilChainingOps

class ContractFilterSpec extends AnyFlatSpec with Matchers {
  private val partiesSet = Set("Alice", "Bob").asJava

  behavior of classOf[ContractFilter[_]].getSimpleName

  it should "correctly allow constructing a transaction filter for templates" in {
    def assertCreatedEventBlob(contractFilter: ContractFilter[_], expectIncluded: Boolean) = {
      val expectedInclusiveFilter = new InclusiveFilter(
        Collections.emptyMap[Identifier, Filter.Interface](),
        Collections
          .singletonMap(TestHelpers.Tmpl.templateId, if(expectIncluded) Filter.Template.INCLUDE_CREATED_EVENT_BLOB else Filter.Template.HIDE_CREATED_EVENT_BLOB),
      )

      val expectedTransactionFilter = new FiltersByParty(
        Map[String, Filter](
          "Alice" -> expectedInclusiveFilter,
          "Bob" -> expectedInclusiveFilter,
        ).asJava
      )

      contractFilter.transactionFilter(partiesSet) shouldBe expectedTransactionFilter
    }

    ContractFilter
      .of(new TmplCompanion)
      // Assert default behavior of transactionFilter
      .tap(assertCreatedEventBlob(_, expectIncluded = false))
      // Now enable created event blob
      .pipe(_.withIncludeCreatedEventBlob(true))
      .tap(assertCreatedEventBlob(_, expectIncluded = true))
      // Now disable created event blob
      .pipe(_.withIncludeCreatedEventBlob(false))
      .tap(assertCreatedEventBlob(_, expectIncluded = false))
  }

  it should "correctly allow constructing a transaction filter for interfaces" in {
    def assertCreatedEventBlob(contractFilter: ContractFilter[_], expectIncluded: Boolean) = {
      val expectedInclusiveFilter = new InclusiveFilter(
        Collections
          .singletonMap(
            TestHelpers.Tmpl.interfaceId,
            if(expectIncluded) Filter.Interface.INCLUDE_VIEW_INCLUDE_CREATED_EVENT_BLOB else Filter.Interface.INCLUDE_VIEW_HIDE_CREATED_EVENT_BLOB,
          ),
        Collections.emptyMap[Identifier, Filter.Template](),
      )

      val expectedTransactionFilter = new FiltersByParty(
        Map[String, Filter](
          "Alice" -> expectedInclusiveFilter,
          "Bob" -> expectedInclusiveFilter,
        ).asJava
      )

      contractFilter.transactionFilter(partiesSet) shouldBe expectedTransactionFilter
    }

    ContractFilter
      .of(new IfaceCompanion)
      // Assert default behavior of transactionFilter
      .tap(assertCreatedEventBlob(_, expectIncluded = false))
      // Now enable created event blob
      .pipe(_.withIncludeCreatedEventBlob(true))
      .tap(assertCreatedEventBlob(_, expectIncluded = true))
      // Now disable created event blob
      .pipe(_.withIncludeCreatedEventBlob(false))
      .tap(assertCreatedEventBlob(_, expectIncluded = false))
  }

}
