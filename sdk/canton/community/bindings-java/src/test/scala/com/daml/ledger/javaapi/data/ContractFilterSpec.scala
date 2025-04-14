// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.javaapi.data.codegen.json.TestHelpers
import com.daml.ledger.javaapi.data.codegen.json.TestHelpers.Tmpl.{IfaceCompanion, TmplCompanion}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.{Collections, Optional}
import scala.jdk.CollectionConverters.{MapHasAsJava, SetHasAsJava}
import scala.util.chaining.scalaUtilChainingOps

class ContractFilterSpec extends AnyFlatSpec with Matchers {
  private val partiesSet = Set("Alice", "Bob").asJava

  behavior of classOf[ContractFilter[_]].getSimpleName

  private def templateCumulativeFilter(expectedIncluded: Boolean) = new CumulativeFilter(
    Collections.emptyMap[Identifier, Filter.Interface](),
    Collections
      .singletonMap(
        TestHelpers.Tmpl.templateId,
        if (expectedIncluded) Filter.Template.INCLUDE_CREATED_EVENT_BLOB
        else Filter.Template.HIDE_CREATED_EVENT_BLOB,
      ),
    Optional.empty(),
  )

  private def interfaceCumulativeFilter(expectedIncluded: Boolean) = new CumulativeFilter(
    Collections
      .singletonMap(
        TestHelpers.Tmpl.interfaceId,
        if (expectedIncluded) Filter.Interface.INCLUDE_VIEW_INCLUDE_CREATED_EVENT_BLOB
        else Filter.Interface.INCLUDE_VIEW_HIDE_CREATED_EVENT_BLOB,
      ),
    Collections.emptyMap[Identifier, Filter.Template](),
    Optional.empty(),
  )

  private def assertFilters(
      contractFilter: ContractFilter[_],
      expectedIncluded: Boolean,
      expectedVerbose: Boolean,
      expectedShape: TransactionShape,
      cumulativeFilter: Boolean => CumulativeFilter,
  ) = {
    val expectedCumulativeFilter = cumulativeFilter(expectedIncluded)

    val expectedPartyToFilters = Map[String, Filter](
      "Alice" -> expectedCumulativeFilter,
      "Bob" -> expectedCumulativeFilter,
    ).asJava

    val expectedEventFormatWithParties = new EventFormat(
      expectedPartyToFilters,
      Optional.empty(),
      expectedVerbose,
    )

    val expectedWildcardEventFormat = new EventFormat(
      Map.empty[String, Filter].asJava,
      Optional.of(expectedCumulativeFilter),
      expectedVerbose,
    )

    val expectedTransactionFormatWithParties = new TransactionFormat(
      expectedEventFormatWithParties,
      expectedShape,
    )

    val expectedWildcardTransactionFormat = new TransactionFormat(
      expectedWildcardEventFormat,
      expectedShape,
    )

    contractFilter.transactionFilter(Optional.of(partiesSet)) shouldBe new TransactionFilter(
      expectedPartyToFilters,
      Optional.empty(),
    )

    contractFilter.transactionFilter(Optional.empty()) shouldBe new TransactionFilter(
      Collections.emptyMap(),
      Optional.of(expectedCumulativeFilter),
    )

    contractFilter.eventFormat(Optional.of(partiesSet)) shouldBe expectedEventFormatWithParties
    contractFilter.eventFormat(Optional.empty()) shouldBe expectedWildcardEventFormat

    contractFilter.transactionFormat(
      Optional.of(partiesSet)
    ) shouldBe expectedTransactionFormatWithParties
    contractFilter.transactionFormat(Optional.empty()) shouldBe expectedWildcardTransactionFormat

    contractFilter.updateFormat(Optional.of(partiesSet)) shouldBe new UpdateFormat(
      Optional.of(expectedTransactionFormatWithParties),
      Optional.of(expectedEventFormatWithParties),
      Optional.empty(),
    )

    contractFilter.updateFormat(Optional.empty()) shouldBe new UpdateFormat(
      Optional.of(expectedWildcardTransactionFormat),
      Optional.of(expectedWildcardEventFormat),
      Optional.empty(),
    )
  }

  it should "correctly allow constructing a transaction filter for templates" in {
    ContractFilter
      .of(new TmplCompanion)
      // Assert default behavior of transactionFilter
      .tap(
        assertFilters(
          _,
          expectedIncluded = false,
          expectedVerbose = false,
          expectedShape = TransactionShape.ACS_DELTA,
          templateCumulativeFilter,
        )
      )
      // Now enable created event blob
      .withIncludeCreatedEventBlob(true)
      .withVerbose(true)
      .withTransactionShape(TransactionShape.LEDGER_EFFECTS)
      .tap(
        assertFilters(
          _,
          expectedIncluded = true,
          expectedVerbose = true,
          expectedShape = TransactionShape.LEDGER_EFFECTS,
          templateCumulativeFilter,
        )
      )
      // Now disable created event blob
      .withIncludeCreatedEventBlob(false)
      .withVerbose(false)
      .withTransactionShape(TransactionShape.ACS_DELTA)
      .tap(
        assertFilters(
          _,
          expectedIncluded = false,
          expectedVerbose = false,
          expectedShape = TransactionShape.ACS_DELTA,
          templateCumulativeFilter,
        )
      )
  }

  it should "correctly allow constructing a transaction filter for interfaces" in {
    ContractFilter
      .of(new IfaceCompanion)
      // Assert default behavior of transactionFilter
      .tap(
        assertFilters(
          _,
          expectedIncluded = false,
          expectedVerbose = false,
          expectedShape = TransactionShape.ACS_DELTA,
          interfaceCumulativeFilter,
        )
      )
      // Now enable created event blob
      .withIncludeCreatedEventBlob(true)
      .withVerbose(true)
      .withTransactionShape(TransactionShape.LEDGER_EFFECTS)
      .tap(
        assertFilters(
          _,
          expectedIncluded = true,
          expectedVerbose = true,
          expectedShape = TransactionShape.LEDGER_EFFECTS,
          interfaceCumulativeFilter,
        )
      )
      // Now disable created event blob
      .withIncludeCreatedEventBlob(false)
      .withVerbose(false)
      .withTransactionShape(TransactionShape.ACS_DELTA)
      .tap(
        assertFilters(
          _,
          expectedIncluded = false,
          expectedVerbose = false,
          expectedShape = TransactionShape.ACS_DELTA,
          interfaceCumulativeFilter,
        )
      )
  }

}
