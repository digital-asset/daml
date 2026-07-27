// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  InterfaceFilter,
  WildcardFilter,
}
import com.daml.ledger.api.v2.value.Identifier
import com.digitalasset.canton.http.json.v2.JsUpdateServiceConverters.toUpdateFormat
import com.digitalasset.canton.http.json.v2.LegacyDTOs.TransactionFilter
import com.digitalasset.canton.tracing.NoTracing
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Inside, LoneElement}

class JsUpdateServiceTest
    extends AsyncWordSpec
    with Matchers
    with Inside
    with LoneElement
    with NoTracing {

  private val combinations = for {
    verbose <- Seq(true, false)
    byParty <- Seq(true, false)
    anyParty <- Seq(true, false)
  } yield (verbose, byParty, anyParty)

  private val interfaceFilter = CumulativeFilter(
    IdentifierFilter.InterfaceFilter(
      InterfaceFilter(
        interfaceId = Some(Identifier("package", "module", "name")),
        includeInterfaceView = true,
        includeCreatedEventBlob = true,
      )
    )
  )

  private val filters = Filters(Seq(interfaceFilter))

  "toUpdateFormat" should {
    "add a wildcard when filters do not contain a wildcard filter and is needed for trees" in {
      combinations.foreach { case (verbose, byParty, anyParty) =>
        val interfaceFilter = CumulativeFilter(
          IdentifierFilter.InterfaceFilter(
            InterfaceFilter(
              interfaceId = Some(Identifier("package", "module", "name")),
              includeInterfaceView = true,
              includeCreatedEventBlob = true,
            )
          )
        )

        val filters = Filters(Seq(interfaceFilter))

        val txFilter =
          TransactionFilter(
            filtersByParty =
              if (byParty)
                Map(
                  "party" -> filters
                )
              else Map.empty,
            filtersForAnyParty = if (anyParty) Some(filters) else None,
          )

        val updateFormat = toUpdateFormat(filter = txFilter, verbose = verbose, forTrees = true)

        updateFormat.includeTransactions.flatMap(_.eventFormat) should not be empty
        checkEventFormat(
          eventFormat = updateFormat.includeTransactions.flatMap(_.eventFormat).toList.loneElement,
          verbose = verbose,
          interfaceFilter = interfaceFilter,
        )

        updateFormat.includeReassignments should not be empty
        checkEventFormat(
          eventFormat = updateFormat.includeReassignments.toList.loneElement,
          verbose = verbose,
          interfaceFilter = interfaceFilter,
        )

        updateFormat.includeTopologyEvents shouldBe empty
      }
      succeed
    }

    "NOT add a wildcard when filters contain a wildcard filter" in {
      combinations.foreach { case (verbose, byParty, anyParty) =>
        val wf: CumulativeFilter = CumulativeFilter(
          IdentifierFilter.WildcardFilter(
            WildcardFilter(includeCreatedEventBlob = true)
          )
        )

        val txFilter =
          TransactionFilter(
            filtersByParty =
              if (byParty)
                Map(
                  "party" -> filters.addCumulative(wf)
                )
              else Map.empty,
            filtersForAnyParty = if (anyParty) Some(filters.addCumulative(wf)) else None,
          )

        val updateFormat = toUpdateFormat(filter = txFilter, verbose = verbose, forTrees = true)

        updateFormat.includeTransactions.flatMap(_.eventFormat) should not be empty
        checkEventFormat(
          eventFormat = updateFormat.includeTransactions.flatMap(_.eventFormat).toList.loneElement,
          verbose = verbose,
          interfaceFilter = interfaceFilter,
          includeCreatedEventBlob = true,
        )

        updateFormat.includeReassignments should not be empty
        checkEventFormat(
          eventFormat = updateFormat.includeReassignments.toList.loneElement,
          verbose = verbose,
          interfaceFilter = interfaceFilter,
          includeCreatedEventBlob = true,
        )

        updateFormat.includeTopologyEvents shouldBe empty
      }
      succeed
    }

    "NOT add a wildcard when filters are NOT needed for trees" in {
      combinations.foreach { case (verbose, byParty, anyParty) =>
        val txFilter =
          TransactionFilter(
            filtersByParty =
              if (byParty) Map("party" -> filters)
              else Map.empty,
            filtersForAnyParty = if (anyParty) Some(filters) else None,
          )

        val updateFormat = toUpdateFormat(filter = txFilter, verbose = verbose, forTrees = false)

        updateFormat.includeTransactions.flatMap(_.eventFormat) should not be empty
        val eventFormatTx =
          updateFormat.includeTransactions.flatMap(_.eventFormat).toList.loneElement
        eventFormatTx.verbose shouldBe verbose
        (eventFormatTx.filtersByParty.values ++ eventFormatTx.filtersForAnyParty).foreach {
          filters =>
            filters.cumulative shouldBe Seq(interfaceFilter)
        }

        updateFormat.includeReassignments should not be empty
        val eventFormatRe = updateFormat.includeReassignments.toList.loneElement
        (eventFormatRe.filtersByParty.values ++ eventFormatRe.filtersForAnyParty).foreach {
          filters =>
            filters.cumulative shouldBe Seq(interfaceFilter)
        }

        updateFormat.includeTopologyEvents shouldBe empty
      }
      succeed
    }
  }

  def checkEventFormat(
      eventFormat: EventFormat,
      verbose: Boolean,
      interfaceFilter: CumulativeFilter,
      includeCreatedEventBlob: Boolean = false,
  ): Unit = {
    eventFormat.verbose shouldBe verbose
    (eventFormat.filtersByParty.values ++ eventFormat.filtersForAnyParty).foreach { filters =>
      filters.cumulative should contain(
        CumulativeFilter(
          IdentifierFilter.WildcardFilter(
            WildcardFilter(includeCreatedEventBlob = includeCreatedEventBlob)
          )
        )
      )
      filters.cumulative should contain(interfaceFilter)
      // only a single wildcard filter should be present
      filters.cumulative.map(_.identifierFilter).collect {
        case w: IdentifierFilter.WildcardFilter => w
      } should have size 1
    }
  }

}
