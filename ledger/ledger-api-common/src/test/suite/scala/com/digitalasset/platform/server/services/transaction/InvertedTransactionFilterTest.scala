// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.transaction

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.ledger.api.DomainMocks._
import com.digitalasset.ledger.api.domain._
import org.scalatest.{Matchers, OptionValues, WordSpec}
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class InvertedTransactionFilterTest extends WordSpec with Matchers with OptionValues {

  private def invert(
      transactionFilter: TransactionFilter): InvertedTransactionFilter[Ref.Identifier, Party] =
    InvertedTransactionFilter.extractFrom(transactionFilter)

  private def invert(map: Map[Party, Filters]): InvertedTransactionFilter[Ref.Identifier, Party] =
    invert(TransactionFilter(map))

  private val filter: Map[Party, Filters] => TransactionFilter = TransactionFilter.apply

  private val party2 = Party.assertFromString("party2")
  private val identifier2 =
    identifier.copy(
      qualifiedName =
        identifier.qualifiedName.copy(name = Ref.DottedName.assertFromString("entity2")))
  private val filterForIdentifier = Filters(InclusiveFilters(Set(identifier)))
  private val filterForIdentifier2 = Filters(InclusiveFilters(Set(identifier2)))

  "Inverting TransactionFilters" should {

    "return empty maps for empty filters" in {
      val in = invert(Map.empty[Party, Filters])
      in.specificSubscriptions shouldBe empty
      in.globalSubscribers shouldBe empty
    }
    "return empty maps for filters with a single empty FiltersInclusive" in {
      val in = invert(Map(party -> Filters(InclusiveFilters(Set.empty))))
      in.specificSubscriptions shouldBe empty
      in.globalSubscribers shouldBe empty
    }

    "return the correct value for a single specific subscription" in {
      val in = invert(Map(party -> filterForIdentifier))
      in.specificSubscriptions shouldEqual Map(identifier -> Set(party))
      in.globalSubscribers shouldBe empty
    }

    "return the correct value for two specific subscriptions for the same template ID" in {
      val in = invert(Map(party -> filterForIdentifier, party2 -> filterForIdentifier))
      in.specificSubscriptions shouldEqual Map(identifier -> Set(party, party2))
      in.globalSubscribers shouldBe empty
    }

    "return the correct value for two specific subscriptions for different template IDs" in {
      val in = invert(Map(party -> filterForIdentifier, party2 -> filterForIdentifier2))
      in.specificSubscriptions shouldEqual Map(identifier -> Set(party), identifier2 -> Set(party2))
      in.globalSubscribers shouldBe empty
    }

    "return the correct value for a single specific subscription and another global" in {
      val in = invert(Map(party -> filterForIdentifier, party2 -> Filters.noFilter))
      in.specificSubscriptions shouldEqual Map(identifier -> Set(party))
      in.globalSubscribers shouldEqual Set(party2)
    }

    "return the correct value for two global subscriptions" in {
      val in = invert(Map(party -> Filters.noFilter, party2 -> Filters.noFilter))
      in.specificSubscriptions shouldBe empty
      in.globalSubscribers shouldEqual Set(party, party2)
    }
  }
}
