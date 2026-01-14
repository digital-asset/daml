// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool

import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig.StreamConfig.{
  PartyFilter,
  PartyNamePrefixFilter,
  TransactionsStreamConfig,
}
import com.digitalasset.canton.ledger.api.benchtool.submission.{
  AllocatedParties,
  AllocatedPartySet,
  BenchtoolTestsPackageInfo,
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class ConfigEnricherSpec extends AnyFlatSpec with Matchers {

  it should "expand party-set filter into a sequence of party filters" in {
    def makePartyName(shortName: String): String = s"$shortName"
    def makeParty(shortName: String): Party = new Party(makePartyName(shortName))

    val desugaring = new ConfigEnricher(
      allocatedParties = AllocatedParties(
        signatoryO = Some(makeParty("Sig-0")),
        observers = List(makeParty("Obs-0")),
        divulgees = List(makeParty("Div-0")),
        extraSubmitters = List(makeParty("Sub-0")),
        observerPartySets = List(
          AllocatedPartySet(
            mainPartyNamePrefix = "MyParty",
            List("MyParty-0", "MyParty-1", "MyParty-11", "MyParty-12", "MyParty-21", "MyParty-22")
              .map(makeParty),
          )
        ),
      ),
      packageInfo = BenchtoolTestsPackageInfo.StaticDefault,
    )
    val templates: List[String] = List("otherTemplate", "Foo1")
    val enrichedTemplates: List[String] =
      List(
        "otherTemplate",
        s"${BenchtoolTestsPackageInfo.StaticDefault.packageRef}:Foo:Foo1",
      )

    val actual = desugaring.enrichStreamConfig(
      TransactionsStreamConfig(
        name = "flat",
        filters = List(
          PartyFilter(
            party = "Obs-0",
            templates = templates,
            interfaces = List.empty,
          ),
          PartyFilter(
            party = "Sig-0",
            templates = templates,
            interfaces = List.empty,
          ),
          PartyFilter(
            party = "UnknownParty-0",
            templates = templates,
            interfaces = List.empty,
          ),
        ),
        partyNamePrefixFilters = List(
          PartyNamePrefixFilter(
            partyNamePrefix = "MyParty-1",
            templates = templates,
          ),
          PartyNamePrefixFilter(
            partyNamePrefix = "MyParty-2",
            templates = templates,
          ),
          PartyNamePrefixFilter(
            partyNamePrefix = "Obs",
            templates = templates,
          ),
        ),
        subscriptionDelay = Some(Duration(1337, TimeUnit.SECONDS)),
      )
    )
    actual shouldBe TransactionsStreamConfig(
      name = "flat",
      filters = List(
        PartyFilter(
          party = "Obs-0",
          templates = enrichedTemplates,
          interfaces = List.empty,
        ),
        PartyFilter(
          party = "Sig-0",
          templates = enrichedTemplates,
          interfaces = List.empty,
        ),
        PartyFilter(
          party = "UnknownParty-0",
          templates = enrichedTemplates,
          interfaces = List.empty,
        ),
        PartyFilter(
          party = "MyParty-1",
          templates = enrichedTemplates,
          interfaces = List.empty,
        ),
        PartyFilter(
          party = "MyParty-11",
          templates = enrichedTemplates,
          interfaces = List.empty,
        ),
        PartyFilter(
          party = "MyParty-12",
          templates = enrichedTemplates,
          interfaces = List.empty,
        ),
        PartyFilter(
          party = "MyParty-21",
          templates = enrichedTemplates,
          interfaces = List.empty,
        ),
        PartyFilter(
          party = "MyParty-22",
          templates = enrichedTemplates,
          interfaces = List.empty,
        ),
        PartyFilter(
          party = "Obs-0",
          templates = enrichedTemplates,
          interfaces = List.empty,
        ),
      ),
      subscriptionDelay = Some(Duration(1337, TimeUnit.SECONDS)),
    )
  }
}
