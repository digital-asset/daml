// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.config.WorkflowConfig.StreamConfig.{
  PartyFilter,
  PartyNamePrefixFilter,
  TransactionsStreamConfig,
}
import com.daml.ledger.api.benchtool.submission.{
  AllocatedParties,
  AllocatedPartySet,
  BenchtoolTestsPackageInfo,
}
import com.daml.ledger.client.binding.Primitive
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalaz.syntax.tag._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class ConfigEnricherSpec extends AnyFlatSpec with Matchers {

  it should "expand party-set filter into a sequence of party filters" in {
    def makePartyName(shortName: String): String = s"$shortName"
    def makeParty(shortName: String): Primitive.Party = Primitive.Party(makePartyName(shortName))

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
      BenchtoolTestsPackageInfo.StaticDefault,
    )
    val templates: List[String] = List("otherTemplate", "Foo1")
    val foo1Id = com.daml.ledger.test.benchtool.Foo.Foo1.id.unwrap
    val enrichedTemplates: List[String] =
      List("otherTemplate", s"${foo1Id.packageId}:${foo1Id.moduleName}:${foo1Id.entityName}")

    desugaring.enrichStreamConfig(
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
        partyNamePrefixFiltersO = Some(
          List(
            PartyNamePrefixFilter(
              partyNamePrefix = "MyParty-1",
              templates = templates,
            ),
            PartyNamePrefixFilter(
              partyNamePrefix = "MyParty-2",
              templates = templates,
            ),
          )
        ),
        subscriptionDelay = Some(Duration(1337, TimeUnit.SECONDS)),
      )
    ) shouldBe TransactionsStreamConfig(
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
      ),
      partyNamePrefixFiltersO = None,
      subscriptionDelay = Some(Duration(1337, TimeUnit.SECONDS)),
    )
  }
}
