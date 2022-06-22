// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.config.WorkflowConfig.StreamConfig.{
  PartyFilter,
  TransactionsStreamConfig,
}
import com.daml.ledger.api.benchtool.submission.AllocatedParties
import com.daml.ledger.client.binding.Primitive
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalaz.syntax.tag._

class ConfigEnricherSpec extends AnyFlatSpec with Matchers {

  it should "expand party-set filter into a sequence of party filters" in {
    val partySuffix = "-foo-123"

    def makePartyName(shortName: String): String = s"$shortName$partySuffix"
    def makeParty(shortName: String): Primitive.Party = Primitive.Party(makePartyName(shortName))

    val desugaring = new ConfigEnricher(
      allocatedParties = AllocatedParties(
        signatory = makeParty("Sig-0"),
        observers = List(makeParty("Obs-0")),
        divulgees = List(makeParty("Div-0")),
        extraSubmitters = List(makeParty("Sub-0")),
      )
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
          ),
          PartyFilter(
            party = "Sig-0",
            templates = templates,
          ),
        ),
      )
    ) shouldBe TransactionsStreamConfig(
      name = "flat",
      filters = List(
        PartyFilter(
          party = "Obs-0-foo-123",
          templates = enrichedTemplates,
        ),
        PartyFilter(
          party = "Sig-0-foo-123",
          templates = enrichedTemplates,
        ),
      ),
    )
  }
}
