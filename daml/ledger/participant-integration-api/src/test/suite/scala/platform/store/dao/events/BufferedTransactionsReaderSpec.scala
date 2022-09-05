// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.lf.data.Ref.{IdString, Identifier, Party}
import com.daml.platform.store.dao.events.BufferedTransactionsReader.invertMapping
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class BufferedTransactionsReaderSpec extends AsyncWordSpec with Matchers {

  "invertMapping" should {
    "invert the mapping of Map[Party, Set[TemplateId]] into a Map[TemplateId, Set[Party]]" in {
      def party: String => IdString.Party = Party.assertFromString
      def templateId: String => Identifier = Identifier.assertFromString

      val partiesToTemplates =
        Map(
          party("p11") -> Set(templateId("a:b:t1")),
          party("p12") -> Set(templateId("a:b:t1"), templateId("a:b:t2")),
          party("p21") -> Set(templateId("a:b:t2")),
        )

      val expectedTemplatesToParties =
        Map(
          templateId("a:b:t1") -> Set(party("p11"), party("p12")),
          templateId("a:b:t2") -> Set(party("p21"), party("p12")),
        )

      invertMapping(partiesToTemplates) shouldBe expectedTemplatesToParties
    }
  }
}
