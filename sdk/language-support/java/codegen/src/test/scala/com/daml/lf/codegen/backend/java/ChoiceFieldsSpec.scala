// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java

import com.daml.ledger.javaapi.data.Unit
import com.daml.ledger.javaapi.data.codegen.Choice
import com.daml.ledger.javaapi.data.codegen.json.JsonLfReader
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ut.bar.Bar
import ut.bar.AddOne
import ut.bar.Result
import ut.da.internal.template.Archive

import scala.jdk.CollectionConverters._

final class ChoiceFieldsSpec extends AnyWordSpec with Matchers {
  "Template" should {
    "have choice fields" in {
      val choice: Choice[Bar, Archive, Unit] = Bar.CHOICE_Archive
      choice.name shouldBe "Archive"
    }

    "have choices map in Template Companion(COMPANION)" in {
      val choices = Bar.COMPANION.choices
      val names = choices.keySet()

      choices.size() shouldBe 2
      names shouldBe Set("Archive", "AddOne").asJava
    }

    "encode and decode choice arguments in json" in {
      val choice: Choice[Bar, AddOne, Result] = Bar.CHOICE_AddOne
      val dummyArg = new AddOne(4)
      val encodedArg = choice.argJsonEncoder(dummyArg).intoString()

      "{\"value\": \"4\"}" shouldBe encodedArg

      val decodedArg = choice.argJsonDecoder.decode(new JsonLfReader(encodedArg))
      dummyArg shouldBe decodedArg
    }

    "encode and decode choice results in json" in {
      val choice: Choice[Bar, AddOne, Result] = Bar.CHOICE_AddOne
      val dummyResult = new Result(5)
      val encodedResult = choice.resultJsonEncoder(dummyResult).intoString()

      "{\"result\": \"5\"}" shouldBe encodedResult

      val decodedResult = choice.resultJsonDecoder.decode(new JsonLfReader(encodedResult))
      dummyResult shouldBe decodedResult
    }
  }
}
