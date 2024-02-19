// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java

import com.daml.ledger.javaapi.data.Unit
import com.daml.ledger.javaapi.data.codegen.Choice
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ut.bar.Bar
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

      choices.size() shouldBe 1
      names shouldBe Set("Archive").asJava
    }
  }
}
