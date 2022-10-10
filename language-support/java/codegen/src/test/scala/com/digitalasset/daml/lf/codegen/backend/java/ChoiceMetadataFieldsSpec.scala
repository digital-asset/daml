// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java

import com.daml.ledger.javaapi.data.Unit
import com.daml.ledger.javaapi.data.codegen.ChoiceMetadata
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import retro.InterfaceRetro
import ut.bar.Bar
import ut.da.internal.template.Archive

import scala.jdk.CollectionConverters._

final class ChoiceMetadataFieldsSpec extends AnyWordSpec with Matchers {
  "Template" should {
    "have choice fields" in {
      val choice: ChoiceMetadata[Bar, Archive, Unit] = Bar.CHOICE_Archive
      choice.name shouldBe "Archive"
    }

    "have choices map in Template Companion(COMPANION)" in {
      val choices = Bar.COMPANION.choices
      val names = choices.keySet()

      choices.size() shouldBe 1
      names shouldBe Set("Archive").asJava
    }
  }

  "Interface" should {
    "have choice fields" in {
      val choices = Set(InterfaceRetro.CHOICE_Archive, InterfaceRetro.CHOICE_Transfer)
      val expectedNames = Set("Archive", "Transfer")

      choices.map(_.name) should contain theSameElementsAs expectedNames
    }

    "have choices map in Interface Companion(INTERFACE)" in {
      val choices = InterfaceRetro.INTERFACE.choices
      val actualNames = choices.keySet()
      val expectedNames = Set("Archive", "Transfer").asJava

      choices.size() shouldBe 2
      actualNames shouldBe expectedNames
    }
  }
}
