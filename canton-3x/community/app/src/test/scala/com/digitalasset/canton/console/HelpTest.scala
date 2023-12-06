// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.console.Help.forInstance
import org.scalatest.funsuite.AnyFunSuite

class HelpTest extends AnyFunSuite with BaseTest {

  object Example {

    class MoreNested {}

    class Nested {
      @Help.Summary("A nested method")
      @Help.Topic(Seq("Nested"))
      def nestedMethod(name: String): Int = 44
    }

    @Help.Summary("Full Method")
    @Help.Description("Full Method description")
    @Help.Topic(Seq("Example", "Full Method"))
    def fullMethod(name: String): Int = 42

    @Help.Summary("No Params and Unit", FeatureFlag.Testing)
    def noParametersAndUnit(): Unit = ()

    @Help.Summary("Should not print unit")
    def unitReturning(name: String): Unit = ()

    @Help.Summary("Multiple parameters")
    def multipleParameters(first: String, second: Int): Unit = ()

    @Help.Summary("Some grouped methods")
    @Help.Group("Key Vault Api")
    def nested: Nested = new Nested()

  }

  trait TestHelpful {
    @Help.Summary("Usage")
    def help(): String = forInstance(this)
  }

  object HelpfulExample extends TestHelpful {
    @Help.Summary("Is a thing")
    def thing(): Unit = ()
  }

  object ImplicitsExample extends TestHelpful {
    @Help.Summary("Implicit should not be visible")
    def thing(implicit someImplicit: Int): Unit = ()

    @Help.Summary("Even if in the second argument set")
    def another(name: String)(implicit someImplicit: Int): Unit = ()
  }

  trait Trait1 {
    @Help.Summary("Trait1")
    @Help.Topic(Seq("Top-level Commands"))
    def trait1(): Unit = {}
  }

  trait Trait2 {
    @Help.Summary("Trait2")
    @Help.Topic(Seq("Top-level Commands"))
    def trait2(): Unit = {}
  }

  class MultipleTraits extends Helpful with Trait1 with Trait2 {}

  test("Producing help for Example") {
    forInstance(Example) should be(
      """
        |Top-level Commands
        |------------------
        |multipleParameters - Multiple parameters
        |noParametersAndUnit - No Params and Unit
        |unitReturning - Should not print unit
        |
        |Example: Full Method
        |--------------------
        |fullMethod - Full Method
        |
        |Command Groups
        |--------------
        |nested - Some grouped methods
      """.stripMargin.trim
    )
  }

  test("Omit the testing scope on the help") {
    forInstance(Example, scope = Set(FeatureFlag.Stable)) should not include ("noParametersAndUnit")
  }

  test("Omit the testing scope on suggestions") {
    Help.forMethod(
      Example,
      "no",
      scope = Set(FeatureFlag.Stable),
    ) should not include "noParametersAndUnit"
  }

  test("Not find the method which is out of scope") {
    Help.forMethod(
      Example,
      "noParametersAndUnit",
      scope = Set(FeatureFlag.Stable),
    ) shouldBe "Error: method noParametersAndUnit not found; check your spelling"
  }

  test("Units don't get displayed") {
    Help
      .forMethod(Example, "unitReturning")
      .trim shouldBe "unitReturning(name: String)\nShould not print unit"
    Help
      .forMethod(Example, "noParametersAndUnit")
      .trim shouldBe "noParametersAndUnit\nNo Params and Unit"
  }

  test("Having a trait generating helpful messages") {
    HelpfulExample.help() should be("""
        |Top-level Commands
        |------------------
        |help - Usage
        |thing - Is a thing
      """.stripMargin.trim)
  }

  test("Implicits don't get displayed") {
    Help.forMethod(ImplicitsExample, "thing").trim shouldBe "thing\nImplicit should not be visible"
    Help
      .forMethod(ImplicitsExample, "another")
      .trim shouldBe "another(name: String)\nEven if in the second argument set"
  }

  test("Help should be sourced from all traits") {
    forInstance(new MultipleTraits()) should be(
      """
        |Top-level Commands
        |------------------
        |help - Help for specific commands (use help() or help("method") for more information)
        |trait1 - Trait1
        |trait2 - Trait2
      """.stripMargin.trim
    )
  }

  test("Description and types get displayed for detailed method help") {
    Help.forMethod(Example, "fullMethod") shouldBe
      Seq("fullMethod(name: String): Int", "Full Method description").mkString(System.lineSeparator)
  }

  test("Help suggestions get displayed for top level items") {
    Help.forMethod(Example, "unit") should be(
      """Error: method unit not found; are you looking for one of the following?
        |  unitReturning""".stripMargin
    )
  }

  test("Help suggestions get displayed for nested items") {
    Help.forMethod(Example, "nestedMet") should be(
      """Error: method nestedMet not found; are you looking for one of the following?
        |  nested.nestedMethod""".stripMargin
    )
  }

  test("Description and types get displayed for nested method help") {
    Help.forMethod(Example, "nested.nestedMethod") should be(
      """nested.nestedMethod(name: String): Int
        |A nested method""".stripMargin
    )
  }

}
