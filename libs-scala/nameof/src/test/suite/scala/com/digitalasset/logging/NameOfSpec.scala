// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nameof

import com.daml.nameof.NameOf.qualifiedNameOfCurrentFunc
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class NameOfSpec extends AnyFlatSpec with Matchers {

  behavior of "NameOf"

  case class Ham() {
    def ham(): String = qualifiedNameOfCurrentFunc
  }

  case class Spam() {
    def spam(): String = qualifiedNameOfCurrentFunc
  }

  object Foo {
    def foo(): String = qualifiedNameOfCurrentFunc
  }

  object Root {
    object Nested {
      def nested(): String = qualifiedNameOfCurrentFunc
    }
  }

  it should "return the correct full qualified name of the ham function in class Ham" in {
    Ham().ham() shouldBe "com.daml.nameof.NameOfSpec.Ham.ham"
  }

  it should "return the correct full qualified name of the spam function in class Spam" in {
    Spam().spam() shouldBe "com.daml.nameof.NameOfSpec.Spam.spam"
  }

  it should "return the correct full qualified name of the foo function in object Foo" in {
    Foo.foo() shouldBe "com.daml.nameof.NameOfSpec.Foo.foo"
  }

  it should "return the correct full qualified name of the nested function in object Nested within object Root" in {
    Root.Nested.nested() shouldBe "com.daml.nameof.NameOfSpec.Root.Nested.nested"
  }
}
