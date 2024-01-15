// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nameof

import com.daml.nameof.NameOf.{qualifiedNameOf, qualifiedNameOfCurrentFunc, qualifiedNameOfMember}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class NameOfSpec extends AnyFlatSpec with Matchers {

  behavior of "NameOf.qualifiedNameOfCurrentFunc"

  case class Ham(ster: Int = 2) {
    def ham(): String = qualifiedNameOfCurrentFunc
    val jam: String = ""
    def scam[A](x: A): Boolean = true
  }

  class Spam() {
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

  it should "return the correct full qualified name of the ham function in case class Ham" in {
    Ham().ham() shouldBe "com.daml.nameof.NameOfSpec.Ham.ham"
  }

  it should "return the correct full qualified name of the spam function in class Spam" in {
    new Spam().spam() shouldBe "com.daml.nameof.NameOfSpec.Spam.spam"
  }

  it should "return the correct full qualified name of the foo function in object Foo" in {
    Foo.foo() shouldBe "com.daml.nameof.NameOfSpec.Foo.foo"
  }

  it should "return the correct full qualified name of the nested function in object Nested within object Root" in {
    Root.Nested.nested() shouldBe "com.daml.nameof.NameOfSpec.Root.Nested.nested"
  }

  it should "not compile outside of functions" in {
    "qualifiedNameOfCurrentFunc" shouldNot compile
    "class TestClass { qualifiedNameOfCurrentFunc }" shouldNot compile
  }

  behavior of "NameOf.qualifiedNameOf"

  it should "return the correct full qualified name of the given symbol" in {
    qualifiedNameOf(Ham().ham()) shouldBe "com.daml.nameof.NameOfSpec.Ham.ham"
    qualifiedNameOf(Ham.apply _) shouldBe "com.daml.nameof.NameOfSpec.Ham.apply"
    qualifiedNameOf(Foo.foo()) shouldBe "com.daml.nameof.NameOfSpec.Foo.foo"
    qualifiedNameOf(Root.Nested.nested()) shouldBe
      "com.daml.nameof.NameOfSpec.Root.Nested.nested"
    qualifiedNameOf(None) shouldBe "scala.None"
    qualifiedNameOf(Option.empty) shouldBe "scala.Option.empty"
  }

  it should "not compile if no symbol is found" in {
    "qualifiedNameOf(6)" shouldNot compile
    """qualifiedNameOf("abc")""" shouldNot compile
  }

  behavior of "NameOf.qualifiedNameOfMember"

  it should "return the correct full qualified name of the member" in {
    qualifiedNameOfMember[Ham](_.ster) shouldBe "com.daml.nameof.NameOfSpec.Ham.ster"
    qualifiedNameOfMember[Ham](_.ham()) shouldBe "com.daml.nameof.NameOfSpec.Ham.ham"
    qualifiedNameOfMember[Ham](_.jam) shouldBe "com.daml.nameof.NameOfSpec.Ham.jam"
    qualifiedNameOfMember[Ham](_.scam(??? : Unit)) shouldBe "com.daml.nameof.NameOfSpec.Ham.scam"
    qualifiedNameOfMember[Spam](_.spam()) shouldBe "com.daml.nameof.NameOfSpec.Spam.spam"
    qualifiedNameOfMember[Foo.type](_.foo()) shouldBe "com.daml.nameof.NameOfSpec.Foo.foo"
    qualifiedNameOfMember[Root.Nested.type](_.nested()) shouldBe
      "com.daml.nameof.NameOfSpec.Root.Nested.nested"
    qualifiedNameOfMember[String](_.strip()) shouldBe "java.lang.String.strip"
    qualifiedNameOfMember[Option[_]](_.map(_ => ???)) shouldBe "scala.Option.map"
  }

  it should "not compile if no symbol is found" in {
    "qualifiedNameOfMember[Int](_ => 5)" shouldNot compile
    """qualifiedNameOfMember[String](_ => "a")""" shouldNot compile
    "qualifiedNameOfMember[Ham](_.scam(5).toLong)" shouldNot compile
  }

}
