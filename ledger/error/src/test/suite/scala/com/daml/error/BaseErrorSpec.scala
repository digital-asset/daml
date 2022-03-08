// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn

class A

@nowarn
class B(x: String)

class C(val x: String)

case class FooA(msg: String) extends A
case class FooB_1(msg: String) extends B(x = "aaa")
case class FooB_2(msg: String) extends B(x = msg)
case class FooC_1(msg: String) extends C(x = "aaa")
case class FooC_2(msg: String) extends C(x = msg)

case class Bar(a: String, _b: String, cause: String, throwable: String, loggingContext: String)

class BaseErrorSpec extends AnyWordSpec with Matchers {

  "it" should {

    "FooA" in {
      BaseError.extractContext(FooA("123")) shouldBe Map("msg" -> "123")
    }

    "FooB_1" in {
      BaseError.extractContext(FooB_1("123")) shouldBe Map("msg" -> "123")
    }
    "FooB_2" in {
      BaseError.extractContext(FooB_2("123")) shouldBe Map("msg" -> "123")
    }

    "FooC_1" in {
      BaseError.extractContext(FooC_1("123")) shouldBe Map("msg" -> "123")
    }

    "FooC_2" in {
      BaseError.extractContext(FooC_2("123")) shouldBe Map()
    }

    "Bar" in {
      BaseError.extractContext(
        Bar(a = "1", _b = "2", cause = "3", throwable = "4", loggingContext = "5")
      ) shouldBe Map("a" -> "1")
    }
  }
}
