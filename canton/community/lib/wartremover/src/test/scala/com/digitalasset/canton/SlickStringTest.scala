// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser
import slick.jdbc.PositionedParameters
import slick.jdbc.canton.ActionBasedSQLInterpolation.Implicits.*

class SlickStringTest extends AnyWordSpec with Matchers {

  def assertIsError(result: WartTestTraverser.Result): Assertion = {
    result.errors.length should be >= 1
    result.errors.foreach { _ should include(SlickString.message) }
    succeed
  }

  "SlickString" should {
    "detect explicit setString" in {
      val result = WartTestTraverser(SlickString) {
        val pp = (??? : PositionedParameters)
        pp.setString("foo")
      }
      assertIsError(result)
    }

    "detect explicit setStringOption" in {
      val result = WartTestTraverser(SlickString) {
        val pp = (??? : PositionedParameters) // fo
        pp.setStringOption(None)
      }
      assertIsError(result)
    }

    "detect implicit setString" in {
      val result = WartTestTraverser(SlickString) {
        val pp = (??? : PositionedParameters)
        pp >> "bar"
      }
      assertIsError(result)
    }

    "detect implicit setStringOption" in {
      val result = WartTestTraverser(SlickString) {
        val pp = (??? : PositionedParameters)
        pp >> Option("bar")
      }
      assertIsError(result)
    }

    "detect interpolated setString" in {
      val sqlResult = WartTestTraverser(SlickString) {
        val _ = sql"${"some string"}"
      }
      assertIsError(sqlResult)

      val sqluResult = WartTestTraverser(SlickString) {
        val _ = sqlu"${"some string"}"
      }
      assertIsError(sqluResult)
    }

    "detect interpolated setStringOption" in {
      val sqlResult = WartTestTraverser(SlickString) {
        val _ = sql"${Option("some string")}"
      }
      assertIsError(sqlResult)

      val sqluResult = WartTestTraverser(SlickString) {
        val _ = sqlu"${Option("some string")}"
      }
      assertIsError(sqluResult)
    }

    "allow references to unrelated SetString objects" in {
      val result = WartTestTraverser(SlickString) {
        val _ = SetParameter.SetString
      }
      result.errors shouldBe List.empty
    }
  }

  object SetParameter {
    object SetString extends slick.jdbc.SetParameter[String] {
      override def apply(v1: String, v2: PositionedParameters): Unit = ???
    }
  }
}
