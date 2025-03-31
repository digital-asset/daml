package org.scalatest

import org.scalatest.exceptions.TestFailedException
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class AssertionsUtilTest extends AnyWordSpec with Matchers with EitherValues {

  "AssertionsUtil.assertOnTypeError" should {
    "check for type errors" in {
      AssertionsUtil.assertOnTypeError("val i: String = 5") { typeError =>
        typeError should (include("type mismatch") and include("5") and include("java.lang.String"))
      }

      AssertionsUtil.assertOnTypeError("implicitly[Int =:= String]") { typeError =>
        typeError should include("Cannot prove that Int =:= String")
      }

      Try(
        AssertionsUtil.assertOnTypeError("val i: Int = 5")(_ => succeed)
      ).toEither.swap.value shouldBe a[TestFailedException]

      Try(AssertionsUtil.assertOnTypeError("val i: String = 5") { typeError =>
        typeError should include("foobar")
      }).toEither.swap.value shouldBe a[TestFailedException]
    }

    "fail on parse errors" in {
      val err =
        Try(AssertionsUtil.assertOnTypeError("val i = 5)")(_ => succeed)).toEither.swap.value
      err.getMessage should include("Expected a type error, but got the following parse error:")
    }
  }
}
