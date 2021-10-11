// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import com.daml.lf.data.TryOps.Bracket.bracket

import scala.util.{Failure, Success, Try}

@deprecated("Bracket is deprecated; use scala.util.Using instead", since = "1.16.0")
class TryOpsTest extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  "bracket should call clean after successful calculation" in forAll { (a: Int, b: Int) =>
    var calls = List.empty[String]

    def clean(x: Int): Try[Unit] = {
      calls = s"clean $x" :: calls
      Success(())
    }

    def add(x: Int)(y: Int): Try[Int] = {
      calls = s"add $x $y" :: calls
      Success(x + y)
    }

    val actual = bracket(Try(a))(clean).flatMap(add(b))
    actual shouldBe Success(a + b)
    calls.reverse shouldBe List(s"add $b $a", s"clean $a")
  }

  "bracket should fail if clean failed" in forAll { (a: Int, b: Int, e: Throwable) =>
    var calls = List.empty[String]

    def clean(x: Int): Try[Unit] = {
      calls = s"clean $x $e" :: calls
      Failure(e)
    }

    def add(x: Int)(y: Int): Try[Int] = {
      calls = s"add $x $y" :: calls
      Success(x + y)
    }

    val actual = bracket(Try(a))(clean).flatMap(add(b))
    actual shouldBe Failure(e)
    calls.reverse shouldBe List(s"add $b $a", s"clean $a $e")
  }

  "bracket should call clean if calculation fails" in forAll { (a: Int, b: Int, e: Throwable) =>
    var calls = List.empty[String]

    def clean(x: Int): Try[Unit] = {
      calls = s"clean $x" :: calls
      Success(())
    }

    def add(x: Int)(y: Int): Try[Int] = {
      calls = s"add $x $y" :: calls
      Failure(e)
    }

    val actual = bracket(Try(a))(clean).flatMap(add(b))
    actual shouldBe Failure(e)
    calls.reverse shouldBe List(s"add $b $a", s"clean $a")
  }

  "bracket should return calculation error if if both calculation and clean fail" in forAll {
    (a: Int, b: Int, e1: Throwable, e2: Throwable) =>
      var calls = List.empty[String]

      def clean(x: Int): Try[Unit] = {
        calls = s"clean $x $e2" :: calls
        Failure(e2)
      }

      def add(x: Int)(y: Int): Try[Int] = {
        calls = s"add $x $y" :: calls
        Failure(e1)
      }

      val actual = bracket(Try(a))(clean).flatMap(add(b))
      actual shouldBe Failure(e1)
      calls.reverse shouldBe List(s"add $b $a", s"clean $a $e2")
  }
}
