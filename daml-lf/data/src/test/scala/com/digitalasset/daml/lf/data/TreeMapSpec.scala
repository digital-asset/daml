// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class TreeMapSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  val strictLyOrderedTestCases = Table(
    "list",
    List.empty,
    List("1" -> 1),
    List("1" -> 1, "2" -> 2, "3" -> 3),
  )

  val nonStrictlyOrderedTestCases = Table(
    "list",
    List("1" -> 1, "1" -> 2),
    List("1" -> 1, "2" -> 2, "3" -> 3, "3" -> 2),
  )

  val nonOrderedTestCases = Table(
    "list",
    List("1" -> 1, "0" -> 2),
    List("1" -> 1, "2" -> 2, "3" -> 3, "1" -> 2),
    List("2" -> 2, "3" -> 3, "1" -> 1),
  )

  "TreeMap.fromOrderedEntries should fails if the input list is not ordered" in {
    val negativeTestCases = strictLyOrderedTestCases ++ nonStrictlyOrderedTestCases
    val positiveTestCases = nonOrderedTestCases

    forAll(negativeTestCases) { l =>
      TreeMap.fromOrderedEntries(l) shouldBe l.toMap
    }
    forAll(positiveTestCases) { l =>
      a[IllegalArgumentException] shouldBe thrownBy(TreeMap.fromOrderedEntries(l))
    }
  }

  "TreeMap.fromOrderedEntries should fails if the input list is not strictly ordered" in {
    val negativeTestCases = strictLyOrderedTestCases
    val positiveTestCases = nonStrictlyOrderedTestCases ++ nonOrderedTestCases

    forAll(negativeTestCases) { l =>
      TreeMap.fromStrictlyOrderedEntries(l) shouldBe l.toMap
    }
    forAll(positiveTestCases) { l =>
      a[IllegalArgumentException] shouldBe thrownBy(TreeMap.fromStrictlyOrderedEntries(l))
    }
  }
  "TreeSet.fromOrderedEntries should fails if the input list is not ordered" in {
    val negativeTestCases = strictLyOrderedTestCases ++ nonStrictlyOrderedTestCases
    val positiveTestCases = nonOrderedTestCases

    forAll(negativeTestCases) { l0 =>
      val l = l0.map(_._1)
      TreeSet.fromOrderedEntries(l) shouldBe l.toSet
    }
    forAll(positiveTestCases) { l0 =>
      val l = l0.map(_._1)
      a[IllegalArgumentException] shouldBe thrownBy(TreeSet.fromOrderedEntries(l))
    }
  }

  "TreeSet.fromOrderedEntries should fails if the input list is not strictly ordered" in {
    val negativeTestCases = strictLyOrderedTestCases

    val positiveTestCases = (nonStrictlyOrderedTestCases ++ nonOrderedTestCases)

    forAll(negativeTestCases) { l0 =>
      val l = l0.map(_._1)
      TreeSet.fromStrictlyOrderedEntries(l) shouldBe l.toSet
    }

    forAll(positiveTestCases) { l0 =>
      val l = l0.map(_._1)
      a[IllegalArgumentException] shouldBe thrownBy(TreeSet.fromStrictlyOrderedEntries(l))
    }
  }

}
