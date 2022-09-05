// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.util.Random

class Utf8Spec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  private def codepointToString(cp: Int): String =
    Character.toChars(cp).mkString

  private val lowCodepoints =
    Gen.chooseNum(Character.MIN_CODE_POINT, Character.MIN_HIGH_SURROGATE - 1).map(codepointToString)
  private val highCodepoints =
    Gen.chooseNum(Character.MAX_LOW_SURROGATE + 1, Character.MAX_CODE_POINT).map(codepointToString)
  private val asciiCodepoints =
    Gen.asciiChar.map(c => codepointToString(c.toInt))
  private val twoWordsCodepoints =
    Gen.chooseNum(Character.MAX_VALUE + 1, Character.MAX_CODE_POINT).map(codepointToString)

  private val codepoints =
    Gen.frequency(
      5 -> asciiCodepoints,
      5 -> twoWordsCodepoints,
      1 -> lowCodepoints,
      1 -> highCodepoints,
    )

  private val strings =
    Gen.listOf(codepoints).map(_.mkString)

  // All the legal codepoints in increasing order converted in string
  private val legalCodePoints =
    ((Character.MIN_CODE_POINT to Character.MIN_HIGH_SURROGATE) ++
      (Character.MAX_LOW_SURROGATE + 1 until Character.MAX_CODE_POINT))
      .map(codepointToString)

  "Unicode.explode" should {

    "explode properly counter example" in {
      "a¶‱😂".toList shouldNot be(List("a", "¶", "‱", "😂"))
      Utf8.explode("a¶‱😂") shouldBe ImmArray("a", "¶", "‱", "😂")
    }

    "explode in a same way a naive implementation" in {
      def naiveExplode(s: String) =
        s.codePoints().iterator().asScala.map(codepointToString(_)).to(ImmArray)

      forAll(strings) { s =>
        naiveExplode(s) shouldBe Utf8.explode(s)
      }

    }
  }

  "Unicode.Ordering" should {

    "do not have basic Utf16 ordering issue" in {
      val s1 = "｡"
      val s2 = "😂"
      val List(cp1) = s1.codePoints().iterator().asScala.toList
      val List(cp2) = s2.codePoints().iterator().asScala.toList

      Ordering.String.lt(s1, s2) shouldNot be(Ordering.Int.lt(cp1, cp2))
      Utf8.Ordering.lt(s1, s2) shouldBe Ordering.Int.lt(cp1, cp2)
    }

    "be reflexive" in {
      forAll(strings) { x =>
        Utf8.Ordering.compare(x, x) shouldBe 0
      }
    }

    "consistent when flipping its arguments" in {
      forAll(strings, strings) { (x, y) =>
        Utf8.Ordering.compare(x, y).signum shouldBe -Utf8.Ordering.compare(y, x).signum
      }
    }

    "be transitive" in {
      import Utf8.Ordering.lteq

      forAll(strings, strings, strings) { (x_, y_, z_) =>
        val List(x, y, z) = List(x_, y_, z_).sorted(Utf8.Ordering)
        lteq(x, y) && lteq(y, z) && lteq(x, z) shouldBe true
      }
    }

    "respect Unicode ordering on individual code points" in {

      val shuffledCodepoints = new Random(0).shuffle(legalCodePoints)

      // Sort according Utf16
      val negativeCase = shuffledCodepoints.sorted

      // Sort according our ad hoc ordering
      val positiveCase = shuffledCodepoints.sorted(Utf8.Ordering)

      negativeCase shouldNot be(legalCodePoints)
      positiveCase shouldBe legalCodePoints

    }

    "respect Unicode ordering on complex string" in {

      // a naive inefficient Unicode ordering
      import Ordering.Implicits._
      val naiveOrdering =
        Ordering.by((s: String) => s.codePoints().toArray.toSeq)

      forAll { list: List[String] =>
        list.sorted(naiveOrdering) shouldBe list.sorted(Utf8.Ordering)
      }

    }

    "be strict on individual code points" in {
      (legalCodePoints zip legalCodePoints.tail).foreach { case (x, y) =>
        Utf8.Ordering.compare(x, y) should be < 0
      }
    }

  }

  "pack" should {

    def makeImmArray(cp: Long) = ImmArray('-'.toLong, cp, '-'.toLong)

    "properly converts any legal code points" in {
      for (
        cp <- (Character.MIN_CODE_POINT until Character.MIN_SURROGATE) ++
          ((Character.MAX_SURROGATE + 1) to Character.MAX_CODE_POINT)
      )
        Utf8.pack(makeImmArray(cp.toLong)) shouldBe Right(
          "-" + new String(Character.toChars(cp)) + "-"
        )
    }

    "reject any surrogate code point" in {
      for (cp <- Character.MIN_SURROGATE to Character.MAX_SURROGATE)
        Utf8.pack(makeImmArray(cp.toLong)) shouldBe a[Left[_, _]]
    }

    "reject too small or too big code points" in {
      val testCases = List(
        Long.MinValue,
        Int.MinValue.toLong,
        Character.MIN_CODE_POINT - 2L,
        Character.MIN_CODE_POINT - 1L,
        Character.MAX_CODE_POINT + 1L,
        Character.MAX_CODE_POINT + 2L,
        Int.MaxValue.toLong,
        Long.MaxValue,
      )

      for (cp <- testCases)
        Utf8.pack(makeImmArray(cp)) shouldBe a[Left[_, _]]
    }

    "packs properly" in {
      Utf8.pack(ImmArray.Empty) shouldBe Right("")
      Utf8.pack(ImmArray(0x00061, 0x000b6, 0x02031, 0x1f602)) shouldBe Right("a¶‱😂")
    }
  }

  "unpack" should {
    "unpacks properly" in {
      Utf8.pack(ImmArray.Empty) shouldBe Right("")
      Utf8.unpack("a¶‱😂") shouldBe ImmArray(0x00061, 0x000b6, 0x02031, 0x1f602)
    }
  }

  "pack and unpack" should {
    "form an isomorphism between strings and sequences of legal code points" in {
      forAll(strings)(s => Utf8.pack(Utf8.unpack(s)) shouldBe Right(s))
    }
  }

}
