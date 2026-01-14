// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

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
        Utf8.Ordering.compare(x, y).sign shouldBe -Utf8.Ordering.compare(y, x).sign
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

  "sha256" should {
    "correctly digest string messages" in {
      val testCases = List(
        ("", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
        ("Hello World!", "7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069"),
        ("deadbeef", "2baf1f40105d9501fe319a8ec463fdf4325a2a5df445adf3f572f626253678c9"),
        ("d", "18ac3e7343f016890c510e93f935261169d9e3f565436429830faf0934f4f8e4"),
        ("DeadBeef", "7028db7dd9980cabb65ffafe7b154b8979f275b171d29a1a50fe79ea1f21d77e"),
      )

      for ((msg, digest) <- testCases) {
        Utf8.sha256(Utf8.getBytes(msg)) shouldBe digest
      }
    }

    "correctly digest hex encoded messages" in {
      val testCases = List(
        ("", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
        ("deadbeef", "5f78c33274e43fa9de5659265c1d917e25c03722dcb0b8d27db8d5feaa813953"),
      )

      for ((msg, digest) <- testCases) {
        Utf8.sha256(Ref.HexString.decode(Ref.HexString.assertFromString(msg))) shouldBe digest
      }
    }
  }
}
