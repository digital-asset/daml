// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.util.Random

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class Utf8StringSpec extends WordSpec with Matchers {

  private def codepointToUtf8String(cp: Int) =
    Utf8String(Character.toChars(cp).mkString)

  private val lowCodepoints =
    Gen
      .chooseNum(Character.MIN_CODE_POINT, Character.MIN_HIGH_SURROGATE - 1)
      .map(codepointToUtf8String)
  private val highCodepoints =
    Gen
      .chooseNum(Character.MAX_LOW_SURROGATE + 1, Character.MAX_CODE_POINT)
      .map(codepointToUtf8String)
  private val asciiCodepoints =
    Gen.asciiChar.map(c => codepointToUtf8String(c.toInt))
  private val twoWordsCodepoints =
    Gen.chooseNum(Character.MAX_VALUE + 1, Character.MAX_CODE_POINT).map(codepointToUtf8String)

  private val codepoints =
    Gen.frequency(
      5 -> asciiCodepoints,
      5 -> twoWordsCodepoints,
      1 -> lowCodepoints,
      1 -> highCodepoints
    )

  private val strings: Gen[Utf8String] =
    Gen.listOf(codepoints).map(x => Utf8String(x.mkString))

  // All the valid Unicode codepoints in increasing order converted in string
  private val validCodepoints =
    ((Character.MIN_CODE_POINT to Character.MIN_HIGH_SURROGATE) ++
      (Character.MAX_LOW_SURROGATE + 1 until Character.MAX_CODE_POINT))
      .map(codepointToUtf8String)

  "Unicode.explode" should {

    val s = Utf8String("a¶‱😂")

    "explode properly counter example" in {
      val expectedOutput = ImmArray("a", "¶", "‱", "😂").map(Utf8String(_))
      ImmArray(s.javaString.toList).map(c => Utf8String(c.toString)) shouldNot be(expectedOutput)
      s.explode shouldBe expectedOutput
    }

    "explode in a same way a naive implementation" in {
      def naiveExplode(s: Utf8String) =
        ImmArray(
          s.javaString.codePoints().iterator().asScala.map(codepointToUtf8String(_)).toIterable)

      forAll(strings) { s =>
        naiveExplode(s) == s.explode
      }

    }
  }

  "Unicode.Ordering" should {

    "do not have basic UTF16 ordering issue" in {
      val s1 = Utf8String("｡")
      val s2 = Utf8String("😂")
      val List(cp1) = s1.javaString.codePoints().iterator().asScala.toList
      val List(cp2) = s2.javaString.codePoints().iterator().asScala.toList

      s1.javaString < s2.javaString shouldNot be(cp1 < cp2)
      (s1 < s2) shouldBe (cp1 < cp2)
    }

    "be reflexive" in {
      forAll(strings) { x =>
        x.compare(x) == 0
      }
    }

    "consistent when flipping its arguments" in {
      forAll(strings, strings) { (x, y) =>
        (x compare y).signum == -(y compare x).signum
      }
    }

    "be transitive" in {
      forAll(strings, strings, strings) { (x_, y_, z_) =>
        val List(x, y, z) = List(x_, y_, z_).sorted
        x < y && y < z && x < z
      }
    }

    "respect Unicode ordering on individual codepoints" in {

      val shuffledCodepoints = new Random(0).shuffle(validCodepoints)

      // Sort according UTF16
      val negativeCase = shuffledCodepoints.map(_.javaString).sorted.map(Utf8String(_))

      // Sort according our ad hoc ordering
      val positiveCase = shuffledCodepoints.sorted

      negativeCase shouldNot be(validCodepoints)
      positiveCase shouldBe validCodepoints

    }

    "respect Unicode ordering on complex string" in {

      forAll(Gen.listOfN(20, strings)) { list =>
        val utf16Sorted = list.map(_.javaString).sorted.map(Utf8String(_))
        val utf8Sorted = list.sorted
        utf16Sorted == utf8Sorted
      }

    }

    "be strict on individual codepoints" in {
      (validCodepoints zip validCodepoints.tail).foreach {
        case (x, y) => (x compare y) should be < 0
      }
    }

  }

}
