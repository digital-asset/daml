// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.daml.error.ErrorGenerator.asciiPrintableStrOfN
import com.daml.error.SerializableErrorComponentsSpec.*
import org.scalacheck.Gen
import org.scalatest.Assertions.fail
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, Inside, OptionValues}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class SerializableErrorComponentsSpec
    extends AnyFlatSpec
    with Matchers
    with EitherValues
    with Inside
    with OptionValues
    with ScalaCheckDrivenPropertyChecks {

  behavior of "truncateDetails"

  it should "not change details if reasonably-sized and valid" in {
    val (truncatedContext, truncatedResources) =
      NonSecuritySensitiveErrorCodeComponents.truncateDetails(
        context = rawContextMap,
        errResources = rawErrorResources,
        remainingBudgetBytes = 1000,
      )

    truncatedContext should contain theSameElementsAs rawContextMap
    truncatedResources should contain theSameElementsAs comparable(rawErrorResources)
  }

  it should "truncate context map and resources when oversize" in {
    val cMap =
      Map(
        "k1".padTo(20, 'a') -> cValue(20),
        "k2".padTo(30, 'a') -> cValue(40),
        "k3".padTo(20, 'a') -> cValue(100),
        "k4".padTo(50, 'a') -> cValue(160),
        "k5".padTo(60, 'a') -> cValue(320),
      )

    val errRes: Seq[(ErrorResource, String)] =
      (1 to 10).map(i => ErrorResource.CommandId -> s"some-command-id-$i".padTo(50, 'a'))

    val (truncatedContext, truncatedResources) =
      NonSecuritySensitiveErrorCodeComponents.truncateDetails(
        context = cMap,
        errResources = errRes,
        remainingBudgetBytes = 1000,
      )

    val expected = Map(
      "k1".padTo(20, 'a') -> cValue(20),
      "k2".padTo(30, 'a') -> truncatedValue("", 31),
      "k3".padTo(20, 'a') -> truncatedValue("", 41),
      truncatedValue("k4", 30) -> truncatedValue("", 31),
      truncatedValue("k5", 30) -> truncatedValue("", 31),
    )
    truncatedContext should contain theSameElementsAs expected
    truncatedResources should contain theSameElementsAs comparable(errRes.take(5))
  }

  behavior of "truncateContext"

  it should "validate context map keys" in {
    val invalidKeyEntry = "%k4!" -> cValue(50)
    val invalidKeyEntryCorrectSize = "%k5!".padTo(65, 'a') -> cValue(50)
    val invalidKeyEntryOversize = "%k6!".padTo(100, 'a') -> cValue(50)
    val correctKeyOversize = "k7-size".padTo(64, 'a') -> cValue(50)

    val truncatedContext =
      NonSecuritySensitiveErrorCodeComponents.truncateContext(
        rawContextEntries = rawContextMap.toVector ++ Vector(
          invalidKeyEntry,
          invalidKeyEntryCorrectSize,
          invalidKeyEntryOversize,
          correctKeyOversize,
        ),
        maxBudgetBytes = 1000,
      )

    truncatedContext should contain theSameElementsAs rawContextMap.toVector ++ Vector(
      "k4" -> cValue(50),
      "k5".padTo(63, 'a') -> cValue(50),
      "k6".padTo(63, 'a') -> cValue(50),
      "k7-size".padTo(63, 'a') -> cValue(50),
    )
  }

  it should "return an empty list on empty input" in {
    NonSecuritySensitiveErrorCodeComponents.truncateContext(
      rawContextEntries = Vector.empty,
      maxBudgetBytes = 4096,
    ) shouldBe empty
  }

  it should "return an empty list on too small budget" in {
    NonSecuritySensitiveErrorCodeComponents.truncateContext(
      rawContextEntries = Vector("1" -> "23", "123" -> "45", "12345" -> "678"),
      maxBudgetBytes = 3,
    ) shouldBe empty
  }

  it should "not add context map entries if one from the pair is empty" in {
    val truncatedContext =
      NonSecuritySensitiveErrorCodeComponents.truncateContext(
        rawContextEntries = rawContextMap.toVector ++ Vector(
          "" -> cValue(50),
          "k" -> "",
          // After key is cleaned, it remains empty
          "$!@" -> cValue(50),
          "" -> "",
        ),
        maxBudgetBytes = 1000,
      )

    truncatedContext should contain theSameElementsAs rawContextMap
  }

  it should "pack everything on exact size" in {
    val validContextMapGen = for {
      contextMap <- Gen.mapOfN(
        50,
        for {
          k <- asciiPrintableStrOfN(63)
          v <- asciiPrintableStrOfN(63)
        } yield (k, v),
      )
    } yield contextMap

    forAll(validContextMapGen) { map =>
      whenever(!map.exists { case (k, v) => k.isEmpty || v.isEmpty }) {
        val input = map.toVector
        NonSecuritySensitiveErrorCodeComponents.truncateContext(
          rawContextEntries = input,
          maxBudgetBytes = input.map(v => v._1.length + v._2.length).sum,
        ) should contain theSameElementsAs input
      }
    }
  }

  it should "too small entries that need to be truncated are skipped instead" in {
    NonSecuritySensitiveErrorCodeComponents.truncateContext(
      rawContextEntries = Vector("123" -> "456"),
      maxBudgetBytes = 5,
    ) shouldBe empty

    NonSecuritySensitiveErrorCodeComponents.truncateContext(
      rawContextEntries = Vector("1" -> "23", "123" -> "45", "12345" -> "678"),
      maxBudgetBytes = 8,
    ) should contain theSameElementsAs Vector("12345" -> "678")
  }

  it should "truncate bigger entries more" in {
    val inputSize = 20
    // Input with entries sorted by increasing size
    val input = (1 to inputSize).map { idx => s"k$idx".padTo(5 + idx * 2, 'a') -> cValue(idx * 2) }
    val requestedEntriesSize = input.map(v => v._1.length + v._2.length).sum
    val output =
      NonSecuritySensitiveErrorCodeComponents.truncateContext(
        rawContextEntries = input,
        maxBudgetBytes = requestedEntriesSize / 2,
      )

    def sum(s: Seq[(String, String)]) = s.map(v => v._1.length + v._2.length).sum
    def truncationPercentage(in: Seq[(String, String)], out: Seq[(String, String)]): Double =
      sum(out).toDouble / sum(in).toDouble
    def avgSize(s: Seq[(String, String)]) = sum(s).toDouble / s.size

    val (inputFirstHalf, inputSecondHalf) = input.splitAt(inputSize / 2)
    val (outputFirstHalf, outputSecondHalf) = output.splitAt(inputSize / 2)

    val truncPCSmaller = truncationPercentage(inputFirstHalf, outputFirstHalf)
    val truncPCBigger = truncationPercentage(inputSecondHalf, outputSecondHalf)

    // Check that truncation percentage is lower for the for smaller entries
    truncPCSmaller should be > truncPCBigger

    // Check that the average size still remains higher for bigger entries
    avgSize(outputFirstHalf) < avgSize(outputSecondHalf)
  }
}

private object SerializableErrorComponentsSpec {
  val rawContextMap: Map[String, String] =
    Map("key" -> cValue(10), "key-max-size".padTo(63, 'a') -> cValue(50))
  val rawErrorResources: Seq[(ErrorResource, String)] = Seq(
    ErrorResource.CommandId -> "some-command-id",
    ErrorResource.ContractKey -> "some-contract-key",
  )

  private def cValue(size: Int): String = "a" * size
  private def truncatedValue(prefix: String, size: Int): String =
    if (size < 3 + prefix.length) fail(s"size: $size")
    else s"$prefix${cValue(size - 3 - prefix.length)}..."

  private def comparable(res: Seq[(ErrorResource, String)]): Seq[(String, String)] =
    res.map { case (k, v) => k.asString -> v }
}
