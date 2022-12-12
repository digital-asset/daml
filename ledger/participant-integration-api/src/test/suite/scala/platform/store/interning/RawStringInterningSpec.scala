// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.interning

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RawStringInterningSpec extends AnyFlatSpec with Matchers {

  behavior of "RawStringInterning.from"

  it should "start empty" in {
    val current = RawStringInterning.from(Nil)
    current.map shouldBe empty
    current.idMap shouldBe empty
    current.lastId shouldBe 0
  }

  it should "append empty entries to existing cache" in {
    val previous = RawStringInterning(Map("one" -> 1), Map(1 -> "one"), 1)
    val current = RawStringInterning.from(Nil, previous)
    current.map shouldBe previous.map
    current.idMap shouldBe previous.idMap
    current.lastId shouldBe previous.lastId
  }

  it should "append non-empty entries to empty cache" in {
    val current = RawStringInterning.from(List(1 -> "one"))
    current.map shouldBe Map("one" -> 1)
    current.idMap shouldBe Map(1 -> "one")
    current.lastId shouldBe 1
  }

  it should "append non-empty entries to non-empty cache" in {
    val previous = RawStringInterning(
      Map("one" -> 1, "two" -> 2),
      Map(1 -> "one", 2 -> "two"),
      2,
    )
    val current = RawStringInterning.from(List(3 -> "three"), previous)
    current.map shouldBe Map("one" -> 1, "two" -> 2, "three" -> 3)
    current.idMap shouldBe Map(1 -> "one", 2 -> "two", 3 -> "three")
    current.lastId shouldBe 3
  }

  behavior of "RawStringInterning.newEntries"

  it should "return an empty result if the input and previous state is empty" in {
    val current = RawStringInterning.from(Nil)
    val newEntries = RawStringInterning.newEntries(Iterator.empty, current)
    newEntries shouldBe empty
  }

  it should "return an empty result if the input is empty" in {
    val current = RawStringInterning(Map("one" -> 1), Map(1 -> "one"), 1)
    val newEntries = RawStringInterning.newEntries(Iterator.empty, current)
    newEntries shouldBe empty
  }

  it should "return an empty result if the input only contains duplicates" in {
    val current = RawStringInterning(Map("one" -> 1), Map(1 -> "one"), 1)
    val newEntries = RawStringInterning.newEntries(List("one").iterator, current)
    newEntries shouldBe empty
  }

  it should "return a new entry if the input is an unknown string" in {
    val current = RawStringInterning(Map("one" -> 1), Map(1 -> "one"), 1)
    val newEntries = RawStringInterning.newEntries(List("two").iterator, current)
    newEntries shouldBe Vector(2 -> "two")
  }

  it should "not return a new entry for known strings" in {
    val current = RawStringInterning(Map("one" -> 1), Map(1 -> "one"), 1)
    val newEntries = RawStringInterning.newEntries(List("one", "two").iterator, current)
    newEntries shouldBe Vector(2 -> "two")
  }

  it should "handle duplicate unknown strings" in {
    val current = RawStringInterning(Map("one" -> 1), Map(1 -> "one"), 1)
    val newEntries = RawStringInterning.newEntries(List("two", "two", "two").iterator, current)
    newEntries shouldBe Vector(2 -> "two")
  }

  it should "handle mixed input" in {
    val current = RawStringInterning(Map("one" -> 1, "two" -> 2), Map(1 -> "one", 2 -> "two"), 2)
    val newEntries = RawStringInterning.newEntries(
      List("one", "three", "two", "four", "two", "four").iterator,
      current,
    )
    newEntries shouldBe Vector(3 -> "three", 4 -> "four")
  }

  it should "remove entries after the lastPersistedStringInterningId on `resetTo`" in {
    val current = RawStringInterning(Map("one" -> 1, "two" -> 2), Map(1 -> "one", 2 -> "two"), 2)
    val purgedStringInterning =
      RawStringInterning.resetTo(lastPersistedStringInterningId = 1, current)
    purgedStringInterning shouldBe RawStringInterning(Map("one" -> 1), Map(1 -> "one"), 1)
  }

  it should "not remove entries if lastPersistedStringInterningId is lteq lastId on `resetTo`" in {
    val current = RawStringInterning(Map("one" -> 1, "two" -> 2), Map(1 -> "one", 2 -> "two"), 2)
    val purgedStringInterning =
      RawStringInterning.resetTo(lastPersistedStringInterningId = 2, current)
    purgedStringInterning shouldBe current
  }
}
