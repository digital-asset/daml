// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class ConcurrentHMapTest extends AnyWordSpec with BaseTest {
  "ConcurrentHMap" should {
    class HMapRelation[K, V]
    implicit val stringToInt = new HMapRelation[String, Int]
    implicit val intToBoolean = new HMapRelation[Int, Boolean]

    "behave like a mutable map" in {
      val map = ConcurrentHMap[HMapRelation]("zero" -> 0)

      // get
      map.get("zero") shouldBe Some(0)
      map.get("zerro") shouldBe None

      // putIfAbsent
      map.putIfAbsent(42, true) shouldBe None
      map.putIfAbsent(42, false) shouldBe Some(true)

      // getOrElseUpdate
      map.getOrElseUpdate("key", 1) shouldBe 1
      map.getOrElseUpdate("key", 2) shouldBe 1

      // remove
      map.remove(42) shouldBe Some(true)
      map.get(42) shouldBe None
      map.remove(42) shouldBe None
    }

    "be type safe" in {
      val map = ConcurrentHMap.empty[HMapRelation]
      map.putIfAbsent(0, true)

      // Building
      "ConcurrentHMap[HMapRelation](0 -> true)" should compile
      "ConcurrentHMap[HMapRelation](0 -> 1)" shouldNot typeCheck
      "ConcurrentHMap[HMapRelation](0.0 -> true)" shouldNot typeCheck

      // get
      "map.get(0)" should compile
      "map.get(0.0)" shouldNot typeCheck

      // getOrElseUpdate
      "map.getOrElseUpdate(0, false)" should compile
      "map.getOrElseUpdate(0, 0)" shouldNot typeCheck
      "map.getOrElseUpdate(0.0, false)" shouldNot typeCheck

      // putIfAbsent
      "map.putIfAbsent(0, false)" should compile
      "map.putIfAbsent(0, 0)" shouldNot typeCheck
      "map.putIfAbsent(0.0, false)" shouldNot typeCheck

      // remove
      "map.remove(0)" should compile
      "map.remove(0.0)" shouldNot typeCheck
    }

  }

  "ConcurrentHMap limitations" should {
    "not be typesafe if the relation is not a function" in {
      /*
        We use two evidences:
          String -> String
          String -> Double
       */

      class HMapRelation[K, V]
      implicit val stringToString = new HMapRelation[String, String]

      val map = ConcurrentHMap.empty[HMapRelation]

      {
        implicit val stringToDouble = new HMapRelation[String, Double]
        map.putIfAbsent("42", 42.0)
      }

      an[ClassCastException] should be thrownBy map.get("42").map(_.toLowerCase())
    }

    "not be typesafe if different keys are comparable" in {
      final case class Key[A](x: Int)

      class HMapRelation[K, V]
      val map = ConcurrentHMap.empty[HMapRelation]

      implicit val ev1 = new HMapRelation[Key[Int], Int]
      implicit val ev2 = new HMapRelation[Key[String], String]

      // Key[Int](42) == Key[String](42)
      map.putIfAbsent(Key[Int](42), 1)

      an[ClassCastException] should be thrownBy map.get(Key[String](42)).map(_.toLowerCase())
    }
  }

}
