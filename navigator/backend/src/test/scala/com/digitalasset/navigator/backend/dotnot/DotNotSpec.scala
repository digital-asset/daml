// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.dotnot

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DotNotSpec extends AnyFlatSpec with Matchers {

  behavior of "DotNot"

  it should "register an action on leaf values" in {
    val tree =
      root[String, Int, Unit]("foobar")
        .onLeaf("foo")
        .onAnyValue
        .const(1)

    tree.onTree.nameMatcherToActions should have size 1
    tree.valueMatcherToActions should have size 1
    tree.run("", PropertyCursor.fromString("foo"), "", ()) shouldEqual Right(1)
  }

  it should "register multiple actions on leaf values" in {
    val tree =
      root[String, Int, Unit]("foobar")
        .onLeaf("foo")
        .onValue("*")
        .const(1)
        .onValue("2")
        .const(2)
        .onAnyValue
        .const(3)

    val cursor = PropertyCursor.fromString("foo")
    tree.onTree.nameMatcherToActions should have size 1
    tree.valueMatcherToActions should have size 3
    tree.run("", cursor, "*", ()) shouldEqual Right(1)
    tree.run("", cursor, "2", ()) shouldEqual Right(2)
    tree.run("", cursor, "", ()) shouldEqual Right(3)
    val wrongCursor = PropertyCursor.fromString("bar")
    tree.run("", wrongCursor, "", ()) shouldBe a[Left[_, _]]
  }
}
