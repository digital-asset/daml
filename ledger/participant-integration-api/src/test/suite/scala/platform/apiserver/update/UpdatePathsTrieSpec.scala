// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

import com.daml.error.ErrorsAssertions
import org.scalatest.{EitherValues, OptionValues}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import com.daml.platform.apiserver.update.UpdatePathModifier._

class UpdatePathsTrieSpec
    extends AnyFreeSpec
    with Matchers
    with EitherValues
    with OptionValues
    with ErrorsAssertions {

  "finding update paths in" - {
    val allPaths = UpdatePath
      .parseAll(
        Seq(
          "a1.a2.c3",
          "a1.b2.c3",
          "a1.b2.d3.a4",
          "a1.b2.d3.b4",
          "b1",
        )
      )
      .value
    val tree = UpdatePathsTrie.fromPaths(allPaths).value

    "proper subtrees" in {
      tree.findPath(List("a1")) shouldBe None
      tree.findPath(List.empty) shouldBe None
      tree.findPath(List("a1", "b2")) shouldBe None
    }
    "non-existing subtrees" in {
      tree.findPath(List("a1", "b2", "dummy")) shouldBe None
      tree.findPath(List("dummy")) shouldBe None
      tree.findPath(List("")) shouldBe None
    }
    "existing but empty subtrees" in {
      tree.findPath(List("b1")) shouldBe Some(UpdatePathModifier.NoModifier)
      tree.findPath(List("a1", "b2", "c3")) shouldBe Some(UpdatePathModifier.NoModifier)
      tree.findPath(List("a1", "b2", "d3", "b4")) shouldBe Some(UpdatePathModifier.NoModifier)
    }
  }

  "constructing a trie" - {
    "from one path with one segment" in {
      UpdatePathsTrie
        .fromPaths(
          UpdatePath.parseAll(Seq("foo")).value
        )
        .value shouldBe UpdatePathsTrie(
        None,
        "foo" -> UpdatePathsTrie(Some(NoModifier)),
      )
    }
    "from one path with multiple segments" in {
      UpdatePathsTrie
        .fromPaths(
          UpdatePath.parseAll(Seq("foo.bar.baz")).value
        )
        .value shouldBe UpdatePathsTrie(
        None,
        "foo" -> UpdatePathsTrie(
          None,
          "bar" -> UpdatePathsTrie(
            None,
            "baz" -> UpdatePathsTrie(
              Some(NoModifier)
            ),
          ),
        ),
      )
    }
    "from three paths with multiple segments and with update modifiers" in {
      val t = UpdatePathsTrie
        .fromPaths(
          UpdatePath
            .parseAll(
              Seq(
                "foo.bar.baz",
                "foo.bar!merge",
                "foo.alice",
                "bob.eve",
                "bob!replace",
              )
            )
            .value
        )
        .value
      t shouldBe UpdatePathsTrie(
        None,
        "foo" -> UpdatePathsTrie(
          None,
          "bar" -> UpdatePathsTrie(
            Some(Merge),
            "baz" -> UpdatePathsTrie(Some(NoModifier)),
          ),
          "alice" -> UpdatePathsTrie(Some(NoModifier)),
        ),
        "bob" -> UpdatePathsTrie(
          Some(Replace),
          "eve" -> UpdatePathsTrie(Some(NoModifier)),
        ),
      )
    }

  }

  "checking for presence of a prefix" in {
    val t = UpdatePathsTrie
      .fromPaths(
        UpdatePath
          .parseAll(
            Seq(
              "foo.bar.baz",
              "foo.bax",
            )
          )
          .value
      )
      .value
    t.containsPrefix(List("foo")) shouldBe true
    t.containsPrefix(List("foo", "bar")) shouldBe true
    t.containsPrefix(List("foo", "bax")) shouldBe true
    t.containsPrefix(List("foo", "bar", "baz")) shouldBe true
    t.containsPrefix(List("foo", "bar", "bad")) shouldBe false
    t.containsPrefix(List("")) shouldBe false
    t.containsPrefix(List.empty) shouldBe true

  }

  "fail to build a trie when duplicated field paths" in {
    UpdatePathsTrie
      .fromPaths(
        UpdatePath
          .parseAll(
            Seq(
              "foo.bar",
              "foo.bar!merge",
            )
          )
          .value
      )
      .left
      .value shouldBe UpdatePathError.DuplicatedFieldPath("foo.bar!merge")
  }

}
