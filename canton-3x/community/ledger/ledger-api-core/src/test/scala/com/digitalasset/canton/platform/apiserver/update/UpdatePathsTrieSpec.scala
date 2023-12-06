// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.update

import com.daml.error.ErrorsAssertions
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, OptionValues}

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
      tree.pathExists(List("a1")) shouldBe false
      tree.pathExists(List.empty) shouldBe false
      tree.pathExists(List("a1", "b2")) shouldBe false
    }
    "non-existing subtrees" in {
      tree.pathExists(List("a1", "b2", "dummy")) shouldBe false
      tree.pathExists(List("dummy")) shouldBe false
      tree.pathExists(List("")) shouldBe false
    }
    "existing but empty subtrees" in {
      tree.pathExists(List("b1")) shouldBe true
      tree.pathExists(List("a1", "b2", "c3")) shouldBe true
      tree.pathExists(List("a1", "b2", "d3", "b4")) shouldBe true
    }
  }

  "constructing a trie" - {
    "from one path with one segment" in {
      UpdatePathsTrie
        .fromPaths(
          UpdatePath.parseAll(Seq("foo")).value
        )
        .value shouldBe UpdatePathsTrie(
        exists = false,
        "foo" -> UpdatePathsTrie(exists = true),
      )
    }
    "from one path with multiple segments" in {
      UpdatePathsTrie
        .fromPaths(
          UpdatePath.parseAll(Seq("foo.bar.baz")).value
        )
        .value shouldBe UpdatePathsTrie(
        exists = false,
        "foo" -> UpdatePathsTrie(
          exists = false,
          "bar" -> UpdatePathsTrie(
            exists = false,
            "baz" -> UpdatePathsTrie(
              exists = true
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
                "foo.bar",
                "foo.alice",
                "bob.eve",
                "bob",
              )
            )
            .value
        )
        .value
      t shouldBe UpdatePathsTrie(
        exists = false,
        "foo" -> UpdatePathsTrie(
          exists = false,
          "bar" -> UpdatePathsTrie(
            exists = true,
            "baz" -> UpdatePathsTrie(exists = true),
          ),
          "alice" -> UpdatePathsTrie(exists = true),
        ),
        "bob" -> UpdatePathsTrie(
          exists = true,
          "eve" -> UpdatePathsTrie(exists = true),
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
              "foo.bar",
            )
          )
          .value
      )
      .left
      .value shouldBe UpdatePathError.DuplicatedFieldPath("foo.bar")
  }

}
