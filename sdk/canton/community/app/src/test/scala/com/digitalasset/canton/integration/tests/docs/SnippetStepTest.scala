// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.docs

import com.digitalasset.canton.BaseTestWordSpec

class SnippetStepTest extends BaseTestWordSpec {

  import SnippetStep.*

  "snippet regex" should {
    "correctly match on snippet" in {
      val m = snippetKey.findAllIn(".. snippet:: test")
      m.group(1).trim shouldBe "test"
    }
    "ignore non matching" in {
      val m = snippetKey.findAllIn("blah .. snippet:: test")
      m shouldBe empty
    }
  }

  "success regex" should {
    "match correctly" when {
      "using parameters" in {
        val m = snippetSuccess.findAllIn("  .. success(nope):: command")
        m.isEmpty shouldBe false
        m.groupCount shouldBe 3
        m.group(1) shouldBe "  "
        m.group(2) shouldBe "(nope)"
        m.group(3).trim shouldBe "command"
      }
      "without parameters" in {
        val m = snippetSuccess.findAllIn("  .. success:: command")
        m.groupCount shouldBe 3
        m.group(1) shouldBe "  "
        m.group(2) shouldBe ""
        m.group(3).trim shouldBe "command"
      }
      "not get puzzled by other success mentions" in {
        val m = snippetSuccess.findAllIn("  .. success:: so mucho success::command")
        m.groupCount shouldBe 3
        m.group(1) shouldBe "  "
        m.group(2) shouldBe ""
        m.group(3).trim shouldBe "so mucho success::command"
        m.group(2) shouldBe ""
      }
      "not get puzzled by other parameters" in {
        val m = snippetSuccess.findAllIn("  .. success(pam):: so (mucho) success(noo)::command")
        m.groupCount shouldBe 3
        m.group(1) shouldBe "  "
        m.group(2) shouldBe "(pam)"
        m.group(3).trim shouldBe "so (mucho) success(noo)::command"
      }
    }
    "ignore others" when {
      "if line does not start with whitespaces" in {
        val m = snippetSuccess.findAllIn(".. success(pam):: nope")
        m.isEmpty shouldBe true
      }
    }
  }

  "failure regex" when {
    "not match at the beginning of the line" in {
      val m = snippetFailure.findAllIn(".. failure:: nope")
      m.isEmpty shouldBe true
    }
    "match correctly" in {
      val m = snippetFailure.findAllIn("  .. failure:: command")
      m.groupCount shouldBe 3
      m.group(1) shouldBe "  "
      m.group(3).trim shouldBe "command"
    }
  }

  "shell regex" when {
    "not match at the beginning of the line" in {
      val m = snippetShell.findAllIn(".. shell:: nope")
      m.isEmpty shouldBe true
    }
    "match correctly" in {
      val m = snippetShell.findAllIn("  .. shell(cwd=/run/in/this/dir):: command")
      m.groupCount shouldBe 3
      m.group(1) shouldBe "  "
      m.group(2) shouldBe "(cwd=/run/in/this/dir)"
      m.group(3).trim shouldBe "command"
    }
  }

  "scenario parser" should {
    "parse correctly" in {
      SnippetScenario.parse("""
Some text that should be ignored
.. snippet:: alpha
  .. success(output=5):: foo
  .. assert:: RES.nonEmpty

Then some text

.. snippet:: alpha
  .. failure:: bar


Some more text, followed by a multi-line step

.. snippet:: alpha
    .. success:: "a" +
      "b"
    .. success:: "c"

Then another scenario
.. snippet:: beta
  .. success:: berta

""".split("\n").toList) shouldBe Seq(
        SnippetScenario(
          "alpha",
          Seq(
            Seq(
              SnippetStep.Success("foo", Some(5), 3, "  "),
              SnippetStep.Assert("RES.nonEmpty", 4, "  "),
            ),
            Seq(SnippetStep.Failure("bar", None, 9, "  ")),
            Seq(
              SnippetStep.Success("\"a\" +\n      \"b\"", None, 15, "    "),
              SnippetStep.Success("\"c\"", None, 17, "    "),
            ),
          ),
        ),
        SnippetScenario("beta", Seq(Seq(SnippetStep.Success("berta", None, 21, "  ")))),
      )
    }

    "fail on a typo" in {
      assertThrows[IllegalArgumentException](
        SnippetScenario.parse(
          """
.. snippet:: beta
  .. snippet-success:: berta
""".split("\n").toList
        )
      )
    }
  }
}
