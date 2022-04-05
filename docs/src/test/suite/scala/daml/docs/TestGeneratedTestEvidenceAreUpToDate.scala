// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package docs.src.test.suite.scala.daml.docs

import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestGeneratedTestEvidenceAreUpToDate extends AnyFlatSpec with Matchers {

  private val clue =
    "Generated error docs are not up-to-date. Refresh them by running './docs/scripts/gen-test-evidence.sh'"

  it should "generate error " in {
    val actual = readFromResource("generated-test-evidence/security-test-evidence.csv")
    val expected = com.daml.test.evidence.generator.SecurityTestEvidenceMarkdownGenerator.genText()
    assert(actual == expected, clue)
  }

  private def readFromResource(resourceName: String): String = {
    val stream = getClass.getClassLoader.getResourceAsStream(resourceName)
    IOUtils.toString(stream, StandardCharsets.UTF_8)
  }

}
