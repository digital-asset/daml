// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package docs.src.test.suite.scala.daml.docs

import java.nio.charset.StandardCharsets

import com.daml.error.generator.{
  ErrorCategoryInventoryDocsGenerator,
  ErrorCodeInventoryDocsGenerator,
}
import org.apache.commons.io.IOUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestGeneratedErrorDocsAreUpToDate extends AnyFlatSpec with Matchers {

  private val clue =
    "Generated error docs are not up-to-date. Refresh them by running './docs/scripts/gen-error-docs-src.sh'"

  it should "generated error category inventory should be up-to-date" in {
    val actual = readFromResource("generated-error-pages/error-categories-inventory.rst.inc")
    val expected = ErrorCategoryInventoryDocsGenerator.genText()
    assert(actual == expected, clue)
  }

  it should "generated error codes inventory should be up-to-date" in {
    val actual = readFromResource("generated-error-pages/error-codes-inventory.rst.inc")
    val expected = ErrorCodeInventoryDocsGenerator.genText()
    assert(actual == expected, clue)
  }

  private def readFromResource(resourceName: String): String = {
    val stream = getClass.getClassLoader.getResourceAsStream(resourceName)
    IOUtils.toString(stream, StandardCharsets.UTF_8)
  }

}
