// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator

import com.daml.error.ErrorCategory.{
  BackgroundProcessDegradationWarning,
  SystemInternalAssumptionViolated,
}
import com.daml.error.utils.testpackage.SeriousError
import com.daml.error.utils.testpackage.subpackage.NotSoSeriousError
import com.daml.error.{Explanation, Resolution}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ErrorCodeDocumentationGeneratorSpec extends AnyFlatSpec with Matchers {
  private val className = ErrorCodeDocumentationGenerator.getClass.getSimpleName

  // Scan errors from the utils test package
  private val generator = ErrorCodeDocumentationGenerator("com.daml.error.utils.testpackage")

  s"$className.getDocItems" should "return the correct doc items from the error classes" in {
    val actualDocItems = generator.getDocItems

    val expectedDocItems = Seq(
      DocItem(
        className = SeriousError.getClass.getTypeName,
        category = SystemInternalAssumptionViolated.getClass.getSimpleName,
        hierarchicalGrouping = Nil,
        conveyance =
          "This error is logged with log-level ERROR on the server side.\nThis error is exposed on the API with grpc-status INTERNAL without any details due to security reasons",
        code = "BLUE_SCREEN",
        explanation = Explanation("Things happen."),
        resolution = Resolution("Turn it off and on again."),
      ),
      DocItem(
        className = NotSoSeriousError.getClass.getTypeName,
        category = BackgroundProcessDegradationWarning.getClass.getSimpleName,
        hierarchicalGrouping = "Some error class" :: Nil,
        conveyance = "This error is logged with log-level WARN on the server side.",
        code = "TEST_ROUTINE_FAILURE_PLEASE_IGNORE",
        explanation = Explanation("Test: Things like this always happen."),
        resolution = Resolution("Test: Why not ignore?"),
      ),
    )

    actualDocItems shouldBe expectedDocItems
  }
}
