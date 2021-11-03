// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator

import com.daml.error.utils.testpackage.subpackage.NotSoSeriousError
import com.daml.error.utils.testpackage.{DeprecatedError, SeriousError}
import com.daml.error.{Deprecation, Explanation, Resolution}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.nowarn

@nowarn("msg=deprecated")
class ErrorCodeDocumentationGeneratorSpec extends AnyFlatSpec with Matchers {
  private val className = ErrorCodeDocumentationGenerator.getClass.getSimpleName

  // Scan errors from the utils test package
  private val generator = new ErrorCodeDocumentationGenerator(
    Array("com.daml.error.utils.testpackage")
  )

  s"$className.getDocItems" should "return the correct doc items from the error classes" in {
    val actualDocItems = generator.getDocItems

    val expectedDocItems = Seq(
      DocItem(
        className = SeriousError.getClass.getTypeName,
        category = "SystemInternalAssumptionViolated",
        hierarchicalGrouping = Nil,
        conveyance =
          "This error is logged with log-level ERROR on the server side.\nThis error is exposed on the API with grpc-status INTERNAL without any details due to security reasons",
        code = "BLUE_SCREEN",
        deprecation = Deprecation(""),
        explanation = Explanation("Things happen."),
        resolution = Resolution("Turn it off and on again."),
      ),
      DocItem(
        className = DeprecatedError.getClass.getTypeName,
        category = "SystemInternalAssumptionViolated",
        hierarchicalGrouping = Nil,
        conveyance =
          "This error is logged with log-level ERROR on the server side.\nThis error is exposed on the API with grpc-status INTERNAL without any details due to security reasons",
        code = "DEPRECATED_ERROR",
        deprecation = Deprecation("deprecated."),
        explanation = Explanation("Things happen."),
        resolution = Resolution("Turn it off and on again."),
      ),
      DocItem(
        className = NotSoSeriousError.getClass.getTypeName,
        category = "TransientServerFailure",
        hierarchicalGrouping = "Some error class" :: Nil,
        conveyance =
          "This error is logged with log-level INFO on the server side.\nThis error is exposed on the API with grpc-status UNAVAILABLE including a detailed error message",
        code = "TEST_ROUTINE_FAILURE_PLEASE_IGNORE",
        deprecation = Deprecation(""),
        explanation = Explanation("Test: Things like this always happen."),
        resolution = Resolution("Test: Why not ignore?"),
      ),
    )

    actualDocItems shouldBe expectedDocItems
  }
}
