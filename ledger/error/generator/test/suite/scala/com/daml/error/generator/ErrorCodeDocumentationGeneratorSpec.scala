// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator

import com.daml.error.ErrorCategory.TransientServerFailure
import com.daml.error.utils.testpackage.subpackage.MildErrorsParent
import com.daml.error.utils.testpackage.subpackage.MildErrorsParent.MildErrors
import com.daml.error.utils.testpackage.subpackage.MildErrorsParent.MildErrors.NotSoSeriousError
import com.daml.error.utils.testpackage.{DeprecatedError, SeriousError}
import com.daml.error._
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
    val (actualErrorDocItems, actualGroupDocItems) = generator.getDocItems

    val expectedErrorDocItems = Seq(
      ErrorDocItem(
        className = SeriousError.getClass.getTypeName,
        category = "SystemInternalAssumptionViolated",
        hierarchicalGrouping = ErrorClass(Nil),
        conveyance = Some(
          "This error is logged with log-level ERROR on the server side.\n" +
            "This error is exposed on the API with grpc-status INTERNAL without any details due to security reasons"
        ),
        code = "BLUE_SCREEN",
        deprecation = None,
        explanation = Some(Explanation("Things happen.")),
        resolution = Some(Resolution("Turn it off and on again.")),
      ),
      ErrorDocItem(
        className = DeprecatedError.getClass.getTypeName,
        category = "SystemInternalAssumptionViolated",
        hierarchicalGrouping = ErrorClass(Nil),
        conveyance = Some(
          "This error is logged with log-level ERROR on the server side.\n" +
            "This error is exposed on the API with grpc-status INTERNAL without any details due to security reasons"
        ),
        code = "DEPRECATED_ERROR",
        deprecation = Some(DeprecatedDocs("deprecated.")),
        explanation = Some(Explanation("Things happen.")),
        resolution = Some(Resolution("Turn it off and on again.")),
      ),
      ErrorDocItem(
        className = NotSoSeriousError.getClass.getTypeName,
        category = "TransientServerFailure",
        hierarchicalGrouping = ErrorClass(
          List(
            Grouping("MildErrorsParent", MildErrorsParent.getClass.getName),
            Grouping("MildErrors", MildErrors.getClass.getName),
          )
        ),
        conveyance = Some(
          "This error is logged with log-level INFO on the server side.\n" +
            "This error is exposed on the API with grpc-status UNAVAILABLE including a detailed error message"
        ),
        code = "TEST_ROUTINE_FAILURE_PLEASE_IGNORE",
        deprecation = None,
        explanation = Some(Explanation("Test: Things like this always happen.")),
        resolution = Some(Resolution("Test: Why not ignore?")),
      ),
    )

    val expectedGroupDocItems = Seq(
      GroupDocItem(
        className = MildErrorsParent.getClass.getName,
        explanation = Some(Explanation("Mild error parent explanation")),
        errorClass = ErrorClass(
          Grouping(
            docName = "MildErrorsParent",
            fullClassName = MildErrorsParent.getClass.getName,
          ) :: Nil
        ),
      ),
      GroupDocItem(
        className = MildErrorsParent.MildErrors.getClass.getName,
        explanation = Some(Explanation("Groups mild errors together")),
        errorClass = ErrorClass(
          Grouping(
            docName = "MildErrorsParent",
            fullClassName = MildErrorsParent.getClass.getName,
          ) ::
            Grouping(
              docName = "MildErrors",
              fullClassName = MildErrorsParent.MildErrors.getClass.getName,
            ) :: Nil
        ),
      ),
    )

    actualErrorDocItems should have length (3)
    actualErrorDocItems(0) shouldBe expectedErrorDocItems(0)
    actualErrorDocItems(1) shouldBe expectedErrorDocItems(1)
    actualErrorDocItems(2) shouldBe expectedErrorDocItems(2)

    actualGroupDocItems should have length (2)
    actualGroupDocItems(0) shouldBe expectedGroupDocItems(0)
    actualGroupDocItems(1) shouldBe expectedGroupDocItems(1)
  }

  it should "parse annotations of an error category" in {
    val actual = ErrorCodeDocumentationGenerator.getErrorCategoryAnnotations(TransientServerFailure)

    actual.resolution should not be (empty)
    actual.description should not be (empty)
    actual.retryStrategy should not be (empty)
  }
}
