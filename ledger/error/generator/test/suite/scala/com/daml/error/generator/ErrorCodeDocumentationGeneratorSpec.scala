// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator

import com.daml.error.ErrorCategory.TransientServerFailure
import com.daml.error.utils.testpackage.subpackage.MildErrorsParent
import com.daml.error.utils.testpackage.subpackage.MildErrorsParent.MildErrors
import com.daml.error.utils.testpackage.subpackage.MildErrorsParent.MildErrors.NotSoSeriousError
import com.daml.error.utils.testpackage.{DeprecatedError, SeriousError}
import com.daml.error._
import com.daml.error.generator.ErrorCodeDocumentationGenerator.DeprecatedItem
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.nowarn
import scala.reflect.ClassTag

class ErrorCodeDocumentationGeneratorSpec extends AnyFlatSpec with Matchers {

  it should "return the correct doc items from the error classes" in {
    val searchPackages = Array("com.daml.error.utils.testpackage")
    val actualGroupDocItems = ErrorCodeDocumentationGenerator.getErrorGroupItems(searchPackages)
    val actualErrorDocItems = ErrorCodeDocumentationGenerator.getErrorCodeItems(searchPackages)

    val expectedErrorDocItems = Seq(
      ErrorCodeDocItem(
        errorCodeClassName = SeriousError.getClass.getTypeName,
        category = "SystemInternalAssumptionViolated",
        hierarchicalGrouping = ErrorClass(Nil),
        conveyance = Some(
          "This error is logged with log-level ERROR on the server side. It is exposed on the API with grpc-status INTERNAL without any details for security reasons."
        ),
        code = "BLUE_SCREEN",
        deprecation = None,
        explanation = Some(Explanation("Things happen.")),
        resolution = Some(Resolution("Turn it off and on again.")),
      ),
      ErrorCodeDocItem(
        errorCodeClassName = DeprecatedError.getClass.getTypeName: @nowarn("cat=deprecation"),
        category = "SystemInternalAssumptionViolated",
        hierarchicalGrouping = ErrorClass(Nil),
        conveyance = Some(
          "This error is logged with log-level ERROR on the server side. It is exposed on the API with grpc-status INTERNAL without any details for security reasons."
        ),
        code = "DEPRECATED_ERROR",
        deprecation =
          Some(DeprecatedItem(since = Some("since now"), message = "This is deprecated")),
        explanation = Some(Explanation("Things happen.")),
        resolution = Some(Resolution("Turn it off and on again.")),
      ),
      ErrorCodeDocItem(
        errorCodeClassName = NotSoSeriousError.getClass.getTypeName,
        category = "TransientServerFailure",
        hierarchicalGrouping = ErrorClass(
          List(
            Grouping("MildErrorsParent", MildErrorsParent.getClass.getName),
            Grouping("MildErrors", MildErrors.getClass.getName),
          )
        ),
        conveyance = Some(
          "This error is logged with log-level INFO on the server side and exposed on the API with grpc-status UNAVAILABLE including a detailed error message."
        ),
        code = "TEST_ROUTINE_FAILURE_PLEASE_IGNORE",
        deprecation = None,
        explanation = Some(Explanation("Test: Things like this always happen.")),
        resolution = Some(Resolution("Test: Why not ignore?")),
      ),
    )

    val expectedGroupDocItems = Seq(
      ErrorGroupDocItem(
        className = MildErrorsParent.getClass.getName,
        explanation = Some(Explanation("Mild error parent explanation")),
        errorClass = ErrorClass(
          Grouping(
            docName = "MildErrorsParent",
            fullClassName = MildErrorsParent.getClass.getName,
          ) :: Nil
        ),
      ),
      ErrorGroupDocItem(
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
    val actual = ErrorCodeDocumentationGenerator.getErrorCategoryItem(TransientServerFailure)

    actual.resolution should not be (empty)
    actual.description should not be (empty)
    actual.retryStrategy should not be (empty)
  }

  @deprecated(since = "since 123", message = "message 123") object Foo1
  @deprecated(message = "message 123") object Foo2
  @deprecated(since = "since 123") object Foo3
  @deprecated("message 123", "since 123") object Foo4

  it should "parse annotations" in {
    import scala.reflect.runtime.{universe => ru}

    def getFirstAnnotation[T: ClassTag](obj: T): ru.Annotation = {
      ru.runtimeMirror(getClass.getClassLoader).reflect(obj).symbol.annotations.head
    }

    ErrorCodeDocumentationGenerator.parseScalaDeprecatedAnnotation(
      getFirstAnnotation(Foo1): @nowarn("cat=deprecation")
    ) shouldBe DeprecatedItem(
      since = Some("since 123"),
      message = "message 123",
    )
    ErrorCodeDocumentationGenerator.parseScalaDeprecatedAnnotation(
      getFirstAnnotation(Foo2): @nowarn("cat=deprecation")
    ) shouldBe DeprecatedItem(
      since = None,
      message = "message 123",
    )
    ErrorCodeDocumentationGenerator.parseScalaDeprecatedAnnotation(
      getFirstAnnotation(Foo3): @nowarn("cat=deprecation")
    ) shouldBe DeprecatedItem(
      since = Some("since 123"),
      message = "",
    )
    ErrorCodeDocumentationGenerator.parseScalaDeprecatedAnnotation(
      getFirstAnnotation(Foo4): @nowarn("cat=deprecation")
    ) shouldBe DeprecatedItem(
      since = Some("since 123"),
      message = "message 123",
    )
  }
}
