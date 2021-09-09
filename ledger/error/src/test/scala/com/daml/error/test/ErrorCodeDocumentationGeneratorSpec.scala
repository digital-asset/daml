package com.daml.error.test

import com.daml.error.ErrorCategory.SystemInternalAssumptionViolated
import com.daml.error.generator.{DocItem, ErrorCodeDocumentationGenerator}
import com.daml.error.utils.testpackage.MachineFreezeError
import com.daml.error.{Explanation, Resolution}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ErrorCodeDocumentationGeneratorSpec extends AnyFlatSpec with Matchers {
  private val className = ErrorCodeDocumentationGenerator.getClass.getSimpleName
  private val generator = ErrorCodeDocumentationGenerator("com.daml.error")

  s"$className.getDocItems" should "return the correct doc items from the error classes" in {
    val actualDocItems = generator.getDocItems

    val expectedDocItems = Seq(
      DocItem(
        className = MachineFreezeError.getClass.getTypeName,
        category = SystemInternalAssumptionViolated.getClass.getSimpleName,
        hierarchicalGrouping = Nil,
        conveyance = "This error is logged with log-level ERROR on the server side.\nThis error is exposed on the API with grpc-status INTERNAL without any details due to security reasons",
        code = "BLUE_SCREEN",
        explanation = Explanation("Things happen."),
        resolution = Resolution("Turn it off and on again."),
      )
    )
    actualDocItems shouldBe expectedDocItems
  }
}
