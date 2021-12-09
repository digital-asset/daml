// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator.app

import java.nio.file.{Files, Paths, StandardOpenOption}

import com.daml.error.ErrorCategory
import com.daml.error.generator.ErrorCodeDocumentationGenerator

/** Generates error categories inventory as a reStructuredText
  */
object ErrorCategoryInventoryDocsGenApp {

  def main(args: Array[String]): Unit = {
    val outputText = collectErrorCodesAsReStructuredTextSubsections().mkString("\n\n\n")
    if (args.length >= 1) {
      val outputFile = Paths.get(args(0))
      Files.write(outputFile, outputText.getBytes, StandardOpenOption.CREATE_NEW): Unit
    } else {
      println(outputText)
    }
  }

  def collectErrorCodesAsReStructuredTextSubsections(): Seq[String] = {
    ErrorCategory.all.map { errorCategory: ErrorCategory =>
      val annotations = ErrorCodeDocumentationGenerator.getErrorCategoryAnnotations(errorCategory)

      val categoryId: String = errorCategory.asInt.toString
      val grpcCode: String = errorCategory.grpcCode.fold("N/A")(_.toString)
      val name: String = errorCategory.getClass.getSimpleName.replace("$", "")
      val logLevel: String = errorCategory.logLevel.toString
      val description: String = annotations.description.getOrElse("").replace("\n", " ")
      val resolution: String = annotations.resolution.getOrElse("").replace("\n", " ")
      val retryStrategy: String = annotations.retryStrategy.getOrElse("").replace("\n", " ")

      s"""${name}
         |------------------------------------------------------------------------------------------------------------------
         |    **Category id**: ${categoryId}
         |
         |    **gRPC status code**: ${grpcCode}
         |
         |    **Default log level**: ${logLevel}
         |
         |    **Description**: ${description}
         |
         |    **Resolution**: ${resolution}
         |
         |    **Retry strategy**: ${retryStrategy}""".stripMargin
    }
  }

}
