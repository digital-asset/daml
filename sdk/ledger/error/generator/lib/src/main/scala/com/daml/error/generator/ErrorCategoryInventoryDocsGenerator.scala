// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator

import com.daml.error.ErrorCategory

object ErrorCategoryInventoryDocsGenerator {

  def genText(): String = {
    collectErrorCodesAsReStructuredTextSubsections().mkString("\n\n\n")
  }

  private def collectErrorCodesAsReStructuredTextSubsections(): Seq[String] = {
    ErrorCategory.all.map { errorCategory: ErrorCategory =>
      val annotations = ErrorCodeDocumentationGenerator.getErrorCategoryItem(errorCategory)

      val categoryId: String = errorCategory.asInt.toString
      val grpcCode: String = errorCategory.grpcCode.fold("N/A")(_.toString)
      val name: String = errorCategory.getClass.getSimpleName.replace("$", "")
      val logLevel: String = errorCategory.logLevel.toString
      val description: String = annotations.description.getOrElse("").replace("\n", " ")
      val resolution: String = annotations.resolution.getOrElse("").replace("\n", " ")
      val retryStrategy: String = annotations.retryStrategy.getOrElse("").replace("\n", " ")

      s"""${name}
         |${"=" * 120}
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
