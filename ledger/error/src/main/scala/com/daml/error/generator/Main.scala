// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator

import io.circe.Encoder
import io.circe.syntax._

import java.nio.file.{Files, Paths, StandardOpenOption}

/** Outputs information about self-service error codes needed for generating documentation to a json file.
  */
object Main {

  case class Output(errorCodes: Seq[DocItem])

  implicit val errorCodeEncode: Encoder[DocItem] =
    Encoder.forProduct7(
      "className",
      "category",
      "hierarchicalGrouping",
      "conveyance",
      "code",
      "explanation",
      "resolution",
    )(i =>
      (
        i.className,
        i.category,
        i.hierarchicalGrouping,
        i.conveyance,
        i.code,
        i.explanation.explanation,
        i.resolution.resolution,
      )
    )

  implicit val outputEncode: Encoder[Output] =
    Encoder.forProduct1("errorCodes")(i => (i.errorCodes))

  def main(args: Array[String]): Unit = {
    val errorCodes = new ErrorCodeDocumentationGenerator().getDocItems
    val outputFile = Paths.get(args(0))
    val output = Output(errorCodes)
    val outputText: String = output.asJson.spaces2
    val outputBytes = outputText.getBytes
    val _ = Files.write(outputFile, outputBytes, StandardOpenOption.CREATE_NEW)
  }
}
