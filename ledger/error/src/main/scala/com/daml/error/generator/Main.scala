// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator

import com.daml.error.Grouping
import io.circe.Encoder
import io.circe.syntax._

import java.nio.file.{Files, Paths, StandardOpenOption}

/** Outputs information about self-service error codes needed for generating documentation to a json file.
  */
object Main {

  case class Output(errorCodes: Seq[ErrorDocItem], groups: Seq[GroupDocItem])

  implicit val groupingEncode: Encoder[Grouping] =
    Encoder.forProduct2(
      "docName",
      "className",
    )(i =>
      (
        i.docName,
        i.group.map(_.fullClassName),
      )
    )

  implicit val errorCodeEncode: Encoder[ErrorDocItem] =
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

  implicit val groupEncode: Encoder[GroupDocItem] =
    Encoder.forProduct2(
      "className",
      "explanation",
    )(i =>
      (
        i.className,
        i.explanation.explanation,
      )
    )

  implicit val outputEncode: Encoder[Output] =
    Encoder.forProduct2("errorCodes", "groups")(i => (i.errorCodes, i.groups))

  def main(args: Array[String]): Unit = {
    val (errorCodes, groups) = new ErrorCodeDocumentationGenerator().getDocItems
    val outputFile = Paths.get(args(0))
    val output = Output(errorCodes, groups)
    val outputText: String = output.asJson.spaces2
    val outputBytes = outputText.getBytes
    val _ = Files.write(outputFile, outputBytes, StandardOpenOption.CREATE_NEW)
  }
}
