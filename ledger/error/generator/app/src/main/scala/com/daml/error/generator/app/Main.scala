// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator.app

import com.daml.error.Grouping
import com.daml.error.generator.{ErrorCodeDocumentationGenerator, ErrorDocItem, GroupDocItem}
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
        i.fullClassName,
      )
    )

  implicit val errorCodeEncode: Encoder[ErrorDocItem] =
    Encoder.forProduct8(
      "className",
      "category",
      "hierarchicalGrouping",
      "conveyance",
      "code",
      "deprecation",
      "explanation",
      "resolution",
    )(i =>
      (
        i.className,
        i.category,
        i.hierarchicalGrouping.groupings,
        i.conveyance,
        i.code,
        i.deprecation.fold("")(_.deprecation),
        i.explanation.fold("")(_.explanation),
        i.resolution.fold("")(_.resolution),
      )
    )

  implicit val groupEncode: Encoder[GroupDocItem] =
    Encoder.forProduct2(
      "className",
      "explanation",
    )(i =>
      (
        i.className,
        i.explanation.fold("")(_.explanation),
      )
    )

  implicit val outputEncode: Encoder[Output] =
    Encoder.forProduct2("errorCodes", "groups")(i => (i.errorCodes, i.groups))

  def main(args: Array[String]): Unit = {
    val (errorCodes, groups) = new ErrorCodeDocumentationGenerator().getDocItems
    val output = Output(errorCodes, groups)
    val outputText: String = output.asJson.spaces2

    if (args.length >= 1) {
      val outputFile = Paths.get(args(0))
      val _ = Files.write(outputFile, outputText.getBytes, StandardOpenOption.CREATE_NEW)
    } else {
      println(outputText)
    }

  }
}
