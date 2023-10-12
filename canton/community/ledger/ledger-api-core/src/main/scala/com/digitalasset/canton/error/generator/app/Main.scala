// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error.generator.app

import com.daml.error.Grouping
import com.digitalasset.canton.error.generator.ErrorCodeDocumentationGenerator.DeprecatedItem
import com.digitalasset.canton.error.generator.{
  ErrorCodeDocItem,
  ErrorCodeDocumentationGenerator,
  ErrorGroupDocItem,
}
import io.circe.Encoder
import io.circe.syntax.*

import java.nio.file.{Files, Paths, StandardOpenOption}

/** Outputs information about self-service error codes needed for generating documentation to a json file.
  */
object Main {

  final case class Output(errorCodes: Seq[ErrorCodeDocItem], groups: Seq[ErrorGroupDocItem])

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

  implicit val deprecatedEncode: Encoder[DeprecatedItem] = {
    Encoder.forProduct2(
      "message",
      "since",
    )(i => (i.message, i.since))
  }

  implicit val errorCodeEncode: Encoder[ErrorCodeDocItem] =
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
        i.errorCodeClassName,
        i.category,
        i.hierarchicalGrouping.groupings,
        i.conveyance,
        i.code,
        i.deprecation,
        i.explanation.fold("")(_.explanation),
        i.resolution.fold("")(_.resolution),
      )
    )

  implicit val groupEncode: Encoder[ErrorGroupDocItem] =
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
    val errorCodes = ErrorCodeDocumentationGenerator.getErrorCodeItems()
    val groups = ErrorCodeDocumentationGenerator.getErrorGroupItems()
    val output = Output(errorCodes, groups)
    val outputText: String = output.asJson.spaces2

    if (args.length >= 1) {
      val outputFile = Paths.get(args(0))
      Files.write(outputFile, outputText.getBytes, StandardOpenOption.CREATE_NEW): Unit
    } else {
      println(outputText)
    }

  }
}
