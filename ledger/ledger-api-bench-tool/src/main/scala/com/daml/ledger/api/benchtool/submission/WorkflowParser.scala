// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import io.circe._
import io.circe.yaml.parser

import java.io.Reader

object WorkflowParser {
  import Decoders._

  def parse(reader: Reader): Either[ParserError, WorkflowDescriptor] =
    parser
      .parse(reader)
      .flatMap(_.as[WorkflowDescriptor])
      .left
      .map(error => ParserError(error.getLocalizedMessage))

  case class ParserError(details: String)

  object Decoders {
    implicit val contractDescriptionDecoder: Decoder[SubmissionDescriptor.ContractDescription] =
      Decoder.forProduct4(
        "template",
        "weight",
        "payload_size_bytes",
        "archive_probability",
      )(SubmissionDescriptor.ContractDescription.apply)

    implicit val submissionDescriptorDecoder: Decoder[SubmissionDescriptor] =
      Decoder.forProduct3(
        "num_instances",
        "num_observers",
        "instance_distribution",
      )(SubmissionDescriptor.apply)

    implicit val workflowDescriptorDecoder: Decoder[WorkflowDescriptor] =
      Decoder.forProduct1(
        "submission"
      )(WorkflowDescriptor.apply)
  }

}
