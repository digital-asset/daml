// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import io.circe._
import io.circe.yaml.parser

import java.io.Reader

object DescriptorParser {
  import Decoders._

  def parse(reader: Reader): Either[DescriptorParserError, ContractSetDescriptor] =
    parser
      .parse(reader)
      .flatMap(_.as[ContractSetDescriptor])
      .left
      .map(error => DescriptorParserError(error.getLocalizedMessage))

  case class DescriptorParserError(details: String)

  object Decoders {
    implicit val contractDescriptionDecoder: Decoder[ContractSetDescriptor.ContractDescription] =
      Decoder.forProduct3(
        "template",
        "weight",
        "payload_size_bytes",
      )(ContractSetDescriptor.ContractDescription.apply)

    implicit val descriptorDecoder: Decoder[ContractSetDescriptor] =
      Decoder.forProduct3(
        "num_instances",
        "num_observers",
        "instance_distribution",
      )(ContractSetDescriptor.apply)
  }

}
