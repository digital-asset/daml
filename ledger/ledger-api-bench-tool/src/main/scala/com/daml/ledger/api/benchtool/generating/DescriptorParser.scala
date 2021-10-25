// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.generating

import io.circe._
import io.circe.yaml.parser

import java.io.Reader

object DescriptorParser {

  def parse(reader: Reader): Either[DescriptorParserError, ContractSetDescriptor] =
    parser
      .parse(reader)
      .flatMap(_.as[ContractSetDescriptor])
      .left
      .map(error => DescriptorParserError(error.getLocalizedMessage))

  implicit val decoder: Decoder[ContractSetDescriptor] =
    Decoder.forProduct1("num_instances")(ContractSetDescriptor.apply)

  case class DescriptorParserError(details: String)

}
