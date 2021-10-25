// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.generating

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.StringReader

class DescriptorParserSpec extends AnyWordSpec with Matchers {
  "DescriptorParser" should {
    "return error when empty yaml" in {
      parseYaml("") shouldBe a[Left[_, _]]
    }
    "parse number of instances" in {
      parseYaml("""num_instances: 123""") shouldBe Right(
        ContractSetDescriptor(
          numberOfInstances = 123
        )
      )
    }
  }

  def parseYaml(
      yaml: String
  ): Either[DescriptorParser.DescriptorParserError, ContractSetDescriptor] =
    DescriptorParser.parse(new StringReader(yaml))
}
