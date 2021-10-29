// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import java.io.StringReader

class WorkflowParserSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {
  import SubmissionDescriptor._

  "DescriptorParser" should {
    "parse submission description" in {
      val yaml =
        """submission:
          |  num_instances: 123
          |  num_observers: 5
          |  instance_distribution:
          |    - template: Foo1
          |      weight: 50
          |      payload_size_bytes: 100
          |      archive_probability: 0.9
          |    - template: Foo2
          |      weight: 25
          |      payload_size_bytes: 150
          |      archive_probability: 0.8
          |    - template: Foo3
          |      weight: 25
          |      payload_size_bytes: 30
          |      archive_probability: 0.7""".stripMargin
      parseYaml(yaml) shouldBe Right(
        WorkflowDescriptor(
          submission = SubmissionDescriptor(
            numberOfInstances = 123,
            numberOfObservers = 5,
            instanceDistribution = List(
              ContractDescription(
                template = "Foo1",
                weight = 50,
                payloadSizeBytes = 100,
                archiveChance = 0.9,
              ),
              ContractDescription(
                template = "Foo2",
                weight = 25,
                payloadSizeBytes = 150,
                archiveChance = 0.8,
              ),
              ContractDescription(
                template = "Foo3",
                weight = 25,
                payloadSizeBytes = 30,
                archiveChance = 0.7,
              ),
            ),
          )
        )
      )
    }
  }

  def parseYaml(
      yaml: String
  ): Either[WorkflowParser.ParserError, WorkflowDescriptor] =
    WorkflowParser.parse(new StringReader(yaml))
}
