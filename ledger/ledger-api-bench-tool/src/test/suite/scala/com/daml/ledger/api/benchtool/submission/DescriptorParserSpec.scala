// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import java.io.StringReader

class DescriptorParserSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {
  import ContractSetDescriptor._

  "DescriptorParser" should {
    "return error when invalid descriptor yaml provider" in {
      val cases = Table(
        ("yaml", "reason"),
        ("", "empty yaml"),
        (
          """instance_distribution:
           |num_observers: 3
           |  - template: Foo1
           |    weight: 50
           |    payload_size_bytes: 100""".stripMargin,
          "missing number of instances",
        ),
        ("""num_instances: 123""", "missing instance distribution"),
        (
          """num_instances: 123
           |num_observers: 3
           |instance_distribution:
           |  - template: Foo1
           |    payload_size_bytes: 100""".stripMargin,
          "missing weight",
        ),
        (
          """num_instances: 123
           |num_observers: 3
           |instance_distribution:
           |  - template: Foo1
           |    payload_size_bytes: 100""".stripMargin,
          "missing payload size",
        ),
        (
          """num_instances: 123
           |num_observers: 3
           |instance_distribution:
           |  - weight: 50
           |    payload_size_bytes: 100""".stripMargin,
          "missing template name",
        ),
        (
          """num_instances: 123
            |instance_distribution:
            |  - weight: 50
            |    payload_size_bytes: 100""".stripMargin,
          "missing number of observers",
        ),
      )
      forAll(cases) { case (yaml, _) =>
        parseYaml(yaml) shouldBe a[Left[_, _]]
      }
    }

    "parse descriptor" in {
      val yaml =
        """num_instances: 123
          |num_observers: 5
          |instance_distribution:
          |  - template: Foo1
          |    weight: 50
          |    payload_size_bytes: 100
          |    archive_chance: 0.9
          |  - template: Foo2
          |    weight: 25
          |    payload_size_bytes: 150
          |    archive_chance: 0.8
          |  - template: Foo3
          |    weight: 25
          |    payload_size_bytes: 30
          |    archive_chance: 0.7""".stripMargin
      parseYaml(yaml) shouldBe Right(
        ContractSetDescriptor(
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
    }
  }

  def parseYaml(
      yaml: String
  ): Either[DescriptorParser.DescriptorParserError, ContractSetDescriptor] =
    DescriptorParser.parse(new StringReader(yaml))
}
