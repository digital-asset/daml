// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.config

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.StringReader

class WorkflowConfigParserSpec extends AnyWordSpec with Matchers {

  "WorkflowConfigParser" should {
    "parse submission configuration" in {
      val yaml =
        """submission:
        |  num_instances: 500
        |  num_observers: 4
        |  unique_parties: true
        |  instance_distribution:
        |    - template: Foo1
        |      weight: 50
        |      payload_size_bytes: 60
        |      archive_probability: 0.9
        |    - template: Foo2
        |      weight: 25
        |      payload_size_bytes: 35
        |      archive_probability: 0.8
        |    - template: Foo3
        |      weight: 10
        |      payload_size_bytes: 25
        |      archive_probability: 0.7""".stripMargin

      parseYaml(yaml) shouldBe Right(
        WorkflowConfig(
          submission = Some(
            WorkflowConfig.SubmissionConfig(
              numberOfInstances = 500,
              numberOfObservers = 4,
              uniqueParties = true,
              instanceDistribution = List(
                WorkflowConfig.SubmissionConfig.ContractDescription(
                  template = "Foo1",
                  weight = 50,
                  payloadSizeBytes = 60,
                  archiveChance = 0.9,
                ),
                WorkflowConfig.SubmissionConfig.ContractDescription(
                  template = "Foo2",
                  weight = 25,
                  payloadSizeBytes = 35,
                  archiveChance = 0.8,
                ),
                WorkflowConfig.SubmissionConfig.ContractDescription(
                  template = "Foo3",
                  weight = 10,
                  payloadSizeBytes = 25,
                  archiveChance = 0.7,
                ),
              ),
            )
          ),
          streams = Nil,
        )
      )
    }

    "parse transactions stream configuration" in {
      val yaml =
        """streams:
        |  - type: transactions
        |    name: stream-1
        |    filters:
        |      - party: Obs-2
        |        templates:
        |         - Foo1
        |         - Foo3
        |    begin_offset: foo
        |    end_offset: bar
        |    objectives:
        |      max_delay_seconds: 123
        |      min_consumption_speed: 2.34""".stripMargin
      parseYaml(yaml) shouldBe Right(
        WorkflowConfig(
          submission = None,
          streams = List(
            WorkflowConfig.StreamConfig.TransactionsStreamConfig(
              name = "stream-1",
              filters = List(
                WorkflowConfig.StreamConfig.PartyFilter(
                  party = "Obs-2",
                  templates = List("Foo1", "Foo3"),
                )
              ),
              beginOffset = Some(offset("foo")),
              endOffset = Some(offset("bar")),
              objectives = WorkflowConfig.StreamConfig.Objectives(
                maxDelaySeconds = Some(123),
                minConsumptionSpeed = Some(2.34),
              ),
            )
          ),
        )
      )
    }

    "parse transaction-trees stream configuration" in {
      val yaml =
        """streams:
          |  - type: transaction-trees
          |    name: stream-1
          |    filters:
          |      - party: Obs-2
          |        templates:
          |         - Foo1
          |         - Foo3
          |    begin_offset: foo
          |    end_offset: bar
          |    objectives:
          |      max_delay_seconds: 123
          |      min_consumption_speed: 2.34""".stripMargin
      parseYaml(yaml) shouldBe Right(
        WorkflowConfig(
          submission = None,
          streams = List(
            WorkflowConfig.StreamConfig.TransactionTreesStreamConfig(
              name = "stream-1",
              filters = List(
                WorkflowConfig.StreamConfig.PartyFilter(
                  party = "Obs-2",
                  templates = List("Foo1", "Foo3"),
                )
              ),
              beginOffset = Some(offset("foo")),
              endOffset = Some(offset("bar")),
              objectives = WorkflowConfig.StreamConfig.Objectives(
                maxDelaySeconds = Some(123),
                minConsumptionSpeed = Some(2.34),
              ),
            )
          ),
        )
      )
    }

    "parse active contracts stream configuration" in {
      val yaml =
        """streams:
          |  - type: active-contracts
          |    name: stream-1
          |    filters:
          |      - party: Obs-2
          |        templates:
          |         - Foo1
          |         - Foo3""".stripMargin
      parseYaml(yaml) shouldBe Right(
        WorkflowConfig(
          submission = None,
          streams = List(
            WorkflowConfig.StreamConfig.ActiveContractsStreamConfig(
              name = "stream-1",
              filters = List(
                WorkflowConfig.StreamConfig.PartyFilter(
                  party = "Obs-2",
                  templates = List("Foo1", "Foo3"),
                )
              ),
            )
          ),
        )
      )
    }

    "parse completions stream configuration" in {
      val yaml =
        """streams:
          |  - type: completions
          |    name: stream-1
          |    party: Obs-2
          |    begin_offset: foo
          |    application_id: foobar""".stripMargin
      parseYaml(yaml) shouldBe Right(
        WorkflowConfig(
          submission = None,
          streams = List(
            WorkflowConfig.StreamConfig.CompletionsStreamConfig(
              name = "stream-1",
              party = "Obs-2",
              beginOffset = Some(offset("foo")),
              applicationId = "foobar",
            )
          ),
        )
      )
    }
  }

  def parseYaml(yaml: String): Either[WorkflowConfigParser.ParserError, WorkflowConfig] =
    WorkflowConfigParser.parse(new StringReader(yaml))

  def offset(str: String): LedgerOffset = LedgerOffset.defaultInstance.withAbsolute(str)

}
