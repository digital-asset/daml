// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.config

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.StringReader

class WorkflowConfigParserSpec extends AnyWordSpec with Matchers {

  "WorkflowConfigParser" should {
    "parse complete workflow configuration" in {
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
          |streams:
          |  - type: active-contracts
          |    name: stream-1
          |    filters:
          |      - party: Obs-2
          |        templates:
          |         - Foo1
          |         - Foo3
          |    objectives:
          |      min_item_rate: 123
          |      max_item_rate: 456""".stripMargin

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
                )
              ),
            )
          ),
          streams = List(
            WorkflowConfig.StreamConfig.ActiveContractsStreamConfig(
              name = "stream-1",
              filters = List(
                WorkflowConfig.StreamConfig.PartyFilter(
                  party = "Obs-2",
                  templates = List("Foo1", "Foo3"),
                )
              ),
              objectives = Some(
                WorkflowConfig.StreamConfig.RateObjectives(
                  minItemRate = Some(123),
                  maxItemRate = Some(456),
                )
              ),
            )
          ),
        )
      )
    }

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
        |      min_consumption_speed: 2.34
        |      min_item_rate: 12
        |      max_item_rate: 34""".stripMargin
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
              objectives = Some(
                WorkflowConfig.StreamConfig.TransactionObjectives(
                  maxDelaySeconds = Some(123),
                  minConsumptionSpeed = Some(2.34),
                  minItemRate = Some(12),
                  maxItemRate = Some(34),
                )
              ),
            )
          ),
        )
      )
    }

    "parse stream configuration with some objectives set" in {
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
          |      min_consumption_speed: 2.34
          |      min_item_rate: 12""".stripMargin
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
              objectives = Some(
                WorkflowConfig.StreamConfig.TransactionObjectives(
                  maxDelaySeconds = None,
                  minConsumptionSpeed = Some(2.34),
                  minItemRate = Some(12),
                  maxItemRate = None,
                )
              ),
            )
          ),
        )
      )
    }

    "parse stream configuration without objectives" in {
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
          |    end_offset: bar""".stripMargin
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
              objectives = None,
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
          |      min_consumption_speed: 2.34
          |      min_item_rate: 12
          |      max_item_rate: 34""".stripMargin
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
              objectives = Some(
                WorkflowConfig.StreamConfig.TransactionObjectives(
                  maxDelaySeconds = Some(123),
                  minConsumptionSpeed = Some(2.34),
                  minItemRate = Some(12),
                  maxItemRate = Some(34),
                )
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
          |         - Foo3
          |    objectives:
          |      min_item_rate: 123
          |      max_item_rate: 4567""".stripMargin
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
              objectives = Some(
                WorkflowConfig.StreamConfig.RateObjectives(
                  minItemRate = Some(123),
                  maxItemRate = Some(4567),
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
          |    application_id: foobar
          |    objectives:
          |      min_item_rate: 12
          |      max_item_rate: 345""".stripMargin
      parseYaml(yaml) shouldBe Right(
        WorkflowConfig(
          submission = None,
          streams = List(
            WorkflowConfig.StreamConfig.CompletionsStreamConfig(
              name = "stream-1",
              party = "Obs-2",
              beginOffset = Some(offset("foo")),
              applicationId = "foobar",
              objectives = Some(
                WorkflowConfig.StreamConfig.RateObjectives(
                  minItemRate = Some(12),
                  maxItemRate = Some(345),
                )
              ),
            )
          ),
        )
      )
    }

    "parse ledger-begin and ledger-end markers" in {
      val yaml =
        """streams:
          |  - type: transactions
          |    name: stream-1
          |    filters:
          |      - party: Obs-2
          |        templates:
          |         - Foo1
          |         - Foo3
          |    begin_offset: ledger-begin
          |    end_offset: ledger-end""".stripMargin
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
              beginOffset = Some(ledgerBeginOffset),
              endOffset = Some(ledgerEndOffset),
              objectives = None,
            )
          ),
        )
      )
    }
  }

  def parseYaml(yaml: String): Either[WorkflowConfigParser.ParserError, WorkflowConfig] =
    WorkflowConfigParser.parse(new StringReader(yaml))

  def offset(str: String): LedgerOffset = LedgerOffset.defaultInstance.withAbsolute(str)
  private val ledgerBeginOffset =
    LedgerOffset.defaultInstance.withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)
  private val ledgerEndOffset =
    LedgerOffset.defaultInstance.withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)
}
