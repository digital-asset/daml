// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.config

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import java.io.StringReader

import com.daml.ledger.api.benchtool.config.WorkflowConfig.{FooSubmissionConfig, PruningConfig}
import com.daml.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig.PartySet
import com.daml.ledger.api.benchtool.config.WorkflowConfig.StreamConfig.PartyNamePrefixFilter
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

class WorkflowConfigParserSpec extends AnyWordSpec with Matchers {

  private val ledgerBeginOffset =
    LedgerOffset.defaultInstance.withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)
  private val ledgerEndOffset =
    LedgerOffset.defaultInstance.withBoundary(LedgerOffset.LedgerBoundary.LEDGER_END)

  "WorkflowConfigParser" should {
    "parse complete workflow configuration" in {
      val yaml =
        """submission:
          |  type: foo
          |  num_instances: 500
          |  num_observers: 4
          |  num_divulgees: 5
          |  num_extra_submitters: 6
          |  unique_parties: true
          |  allow_non_transient_contracts: true
          |  instance_distribution:
          |    - template: Foo1
          |      weight: 50
          |      payload_size_bytes: 60
          |  nonconsuming_exercises:
          |      probability: 4.9
          |      payload_size_bytes: 100
          |  consuming_exercises:
          |      probability: 0.5
          |      payload_size_bytes: 200
          |  application_ids:
          |       - id: App-1
          |         weight: 100
          |       - id: App-2
          |         weight: 102
          |  observers_party_sets:
          |     - party_name_prefix: FooParty
          |       count: 99
          |       visibility: 0.35
          |     - party_name_prefix: BazParty
          |       count: 10
          |       visibility: 0.01
          |streams:
          |  - type: active-contracts
          |    name: stream-1
          |    filters:
          |      - party: Obs-2
          |        templates:
          |         - Foo1
          |         - Foo3
          |    subscription_delay: 7min         
          |    objectives:
          |      min_item_rate: 123
          |      max_item_rate: 456
          |    max_item_count: 700
          |  - type: transactions
          |    name: stream-2
          |    filters:
          |      - party: Obs-2
          |        templates:
          |         - Foo1
          |unary:
          | - type: pruning
          |   name: pruning-123
          |   prune_all_divulged_contracts: false
          |   max_duration_objective: 56 ms
          |""".stripMargin

      parseYaml(yaml) shouldBe Right(
        WorkflowConfig(
          submission = Some(
            WorkflowConfig.FooSubmissionConfig(
              allowNonTransientContracts = true,
              numberOfInstances = 500,
              numberOfObservers = 4,
              numberOfDivulgees = 5,
              numberOfExtraSubmitters = 6,
              uniqueParties = true,
              instanceDistribution = List(
                WorkflowConfig.FooSubmissionConfig.ContractDescription(
                  template = "Foo1",
                  weight = 50,
                  payloadSizeBytes = 60,
                )
              ),
              nonConsumingExercises = Some(
                WorkflowConfig.FooSubmissionConfig.NonconsumingExercises(
                  probability = 4.9,
                  payloadSizeBytes = 100,
                )
              ),
              consumingExercises = Some(
                WorkflowConfig.FooSubmissionConfig.ConsumingExercises(
                  probability = 0.5,
                  payloadSizeBytes = 200,
                )
              ),
              applicationIds = List(
                FooSubmissionConfig.ApplicationId(
                  applicationId = "App-1",
                  weight = 100,
                ),
                FooSubmissionConfig.ApplicationId(
                  applicationId = "App-2",
                  weight = 102,
                ),
              ),
              observerPartySets = List(
                PartySet(partyNamePrefix = "FooParty", count = 99, visibility = 0.35),
                PartySet(partyNamePrefix = "BazParty", count = 10, visibility = 0.01),
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
                WorkflowConfig.StreamConfig.AcsAndCompletionsObjectives(
                  minItemRate = Some(123),
                  maxItemRate = Some(456),
                )
              ),
              maxItemCount = Some(700),
              timeoutO = None,
              subscriptionDelay = Some(Duration(7, TimeUnit.MINUTES)),
            ),
            // Configuration with all optional values missing
            WorkflowConfig.StreamConfig.TransactionsStreamConfig(
              name = "stream-2",
              filters = List(
                WorkflowConfig.StreamConfig.PartyFilter(
                  party = "Obs-2",
                  templates = List("Foo1"),
                )
              ),
            ),
          ),
          pruning = Some(
            PruningConfig(
              name = "pruning-123",
              pruneAllDivulgedContracts = false,
              maxDurationObjective = Duration(56, TimeUnit.MILLISECONDS),
            )
          ),
        )
      )
    }

    "parse foo submission configuration" in {
      val yaml =
        """submission:
        |  type: foo
        |  num_instances: 500
        |  num_divulgees: 1
        |  num_observers: 4
        |  num_divulgees: 5
        |  num_extra_submitters: 6
        |  unique_parties: true
        |  instance_distribution:
        |    - template: Foo1
        |      weight: 50
        |      payload_size_bytes: 60
        |    - template: Foo2
        |      weight: 25
        |      payload_size_bytes: 35
        |    - template: Foo3
        |      weight: 10
        |      payload_size_bytes: 25
        """.stripMargin

      parseYaml(yaml) shouldBe Right(
        WorkflowConfig(
          submission = Some(
            WorkflowConfig.FooSubmissionConfig(
              numberOfInstances = 500,
              numberOfObservers = 4,
              numberOfDivulgees = 5,
              numberOfExtraSubmitters = 6,
              uniqueParties = true,
              instanceDistribution = List(
                WorkflowConfig.FooSubmissionConfig.ContractDescription(
                  template = "Foo1",
                  weight = 50,
                  payloadSizeBytes = 60,
                ),
                WorkflowConfig.FooSubmissionConfig.ContractDescription(
                  template = "Foo2",
                  weight = 25,
                  payloadSizeBytes = 35,
                ),
                WorkflowConfig.FooSubmissionConfig.ContractDescription(
                  template = "Foo3",
                  weight = 10,
                  payloadSizeBytes = 25,
                ),
              ),
              nonConsumingExercises = None,
              consumingExercises = None,
              applicationIds = List.empty,
            )
          ),
          streams = Nil,
        )
      )
    }

    "parse fibonacci submission configuration" in {
      val yaml =
        """submission:
          |  type: fibonacci
          |  num_instances: 500
          |  unique_parties: true
          |  value: 7
          |  wait_for_submission: true
        """.stripMargin

      parseYaml(yaml) shouldBe Right(
        WorkflowConfig(
          submission = Some(
            WorkflowConfig.FibonacciSubmissionConfig(
              numberOfInstances = 500,
              uniqueParties = true,
              value = 7,
              waitForSubmission = true,
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
          |    party_prefix_filters:
          |      - party_name_prefix: MyParty
          |        templates: [Foo1, Foo2]
          |      - party_name_prefix: MyOtherParty
          |        templates: [Foo1]
          |    begin_offset: foo
          |    end_offset: bar
          |    subscription_delay: 7min    
          |    objectives:
          |      max_delay_seconds: 123
          |      min_consumption_speed: 2.34
          |      min_item_rate: 12
          |      max_item_rate: 34
          |      max_stream_duration: 56 ms
          |""".stripMargin
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
              partyNamePrefixFilters = List(
                PartyNamePrefixFilter(
                  partyNamePrefix = "MyParty",
                  templates = List("Foo1", "Foo2"),
                ),
                PartyNamePrefixFilter(
                  partyNamePrefix = "MyOtherParty",
                  templates = List("Foo1"),
                ),
              ),
              beginOffset = Some(offset("foo")),
              endOffset = Some(offset("bar")),
              objectives = Some(
                WorkflowConfig.StreamConfig.TransactionObjectives(
                  maxDelaySeconds = Some(123),
                  minConsumptionSpeed = Some(2.34),
                  minItemRate = Some(12),
                  maxItemRate = Some(34),
                  maxTotalStreamRuntimeDuration = Some(Duration(56, TimeUnit.MILLISECONDS)),
                )
              ),
              maxItemCount = None,
              timeoutO = None,
              subscriptionDelay = Some(Duration(7, TimeUnit.MINUTES)),
            )
          ),
        )
      )
      // Right(WorkflowConfig(None,List(TransactionsStreamConfig(stream-1,List(PartyFilter(Obs-2,List(Foo1, Foo3),List())),List(PartyNamePrefixFilter(MyParty,List(Foo1, Foo2),List()), PartyNamePrefixFilter(MyOtherParty,List(Foo1),List())),Some(LedgerOffset(Absolute(foo))),Some(LedgerOffset(Absolute(foo))),Some(TransactionObjectives(Some(123),Some(2.34),Some(12.0),Some(34.0),Some(56 milliseconds))),Some(7 minutes),None,None)))) was not equal to
      // Right(WorkflowConfig(None,List(TransactionsStreamConfig(stream-1,List(PartyFilter(Obs-2,List(Foo1, Foo3),List())),List(PartyNamePrefixFilter(MyParty,List(Foo1, Foo2),List()), PartyNamePrefixFilter(MyOtherParty,List(Foo1),List())),Some(LedgerOffset(Absolute(foo))),Some(LedgerOffset(Absolute(bar))),Some(TransactionObjectives(Some(123),Some(2.34),Some(12.0),Some(34.0),Some(56 milliseconds))),Some(7 minutes),None,None))))
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
          |    subscription_delay: 7min    
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
              maxItemCount = None,
              timeoutO = None,
              subscriptionDelay = Some(Duration(7, TimeUnit.MINUTES)),
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
          |    subscription_delay: 7min         
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
              maxItemCount = None,
              timeoutO = None,
              subscriptionDelay = Some(Duration(7, TimeUnit.MINUTES)),
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
          |      - party: Obs-3
          |    begin_offset: foo
          |    end_offset: bar
          |    subscription_delay: 7min    
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
                ),
                WorkflowConfig.StreamConfig.PartyFilter(
                  party = "Obs-3",
                  templates = List.empty,
                ),
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
              maxItemCount = None,
              timeoutO = None,
              subscriptionDelay = Some(Duration(7, TimeUnit.MINUTES)),
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
          |    subscription_delay: 7min         
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
                WorkflowConfig.StreamConfig.AcsAndCompletionsObjectives(
                  minItemRate = Some(123),
                  maxItemRate = Some(4567),
                )
              ),
              maxItemCount = None,
              timeoutO = None,
              subscriptionDelay = Some(Duration(7, TimeUnit.MINUTES)),
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
          |    parties: [Obs-2]
          |    begin_offset: foo
          |    application_id: foobar
          |    timeout: 100s
          |    max_item_count: 101
          |    subscription_delay: 7min    
          |    objectives:
          |      min_item_rate: 12
          |      max_item_rate: 345""".stripMargin
      parseYaml(yaml) shouldBe Right(
        WorkflowConfig(
          submission = None,
          streams = List(
            WorkflowConfig.StreamConfig.CompletionsStreamConfig(
              name = "stream-1",
              parties = List("Obs-2"),
              beginOffset = Some(offset("foo")),
              applicationId = "foobar",
              objectives = Some(
                WorkflowConfig.StreamConfig.AcsAndCompletionsObjectives(
                  minItemRate = Some(12),
                  maxItemRate = Some(345),
                )
              ),
              timeoutO = Some(Duration(100, TimeUnit.SECONDS)),
              maxItemCount = Some(101L),
              subscriptionDelay = Some(Duration(7, TimeUnit.MINUTES)),
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
          |    subscription_delay: 7min         
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
              maxItemCount = None,
              timeoutO = None,
              subscriptionDelay = Some(Duration(7, TimeUnit.MINUTES)),
            )
          ),
        )
      )
    }
  }

  "parse stream configuration with interface filters" in {
    val yaml =
      """streams:
        |  - type: transactions
        |    name: stream-1
        |    filters:
        |      - party: Obs-2
        |        interfaces:
        |         - FooInterface
        |    begin_offset: foo
        |    end_offset: bar
        |    subscription_delay: 7min    
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
                interfaces = List("FooInterface"),
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
            maxItemCount = None,
            timeoutO = None,
            subscriptionDelay = Some(Duration(7, TimeUnit.MINUTES)),
          )
        ),
      )
    )
  }

  "parse party_prefix_filters interfaces" in {
    val yaml =
      """streams:
        |  - type: transactions
        |    name: stream-1
        |    filters:
        |      - party: Obs-2
        |        templates:
        |         - Foo1
        |         - Foo3
        |    party_prefix_filters:
        |      - party_name_prefix: My-Party
        |        interfaces: [FooInterface]
        |    begin_offset: foo
        |    end_offset: bar
        |    subscription_delay: 7min    
        |    objectives:
        |      max_delay_seconds: 123
        |      min_consumption_speed: 2.34
        |      min_item_rate: 12
        |      max_item_rate: 34
        |      max_stream_duration: 56 ms
        |""".stripMargin
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
            partyNamePrefixFilters = List(
              PartyNamePrefixFilter(
                partyNamePrefix = "My-Party",
                interfaces = List("FooInterface"),
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
                maxTotalStreamRuntimeDuration = Some(Duration(56, TimeUnit.MILLISECONDS)),
              )
            ),
            maxItemCount = None,
            timeoutO = None,
            subscriptionDelay = Some(Duration(7, TimeUnit.MINUTES)),
          )
        ),
      )
    )
  }

  def parseYaml(yaml: String): Either[WorkflowConfigParser.ParserError, WorkflowConfig] =
    WorkflowConfigParser.parse(new StringReader(yaml))

  def offset(str: String): LedgerOffset = LedgerOffset.defaultInstance.withAbsolute(str)

}
