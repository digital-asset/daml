// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.config

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import java.io.File
import java.util.concurrent.TimeUnit

class CliSpec extends AnyWordSpec with Matchers with OptionValues with TableDrivenPropertyChecks {

  "Cli" should {
    "produce the default config when no arguments defined" in {
      parse() shouldBe Config.Default
    }

    "parse ledger API endpoint" in {
      val endpoint = "foo:123"
      val expectedConfig = Config.Default.copy(
        ledger = Config.Ledger(
          hostname = "foo",
          port = 123,
        )
      )
      parse("--endpoint", endpoint) shouldBe expectedConfig
      parse("-e", endpoint) shouldBe expectedConfig
    }

    "parse workflow config location" in {
      val workflowFile = "/some/path/to/file"
      val expectedConfig = Config.Default.copy(workflowConfigFile = Some(new File(workflowFile)))
      parse("--workflow-config", workflowFile) shouldBe expectedConfig
      parse("-w", workflowFile) shouldBe expectedConfig
    }

    "parse maximum number of in-flight commands parameter" in {
      val maxCommands = 123
      val expectedConfig = Config.Default.copy(maxInFlightCommands = maxCommands)
      parse("--max-in-flight-commands", maxCommands.toString) shouldBe expectedConfig
    }

    "parse submission batch size" in {
      val batchSize = 1234
      val expectedConfig = Config.Default.copy(submissionBatchSize = batchSize)
      parse("--submission-batch-size", batchSize.toString) shouldBe expectedConfig
    }

    "parse log interval" in {
      val cases = Table(
        "cli value" -> "duration",
        "1s" -> 1.second,
        "123millis" -> 123.millis,
        "5m" -> 5.minutes,
      )
      forAll(cases) { (argument, intervalDuration) =>
        val expectedConfig = Config.Default.copy(reportingPeriod = intervalDuration)
        parse("--log-interval", argument) shouldBe expectedConfig
        parse("-r", argument) shouldBe expectedConfig
      }
    }

    "parse thread pool executor's core pool size" in {
      val size = 123
      val expectedConfig =
        Config.Default.copy(concurrency = Config.Default.concurrency.copy(corePoolSize = size))
      parse("--core-pool-size", size.toString) shouldBe expectedConfig
    }

    "parse thread pool executor's max pool size" in {
      val size = 123
      val expectedConfig =
        Config.Default.copy(concurrency = Config.Default.concurrency.copy(maxPoolSize = size))
      parse("--max-pool-size", size.toString) shouldBe expectedConfig
    }

    "parse stream type" in {
      import WorkflowConfig.StreamConfig._
      val name = "streamname"
      val party1 = "dummy1"
      val party2 = "dummy2"
      val appId = "appid"
      val cases = Table(
        "cli argument" -> "stream config",
        s"stream-type=transactions,name=$name,filters=$party1" -> TransactionsStreamConfig(
          name = name,
          filters = List(PartyFilter(party1, Nil, Nil)),
          beginOffset = None,
          endOffset = None,
          objectives = None,
          maxItemCount = None,
          timeoutDurationO = None,
        ),
        s"stream-type=transaction-trees,name=$name,filters=$party1" -> TransactionTreesStreamConfig(
          name = name,
          filters = List(PartyFilter(party1, Nil, Nil)),
          beginOffset = None,
          endOffset = None,
          objectives = None,
          maxItemCount = None,
          timeoutDurationO = None,
        ),
        s"stream-type=active-contracts,name=$name,filters=$party1" -> ActiveContractsStreamConfig(
          name = name,
          filters = List(PartyFilter(party1, Nil, Nil)),
          objectives = None,
          maxItemCount = None,
          timeoutDurationO = None,
        ),
        s"stream-type=completions,name=$name,parties=$party1+$party2,application-id=$appId,timeout=123s,max-item-count=5" -> CompletionsStreamConfig(
          name = name,
          parties = List(party1, party2),
          applicationId = appId,
          beginOffset = None,
          objectives = None,
          timeoutDurationO = Some(Duration(123, TimeUnit.SECONDS)),
          maxItemCount = Some(5),
        ),
      )
      forAll(cases) { (argument, config) =>
        val expectedConfig =
          Config.Default.copy(workflow = Config.Default.workflow.copy(streams = List(config)))
        parse("--consume-stream", argument) shouldBe expectedConfig
        parse("-s", argument) shouldBe expectedConfig
      }
    }

    "parse stream filters" in {
      import WorkflowConfig.StreamConfig._
      val name = "streamname"
      val party1 = "alice"
      val party2 = "bob"
      val party3 = "david"
      val template1 = "packageid:Foo:Foo1"
      val template2 = "packageid2:Foo:Foo2"
      // each party filter separated by '+' and each template in a filter separated by '@'
      val filters = s"$party1+$party2@$template1@$template2+$party3@$template2"
      val filtersList = List(
        PartyFilter(party1, List(), List()),
        PartyFilter(party2, List(template1, template2), List()),
        PartyFilter(party3, List(template2), List()),
      )
      val cases = Table(
        "cli argument" -> "stream config",
        s"stream-type=transactions,name=$name,filters=$filters" -> TransactionsStreamConfig(
          name = name,
          filters = filtersList,
          beginOffset = None,
          endOffset = None,
          objectives = None,
          maxItemCount = None,
          timeoutDurationO = None,
        ),
        s"stream-type=transaction-trees,name=$name,filters=$filters" -> TransactionTreesStreamConfig(
          name = name,
          filters = filtersList,
          beginOffset = None,
          endOffset = None,
          objectives = None,
          maxItemCount = None,
          timeoutDurationO = None,
        ),
        s"stream-type=active-contracts,name=$name,filters=$filters" -> ActiveContractsStreamConfig(
          name = name,
          filters = filtersList,
          objectives = None,
          maxItemCount = None,
          timeoutDurationO = None,
        ),
      )
      forAll(cases) { (argument, config) =>
        val expectedConfig =
          Config.Default.copy(workflow = Config.Default.workflow.copy(streams = List(config)))
        parse("--consume-stream", argument) shouldBe expectedConfig
        parse("-s", argument) shouldBe expectedConfig
      }
    }

    "parse begin offset" in {
      import WorkflowConfig.StreamConfig._
      val name = "streamname"
      val party = "dummy"
      val cases = Table(
        "cli parameter" -> "offset",
        "abcdef" -> LedgerOffset.defaultInstance.withAbsolute("abcdef"),
        "ledger-begin" -> LedgerOffset.defaultInstance.withBoundary(
          LedgerOffset.LedgerBoundary.LEDGER_BEGIN
        ),
        "ledger-end" -> LedgerOffset.defaultInstance.withBoundary(
          LedgerOffset.LedgerBoundary.LEDGER_END
        ),
      )
      forAll(cases) { (argument, offset) =>
        val streamConfig = TransactionsStreamConfig(
          name = name,
          filters = List(PartyFilter(party, Nil, Nil)),
          beginOffset = Some(offset),
          endOffset = None,
          objectives = None,
          maxItemCount = None,
          timeoutDurationO = None,
        )
        val expectedConfig =
          Config.Default.copy(workflow = Config.Default.workflow.copy(streams = List(streamConfig)))

        parse(
          "--consume-stream",
          s"stream-type=transactions,name=$name,filters=$party,begin-offset=$argument",
        ) shouldBe expectedConfig
      }
    }

    "parse end offset" in {
      import WorkflowConfig.StreamConfig._
      val name = "streamname"
      val party = "dummy"
      val cases = Table(
        "cli parameter" -> "offset",
        "abcdef" -> LedgerOffset.defaultInstance.withAbsolute("abcdef"),
        "ledger-begin" -> LedgerOffset.defaultInstance.withBoundary(
          LedgerOffset.LedgerBoundary.LEDGER_BEGIN
        ),
        "ledger-end" -> LedgerOffset.defaultInstance.withBoundary(
          LedgerOffset.LedgerBoundary.LEDGER_END
        ),
      )
      forAll(cases) { (argument, offset) =>
        val streamConfig = TransactionsStreamConfig(
          name = name,
          filters = List(PartyFilter(party, Nil, Nil)),
          beginOffset = None,
          endOffset = Some(offset),
          objectives = None,
          maxItemCount = None,
          timeoutDurationO = None,
        )
        val expectedConfig =
          Config.Default.copy(workflow = Config.Default.workflow.copy(streams = List(streamConfig)))

        parse(
          "--consume-stream",
          s"stream-type=transactions,name=$name,filters=$party,end-offset=$argument",
        ) shouldBe expectedConfig
      }
    }

    "parse transaction objectives" in {
      import WorkflowConfig.StreamConfig._
      val name = "streamname"
      val party = "dummy"
      val cases = Table(
        "cli parameter" -> "objectives",
        "max-delay=5" -> TransactionObjectives(maxDelaySeconds = Some(5), None, None, None),
        "min-consumption-speed=1.23" -> TransactionObjectives(
          None,
          minConsumptionSpeed = Some(1.23),
          None,
          None,
        ),
        "min-item-rate=1234.5" -> TransactionObjectives(
          None,
          None,
          minItemRate = Some(1234.5),
          None,
        ),
        "max-item-rate=1234.5" -> TransactionObjectives(
          None,
          None,
          None,
          maxItemRate = Some(1234.5),
        ),
      )
      forAll(cases) { (argument, objectives) =>
        val streamConfig = TransactionsStreamConfig(
          name = name,
          filters = List(PartyFilter(party, Nil, Nil)),
          beginOffset = None,
          endOffset = None,
          objectives = Some(objectives),
          maxItemCount = None,
          timeoutDurationO = None,
        )
        val expectedConfig =
          Config.Default.copy(workflow = Config.Default.workflow.copy(streams = List(streamConfig)))

        parse(
          "--consume-stream",
          s"stream-type=transactions,name=$name,filters=$party,$argument",
        ) shouldBe expectedConfig
      }
    }

    "parse rate objectives" in {
      import WorkflowConfig.StreamConfig._
      val name = "streamname"
      val party = "dummy"
      val cases = Table(
        "cli parameter" -> "objectives",
        "min-item-rate=1234.5" -> AcsAndCompletionsObjectives(minItemRate = Some(1234.5), None),
        "max-item-rate=1234.5" -> AcsAndCompletionsObjectives(None, maxItemRate = Some(1234.5)),
      )
      forAll(cases) { (argument, objectives) =>
        val streamConfig = ActiveContractsStreamConfig(
          name = name,
          filters = List(PartyFilter(party, Nil, Nil)),
          objectives = Some(objectives),
          maxItemCount = None,
          timeoutDurationO = None,
        )
        val expectedConfig =
          Config.Default.copy(workflow = Config.Default.workflow.copy(streams = List(streamConfig)))

        parse(
          "--consume-stream",
          s"stream-type=active-contracts,name=$name,filters=$party,$argument",
        ) shouldBe expectedConfig
      }
    }

    "parse `latency-test` flag" in {
      val expectedConfig = Config.Default.copy(latencyTest = true)
      parse("--latency-test") shouldBe expectedConfig
    }

    "parse `max-latency-objective` flag" in {
      val expectedConfig = Config.Default.copy(maxLatencyObjectiveMillis = 6000L)
      parse("--max-latency-millis", "6000") shouldBe expectedConfig
    }

    "`latency-test` cannot be enabled with configured workflow streams" in {
      Cli.config(
        Array(
          "--latency-test",
          "--consume-stream",
          s"stream-type=transactions,name=some-name,filters=some-filter,end-offset=ABC",
        )
      ) shouldBe empty
    }
  }

  private def parse(args: String*): Config =
    Cli.config(args.toArray).value
}
