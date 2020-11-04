// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.validator.LedgerStateOperations
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{AsyncFunSuite, AsyncWordSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class LogAppendingReadServiceFactorySpec extends AsyncWordSpec with Matchers {
  "LogFragmentingReadServiceFactory" should {
    "handle empty blocks" in {
      val actorSystem: ActorSystem = ActorSystem("LogAppendingReadServiceFactorySpec")
      implicit val materializer: Materializer = Materializer(actorSystem)
      val metrics = new Metrics(new MetricRegistry)
      val factory = new LogAppendingReadServiceFactory(metrics)

      // Append empty WriteSet
      factory.appendBlock(Seq.empty)

      // Append WriteSet consisting entirely of unknown keys
      val unknownKey: LedgerStateOperations.Key = ByteString.copyFrom("???", "utf-8")
      val emptyValue: LedgerStateOperations.Value = ByteString.EMPTY
      factory.appendBlock(List(unknownKey -> emptyValue))

      // Check that all state updates can be served
      factory.createReadService
        .stateUpdates(None)
        .runWith(Sink.fold(0)((n, _) => n + 1))
        .map(count => count shouldBe 0)
    }
  }
}
