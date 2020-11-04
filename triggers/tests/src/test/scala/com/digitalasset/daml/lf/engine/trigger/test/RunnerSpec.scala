// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger
package test

import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.scalatest.AsyncForAll
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.duration.Duration.Zero
import scala.concurrent.Future

class RunnerSpec extends AsyncWordSpec with Matchers with AsyncForAll with AkkaBeforeAndAfterAll {
  import Runner.retrying

  "retrying" should {
    "terminate immediately on empty input" in {
      Source
        .empty[Unit]
        .via(retrying(5, _ => Zero, 8, a => Future successful Some(a), a => Future successful a))
        .runWith(Sink.seq)
        .map(_ shouldBe empty)
    }
  }
}
