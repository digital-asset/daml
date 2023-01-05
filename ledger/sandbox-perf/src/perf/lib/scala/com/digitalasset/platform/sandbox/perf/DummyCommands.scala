// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.perf

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.domain
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.platform.sandbox.services.TestCommands

trait DummyCommands extends TestCommands {

  protected def dummyCreates(ledgerId: domain.LedgerId): Source[SubmitAndWaitRequest, NotUsed] = {
    val templates = templateIds
    Source
      .unfold(0) { i =>
        val next = i + 1
        Some((next, next))
      }
      .map(i =>
        buildRequest(
          ledgerId = ledgerId,
          commandId = s"command-id-create-$i",
          commands = Seq(createWithOperator(templates.dummy)),
          applicationId = "app1",
        ).toSync
      )
  }
}
