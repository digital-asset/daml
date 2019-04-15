// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import java.util.UUID

import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  IsStatusException,
  MockMessages,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.platform.apitesting.{LedgerContext, MultiLedgerFixture}
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.google.protobuf.empty.Empty
import io.grpc.Status
import org.scalatest.{AsyncWordSpec, Inspectors, Matchers}

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class CommandServiceBackPressureIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with Matchers
    with Inspectors
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll {

  private def request(
      ctx: LedgerContext,
      id: String = UUID.randomUUID().toString,
      ledgerId: String = config.ledgerId.getOrElse("")) =
    MockMessages.submitAndWaitRequest
      .update(_.commands.ledgerId := ledgerId, _.commands.commandId := id)
      .copy(traceContext = None)

  "Commands Service" when {
    "overloaded with command submission" should {
      "reject requests with RESOURCE_EXHAUSTED" in allFixtures { ctx =>
        val responses: immutable.Seq[Future[Empty]] = (1 to 256) map { _ =>
          ctx.commandService.submitAndWait(request(ctx))
        }

        val done: Future[immutable.Seq[Try[Empty]]] =
          Future.sequence(responses.map(_.map(Success(_)).recover({
            case ex => Failure(ex)
          })))

        done.map(_.collect { case Failure(ex) => ex }) map { errors =>
          info(s"${errors.size} requests failed with ${Status.RESOURCE_EXHAUSTED}")
          errors should not be empty
          forAll(errors) {
            IsStatusException(Status.RESOURCE_EXHAUSTED)(_)
          }
        }
      }
    }

  }

  override protected def config: Config =
    Config.default.withCommandConfiguration(
      SandboxConfig.defaultCommandConfig
        .copy(inputBufferSize = 1, maxParallelSubmissions = 1, maxCommandsInFlight = 2)
    )

}
