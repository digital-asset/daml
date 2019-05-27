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
import com.digitalasset.platform.testing.LedgerBackend.SandboxSql
import com.digitalasset.platform.apitesting.{LedgerContext, MultiLedgerFixture}
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.google.protobuf.empty.Empty
import io.grpc.Status
import org.scalatest.{AsyncWordSpec, Inspectors, Matchers}

import scalaz.syntax.tag._
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

  private def submitAndWaitRequest(
      ctx: LedgerContext,
      id: String = UUID.randomUUID().toString) =
    MockMessages.submitAndWaitRequest
      .update(
        _.commands.ledgerId := ctx.ledgerId.unwrap,
        _.commands.commandId := id,
        _.optionalTraceContext := None)

  private def submitRequest(
      ctx: LedgerContext,
      id: String = UUID.randomUUID().toString) =
    MockMessages.submitRequest
      .update(
        _.commands.ledgerId := ctx.ledgerId.unwrap,
        _.commands.commandId := id,
        _.optionalTraceContext := None)

  "Commands Submission Service" when {
    "overloaded with commands" should {
      // this test only works on the SQL implementation, as we don't have explicit back-pressure implemented in
      // the in-memory implementation
      "reject requests with RESOURCE_EXHAUSTED" in forAllMatchingFixtures {
        case TestFixture(SandboxSql, ctx) =>
          val responses: immutable.Seq[Future[Empty]] = (1 to 256) map { _ =>
            ctx.commandSubmissionService.submit(submitRequest(ctx))
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

  "Commands Service" when {
    "overloaded with commands" should {
      "reject requests with RESOURCE_EXHAUSTED" in allFixtures { ctx =>
        val responses: immutable.Seq[Future[Empty]] = (1 to 256) map { _ =>
          ctx.commandService.submitAndWait(submitAndWaitRequest(ctx))
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
