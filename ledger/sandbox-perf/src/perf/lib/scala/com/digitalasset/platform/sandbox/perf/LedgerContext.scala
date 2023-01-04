// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.perf

import akka.actor.ActorSystem
import akka.pattern
import com.daml.lf.data.Ref.PackageId
import com.daml.ledger.api.domain
import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc
import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsServiceStub
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityServiceStub
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc,
}
import io.grpc.{Channel, StatusRuntimeException}
import org.slf4j.LoggerFactory

import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

final class LedgerContext(channel: Channel, packageIds: Iterable[PackageId])(implicit
    executionContext: ExecutionContext
) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  val ledgerId: domain.LedgerId =
    domain.LedgerId(
      LedgerIdentityServiceGrpc
        .blockingStub(channel)
        .getLedgerIdentity(GetLedgerIdentityRequest())
        .ledgerId
    ): @nowarn(
      "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*"
    )

  @nowarn("cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*")
  def reset()(implicit system: ActorSystem): Future[LedgerContext] = {
    def waitForNewLedger(retries: Int): Future[domain.LedgerId] =
      if (retries <= 0)
        Future.failed(new RuntimeException("waitForNewLedger: out of retries"))
      else {
        ledgerIdentityService
          .getLedgerIdentity(GetLedgerIdentityRequest())
          .flatMap { resp =>
            // TODO: compare with current Ledger ID and retry when not changed
            Future.successful(domain.LedgerId(resp.ledgerId))
          }
          .recoverWith {
            case _: StatusRuntimeException =>
              logger.debug(
                "waitForNewLedger: retrying identity request in 1 second. {} retries remain",
                retries - 1,
              )
              pattern.after(1.seconds, system.scheduler)(waitForNewLedger(retries - 1))
            case t: Throwable =>
              logger.warn("waitForNewLedger: failed to reconnect!")
              throw t
          }
      }
    for {
      _ <- waitForNewLedger(10)
    } yield new LedgerContext(channel, packageIds)
  }

  def ledgerIdentityService: LedgerIdentityServiceStub =
    LedgerIdentityServiceGrpc.stub(channel): @nowarn(
      "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*"
    )

  def commandService: CommandService =
    CommandServiceGrpc.stub(channel)

  def acsService: ActiveContractsServiceStub =
    ActiveContractsServiceGrpc.stub(channel)

}
