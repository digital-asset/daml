// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.command_inspection_service.CommandState.COMMAND_STATE_UNSPECIFIED
import com.daml.ledger.api.v2.admin.command_inspection_service.{
  CommandInspectionServiceGrpc,
  GetCommandStatusRequest,
}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.services.SubmitAndWaitDummyCommandHelpers

import java.util.UUID
import scala.concurrent.Future

final class GetCommandStatusAuthIT
    extends AdminServiceCallAuthTests
    with SubmitAndWaitDummyCommandHelpers {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String =
    "CommandInspectionService#GetCommandStatus"

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = {
    import env.*
    val commandId = UUID.randomUUID().toString
    val randomPartyResolved = getPartyId(randomParty)
    for {
      _ <- submitAndWait(
        canActAsRandomParty.token,
        randomPartyActUser,
        randomPartyResolved,
        commandId = Some(commandId),
      )
      _ <- stub(CommandInspectionServiceGrpc.stub(channel), context.token).getCommandStatus(
        GetCommandStatusRequest(commandId, COMMAND_STATE_UNSPECIFIED, 1)
      )
    } yield ()
  }

}
