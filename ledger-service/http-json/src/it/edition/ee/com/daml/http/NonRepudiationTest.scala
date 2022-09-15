// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.nonrepudiation.CommandIdString

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
final class NonRepudiationTest
    extends AbstractNonRepudiationTest
    with AbstractHttpServiceIntegrationTestFunsCustomToken {

  import HttpServiceTestFixture._

  "correctly sign a command" in withSetup { fixture =>
    import fixture.db
    val expectedParty = "Alice"
    val expectedNumber = "abc123"
    val expectedCommandId = UUID.randomUUID.toString
    val meta = Some(
      domain.CommandMeta(
        commandId = Some(domain.CommandId(expectedCommandId)),
        actAs = None,
        readAs = None,
        submissionId = None,
        deduplicationPeriod = None,
      )
    )
    val domainParty = domain.Party(expectedParty)
    val command = accountCreateCommand(domainParty, expectedNumber).copy(meta = meta)
    postCreateCommand(command, fixture)
      .flatMap(inside(_) { case domain.OkResponse(_, _, status) =>
        status shouldBe StatusCodes.OK
        val payloads = db.signedPayloads.get(CommandIdString.wrap(expectedCommandId))
        payloads should have size 1
        val signedCommand = SubmitRequest.parseFrom(payloads.head.payload.unsafeArray)
        val commands = signedCommand.getCommands.commands
        commands should have size 1
        val actualFields = commands.head.getCreate.getCreateArguments.fields.map(stripIdentifiers)
        val expectedFields = command.payload.fields.map(stripIdentifiers)
        actualFields should contain theSameElementsAs expectedFields
      })
  }

}
