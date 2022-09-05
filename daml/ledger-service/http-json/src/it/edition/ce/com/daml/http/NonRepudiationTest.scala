// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import com.daml.nonrepudiation.CommandIdString

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
abstract class NonRepudiationTest extends AbstractNonRepudiationTest {

  import HttpServiceTestFixture._

  "fail to work through the non-repudiation proxy" in withSetup { fixture =>
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
      .flatMap { case (status, _) =>
        status shouldBe StatusCodes.InternalServerError
        val payloads = db.signedPayloads.get(CommandIdString.wrap(expectedCommandId))
        payloads shouldBe empty
      }
  }

}

final class NonRepudiationTestCustomToken
    extends NonRepudiationTest
    with AbstractHttpServiceIntegrationTestFunsCustomToken
