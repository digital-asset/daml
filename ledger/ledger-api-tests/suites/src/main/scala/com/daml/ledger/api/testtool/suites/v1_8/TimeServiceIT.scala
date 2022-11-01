// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import java.time.Instant
import java.util.regex.Pattern
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.client.binding.{Primitive => P}
import com.daml.ledger.test.semantic.TimeTests._
import com.daml.platform.error.definitions.LedgerApiErrors

import scala.concurrent.Future

final class TimeServiceIT extends LedgerTestSuite {

  test(
    "TSTimeIsStatic",
    "Time stands still when static time enabled",
    allocate(NoParties),
    runConcurrently = false,
    enabled = _.staticTime,
    disabledReason = "requires ledger static time feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      initialTime <- ledger.time()
      _ <- Future { Thread.sleep(100) }
      laterTime <- ledger.time()
    } yield {
      assertEquals("ledger time should stand still", laterTime, initialTime)
    }
  })

  test(
    "TSTimeCanBeAdvanced",
    "Time can be advanced when static time enabled",
    allocate(NoParties),
    runConcurrently = false,
    enabled = _.staticTime,
    disabledReason = "requires ledger static time feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      initialTime <- ledger.time()
      thirtySecLater = initialTime.plusSeconds(30)
      _ <- ledger.setTime(initialTime, thirtySecLater)
      laterTime <- ledger.time()
    } yield {
      assertEquals("ledger time should be advanced", laterTime, thirtySecLater)
    }
  })

  test(
    "TSFailWhenTimeNotAdvanced",
    "The submission of an exercise before time advancement should fail",
    allocate(SingleParty),
    runConcurrently = false,
    enabled = _.staticTime,
    disabledReason = "requires ledger static time feature",
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      initialTime <- ledger.time()
      thirtySecLater <- createTimestamp(initialTime.plusSeconds(30))
      checker <- ledger.create(party, TimeChecker(party, thirtySecLater))
      failure <- ledger
        .exercise(party, checker.exerciseTimeChecker_CheckTime())
        .mustFail("submitting choice prematurely")
    } yield {
      assertGrpcErrorRegex(
        failure,
        LedgerApiErrors.CommandExecution.Interpreter.GenericInterpretationError,
        Some(Pattern.compile("Unhandled (Daml )?exception")),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "TSSucceedWhenTimeAdvanced",
    "The submission of an exercise after time advancement should succeed",
    allocate(SingleParty),
    runConcurrently = false,
    enabled = _.staticTime,
    disabledReason = "requires ledger static time feature",
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      initialTime <- ledger.time()
      thirtySecLater <- createTimestamp(initialTime.plusSeconds(30))
      checker <- ledger.create(party, TimeChecker(party, thirtySecLater))
      _ <- ledger.setTime(initialTime, initialTime.plusSeconds(30))
      _ <- ledger.exercise(party, checker.exerciseTimeChecker_CheckTime())
    } yield ()
  })

  def createTimestamp(seconds: Instant): Future[P.Timestamp] =
    P.Timestamp
      .discardNanos(seconds)
      .fold(Future.failed[P.Timestamp](new IllegalStateException(s"Empty option")))(
        Future.successful[P.Timestamp]
      )

}
