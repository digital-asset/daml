// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import java.util.regex.Pattern
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.test.java.semantic.timetests._

import scala.concurrent.Future

final class TimeServiceIT extends LedgerTestSuite {
  implicit val timecheckerCompanion
      : ContractCompanion.WithoutKey[TimeChecker.Contract, TimeChecker.ContractId, TimeChecker] =
    TimeChecker.COMPANION

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
    "TSTimeAdvancementCanFail",
    "Time advancement can fail when current time is not accurate",
    allocate(NoParties),
    runConcurrently = false,
    enabled = _.staticTime,
    disabledReason = "requires ledger static time feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      initialTime <- ledger.time()
      invalidInitialTime = initialTime.plusSeconds(1)
      thirtySecLater = initialTime.plusSeconds(30)
      _ <- ledger
        .setTime(invalidInitialTime, thirtySecLater)
        .mustFailWith(
          "current_time mismatch",
          LedgerApiErrors.RequestValidation.InvalidArgument,
        )
    } yield ()
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
      thirtySecLater = initialTime.plusSeconds(30)
      checker <- ledger.create(party, new TimeChecker(party, thirtySecLater))
      failure <- ledger
        .exercise(party, checker.exerciseTimeChecker_CheckTime())
        .mustFail("submitting choice prematurely")
    } yield {
      assertGrpcErrorRegex(
        failure,
        LedgerApiErrors.CommandExecution.Interpreter.UnhandledException,
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
      thirtySecLater = initialTime.plusSeconds(30)
      checker <- ledger.create(party, new TimeChecker(party, thirtySecLater))
      _ <- ledger.setTime(initialTime, initialTime.plusSeconds(30))
      _ <- ledger.exercise(party, checker.exerciseTimeChecker_CheckTime())
    } yield ()
  })

}
