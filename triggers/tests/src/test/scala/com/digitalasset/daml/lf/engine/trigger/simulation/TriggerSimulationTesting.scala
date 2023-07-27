// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger
package simulation

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.{value => LedgerApi}
import com.daml.lf.crypto
import com.daml.lf.data.{Bytes, Ref}
import com.daml.lf.engine.trigger.test.AbstractTriggerTest
import com.daml.lf.speedy.SExpr.{SEApp, SEValue}
import com.daml.lf.speedy.{SValue, Speedy}
import com.daml.lf.testing.parser
import com.daml.lf.testing.parser.{defaultLanguageVersion, parseExpr}
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext
import com.google.rpc.status.Status
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, Suite}
import scalaz.syntax.tag._

import java.nio.file.{Path, Paths}
import java.util.UUID
import scala.collection.immutable.TreeMap

trait TriggerSimulationTesting extends Matchers with AbstractTriggerTest {
  self: Suite =>

  override protected lazy val darFile: Either[Path, Path] =
    Right(
      Paths.get(
        Option(System.getenv("DAR")).getOrElse(
          throw new RuntimeException(
            "Trigger simulations need a Dar file specified using the environment variable: DAR"
          )
        )
      )
    )

  override protected def triggerRunnerConfiguration: TriggerRunnerConfig =
    super.triggerRunnerConfiguration.copy(hardLimit =
      super.triggerRunnerConfiguration.hardLimit
        .copy(allowTriggerTimeouts = true, allowInFlightCommandOverflows = true)
    )

  protected def safeSValueFromLf(lfValue: String): Either[String, SValue] = {
    val parserParameters: parser.ParserParameters[this.type] =
      parser.ParserParameters(
        defaultPackageId = packageId,
        languageVersion = defaultLanguageVersion,
      )

    parseExpr(lfValue)(parserParameters).flatMap(expr =>
      Speedy.Machine
        .runPureExpr(expr, compiledPackages)(LoggingContext.ForTesting)
        .left
        .map(_.toString)
    )
  }

  protected def xxx(lfValue: String) = {
    val parserParameters: parser.ParserParameters[this.type] =
      parser.ParserParameters(
        defaultPackageId = packageId,
        languageVersion = defaultLanguageVersion,
      )

    parseExpr(lfValue)(parserParameters)
  }

  protected def unsafeSValueFromLf(lfValue: String): SValue = {
    safeSValueFromLf(lfValue).left
      .map(cause => throw new RuntimeException(s"$cause - parsing $lfValue"))
      .toOption
      .get
  }

  protected def safeSValueApp(lfFuncExpr: String, svalue: SValue): Either[String, SValue] = {
    val sexpr = SEApp(SEValue(unsafeSValueFromLf(lfFuncExpr)), Array(svalue))

    Speedy.Machine
      .runPureSExpr(sexpr, compiledPackages)(LoggingContext.ForTesting)
      .left
      .map(_.toString)
  }

  protected def unsafeSValueApp(lfFuncExpr: String, svalue: SValue): SValue = {
    safeSValueApp(lfFuncExpr, svalue).left
      .map(cause =>
        throw new RuntimeException(s"$cause - parsing $lfFuncExpr and applying to $svalue")
      )
      .toOption
      .get
  }

  protected def mkParty(party: ApiTypes.Party): String = {
    mkParty(party.unwrap)
  }

  protected def mkParty(party: String): String = {
    s"(case TEXT_TO_PARTY \"$party\" of None -> ERROR @Party \"none\" | Some x -> x)"
  }

  protected def userState(state: SValue)(implicit triggerDefn: TriggerDefinition): SValue = {
    Runner.triggerUserState(state, triggerDefn.level, triggerDefn.version)
  }

  protected def activeContracts(
      state: SValue
  )(implicit triggerDefn: TriggerDefinition): TreeMap[SValue, TreeMap[SValue, SValue]] = {
    Runner
      .getActiveContracts(state, triggerDefn.level, triggerDefn.version)
      .getOrElse(throw new RuntimeException("???"))
  }

  protected def createdEvent(
      templateId: String,
      createArguments: String,
      contractId: String = UUID.randomUUID().toString,
      contractKey: Option[String] = None,
  ): CreatedEvent = {
    val apiContractId = toContractId(contractId).coid
    val apiTemplateId = Ref.Identifier.assertFromString(templateId)
    val apiCreateArguments = Converter
      .toLedgerRecord(unsafeSValueFromLf(createArguments))
      .left
      .map(cause => throw new RuntimeException(cause))
      .toOption
    val apiContractKey = contractKey.flatMap(key =>
      Converter
        .toLedgerValue(unsafeSValueFromLf(key))
        .left
        .map(cause => throw new RuntimeException(cause))
        .toOption
    )

    CreatedEvent(
      contractId = apiContractId,
      templateId = Some(
        LedgerApi.Identifier(
          apiTemplateId.packageId,
          apiTemplateId.qualifiedName.module.toString,
          apiTemplateId.qualifiedName.name.toString,
        )
      ),
      createArguments = apiCreateArguments,
      contractKey = apiContractKey,
    )
  }

  protected def archivedEvent(templateId: String, contractId: String): ArchivedEvent = {
    val apiContractId = toContractId(contractId).coid
    val apiTemplateId = Ref.Identifier.assertFromString(templateId)

    ArchivedEvent(
      contractId = apiContractId,
      templateId = Some(
        LedgerApi.Identifier(
          apiTemplateId.packageId,
          apiTemplateId.qualifiedName.module.toString,
          apiTemplateId.qualifiedName.name.toString,
        )
      ),
    )
  }

  protected def transaction(
      createdEvent: CreatedEvent,
      createdEvents: CreatedEvent*
  ): Transaction = {
    Transaction(events =
      (createdEvent +: createdEvents).map(evt => Event(Event.Event.Created(evt)))
    )
  }

  protected def transaction(
      archivedEvent: ArchivedEvent,
      archivedEvents: ArchivedEvent*
  ): Transaction = {
    Transaction(events =
      (archivedEvent +: archivedEvents).map(evt => Event(Event.Event.Archived(evt)))
    )
  }

  protected def completion(commandId: String, status: Status = new Status(0)): Completion = {
    Completion(commandId = commandId, status = Some(status))
  }

  protected def assertEqual(actualLfExpr: String, expectedLfExpr: String): Assertion = {
    val actualSValue = unsafeSValueFromLf(actualLfExpr)

    assertEqual(actualSValue, expectedLfExpr)
  }

  protected def assertEqual(actualSValue: SValue, expectedLfExpr: String): Assertion = {
    val expectedSValue = unsafeSValueFromLf(expectedLfExpr)

    actualSValue should be(expectedSValue)
  }

  private def toContractId(s: String): ContractId =
    ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), Bytes.assertFromString("00"))
}
