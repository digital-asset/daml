// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger
package simulation

import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, InterfaceView}
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

import java.util.UUID

trait TriggerSimulationTesting extends Matchers with AbstractTriggerTest {
  self: Suite =>

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

  protected def unsafeSValueFromLf(lfValue: String): SValue = {
    safeSValueFromLf(lfValue).left.map(cause => throw new RuntimeException(cause)).toOption.get
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
      .map(cause => throw new RuntimeException(cause))
      .toOption
      .get
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
