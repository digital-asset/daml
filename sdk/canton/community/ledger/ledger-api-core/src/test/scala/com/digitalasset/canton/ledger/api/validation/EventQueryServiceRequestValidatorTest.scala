// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.v1.value.Value
import com.daml.ledger.api.v1.{event_query_service, value as api}
import com.daml.lf.crypto.Hash.KeyPackageName
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.LedgerString
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.{GlobalKey, NodeId}
import com.daml.lf.value.Value as Lf
import com.digitalasset.canton.HasExecutorService
import com.digitalasset.canton.ledger.api.messages.event
import com.digitalasset.canton.ledger.api.messages.event.KeyContinuationToken
import com.digitalasset.canton.ledger.api.validation.EventQueryServiceRequestValidator.KeyTypeValidator
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import io.grpc.Status.Code
import io.grpc.Status.Code.*
import io.grpc.StatusRuntimeException
import org.mockito.MockitoSugar
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpecLike

import scala.concurrent.{ExecutionContext, Future}

class EventQueryServiceRequestValidatorTest
    extends AsyncWordSpecLike
    with ValidatorTestUtils
    with MockitoSugar
    with HasExecutorService {

  override protected def loggerFactory: NamedLoggerFactory = NamedLoggerFactory.root

  private val wellTypedKey = Lf.ValueText("contractKey")

  private val badlyTyped = "badlyTyped"
  private val unTyped = "unTyped"

  private def gk(v: Lf): GlobalKey =
    GlobalKey.assertBuild(Ref.TypeConName.assertFromString("a:b:c"), v, KeyPackageName.empty)

  private val keyTypeValidator: KeyTypeValidator = (_, t, _) =>
    Future.successful(t match {
      case v: Lf.ValueText if v.value == badlyTyped => Left("Badly typed key")
      case v: Lf.ValueText if v.value == unTyped => Right(gk(wellTypedKey))
      case v => Right(gk(v))
    })

  private val validator =
    new EventQueryServiceRequestValidator(PartyNameChecker.AllowAllParties, keyTypeValidator)

  implicit val ec: ExecutionContext = executorService
  implicit val contextualizedErrorLogger: ContextualizedErrorLogger = NoLogging
  implicit val loggingContextWithTrace: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  "EventQueryServiceRequestValidator" when {

    "validating event by contract id requests" should {

      val expected = event.GetEventsByContractIdRequest(
        contractId = contractId,
        requestingParties = Set(party),
      )

      val req = event_query_service.GetEventsByContractIdRequest(
        contractId.coid,
        expected.requestingParties.toSeq,
      )

      "pass on valid input" in {
        validator.validateEventsByContractId(req) shouldBe Right(expected)
      }

      "fail on empty contractId" in {
        requestMustFailWith(
          request = validator.validateEventsByContractId(req.withContractId("")),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: contract_id",
          metadata = Map.empty,
        )
      }

      "fail on empty requesting parties" in {
        requestMustFailWith(
          request = validator.validateEventsByContractId(req.withRequestingParties(Nil)),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: requesting_parties",
          metadata = Map.empty,
        )
      }

    }

    "validating event by contract key requests" should {

      val txRequest = event.GetEventsByContractKeyRequest(
        globalKey = gk(wellTypedKey),
        requestingParties = Set(party),
        keyContinuationToken = KeyContinuationToken.EndExclusiveEventIdToken(
          EventId(LedgerString.assertFromString("txId"), NodeId(1223))
        ),
      )

      val apiRequest = event_query_service.GetEventsByContractKeyRequest(
        contractKey = Some(api.Value(Value.Sum.Text("contractKey"))),
        templateId = Some(
          com.daml.ledger.api.v1.value
            .Identifier(packageId, moduleName.toString, dottedName.toString)
        ),
        requestingParties = txRequest.requestingParties.toSeq,
        continuationToken = "e:#txId:1223",
      )

      "pass on valid input (no continuation token)" in {
        val apiRequestWithoutToken = apiRequest.withContinuationToken("")
        val expected = txRequest.copy(keyContinuationToken = KeyContinuationToken.NoToken)
        validator
          .validateEventsByContractKey(apiRequestWithoutToken)
          .map(_ shouldBe Right(expected))
      }

      "pass on valid input (event id continuation token)" in {
        validator.validateEventsByContractKey(apiRequest).map(_ shouldBe Right(txRequest))
      }

      "pass on valid input (event sequential id continuation token)" in {
        val apiRequestWithEvtSeqIdToken = apiRequest.copy(continuationToken = "s:1337")
        val expected =
          txRequest.copy(keyContinuationToken = KeyContinuationToken.EndExclusiveSeqIdToken(1337))
        validator
          .validateEventsByContractKey(apiRequestWithEvtSeqIdToken)
          .map(_ shouldBe Right(expected))
      }

      "correct type of untyped key" in {
        validator
          .validateEventsByContractKey(
            apiRequest.withContractKey(api.Value(Value.Sum.Text(unTyped)))
          )
          .map(_ shouldBe Right(txRequest))
      }

      "fail on empty contract_key" in {
        requestMustFailWithAsync(
          requestF = validator.validateEventsByContractKey(apiRequest.clearContractKey),
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: contract_key",
        )
      }

      "fail on badly typed contract_key" in {
        requestMustFailWithAsync(
          requestF = validator.validateEventsByContractKey(
            apiRequest.withContractKey(api.Value(Value.Sum.Text(badlyTyped)))
          ),
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: contract_key: Badly typed key",
        )
      }

      "fail on empty template_id" in {
        requestMustFailWithAsync(
          requestF = validator.validateEventsByContractKey(apiRequest.clearTemplateId),
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: template_id",
        )
      }

      "fail on empty requesting_parties" in {
        requestMustFailWithAsync(
          requestF = validator.validateEventsByContractKey(apiRequest.withRequestingParties(Nil)),
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: requesting_parties",
        )
      }

      "fail on invalid token" in {
        requestMustFailWithAsync(
          requestF =
            validator.validateEventsByContractKey(apiRequest.withContinuationToken("i:gibberish")),
          description =
            "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field continuation_token: Unable to parse 'i:gibberish' into token string",
        )
      }

      def requestMustFailWithAsync(
          requestF: Future[Either[StatusRuntimeException, ?]],
          description: String,
          code: Code = INVALID_ARGUMENT,
      ): Future[Assertion] =
        requestF.map(request => requestMustFailWith(request, code, description))
    }
  }

}
