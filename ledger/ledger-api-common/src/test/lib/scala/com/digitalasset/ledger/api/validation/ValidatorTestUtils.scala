// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.grpc.GrpcStatus
import com.daml.ledger.api.domain
import com.daml.ledger.api.messages.transaction
import com.daml.lf.data.Ref
import com.google.rpc.error_details
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import org.scalatest._
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait ValidatorTestUtils extends Matchers with Inside with OptionValues { self: Suite =>

  protected val includedModule = "includedModule"
  protected val includedTemplate = "includedTemplate"
  protected val expectedLedgerId = "expectedLedgerId"
  protected val expectedApplicationId = "expectedApplicationId"
  protected val packageId = Ref.PackageId.assertFromString("packageId")
  protected val absoluteOffset = Ref.LedgerString.assertFromString("0042")
  protected val party = Ref.Party.assertFromString("party")
  protected val verbose = false
  protected val eventId = "eventId"
  protected val transactionId = "42"
  protected val ledgerEnd = domain.LedgerOffset.Absolute(Ref.LedgerString.assertFromString("1000"))

  /** Fixture that facilitates testing validators with and without self-service error codes.
    *
    * @param testedFactory Creates an instance of a validator to be tested.
    *                      Accepts a boolean to decide if self-service error codes should be enabled.
    */
  class ValidatorFixture[T](testedFactory: Boolean => T) {
    def testRequestFailure(
        testedRequest: T => Either[StatusRuntimeException, _],
        expectedCodeV1: Code,
        expectedDescriptionV1: String,
        expectedCodeV2: Code,
        expectedDescriptionV2: String,
        metadataV2: Map[String, String] = Map.empty,
    ): Assertion = {
      requestMustFailWith(
        request = testedRequest(testedFactory(false)),
        code = expectedCodeV1,
        description = expectedDescriptionV1,
        metadata = Map.empty[String, String],
      )
      requestMustFailWith(
        request = testedRequest(testedFactory(true)),
        code = expectedCodeV2,
        description = expectedDescriptionV2,
        metadataV2,
      )
    }

    def tested(enabledSelfServiceErrorCodes: Boolean): T = {
      testedFactory(enabledSelfServiceErrorCodes)
    }
  }

  protected def hasExpectedFilters(req: transaction.GetTransactionsRequest) = {
    val filtersByParty = req.filter.filtersByParty
    filtersByParty should have size 1
    inside(filtersByParty.headOption.value) { case (p, filters) =>
      p shouldEqual party
      filters shouldEqual domain.Filters(
        Some(
          domain.InclusiveFilters(
            Set(
              Ref.Identifier(
                Ref.PackageId.assertFromString(packageId),
                Ref.QualifiedName(
                  Ref.DottedName.assertFromString(includedModule),
                  Ref.DottedName.assertFromString(includedTemplate),
                ),
              )
            )
          )
        )
      )
    }
  }

  protected def requestMustFailWith(
      request: Future[_],
      code: Code,
      description: String,
      metadata: Map[String, String],
  ): Future[Assertion] = {
    val f = request.map(Right(_)).recover { case ex: StatusRuntimeException => Left(ex) }
    f.map(inside(_)(isError(code, description, metadata)))
  }

  protected def requestMustFailWith(
      request: Either[StatusRuntimeException, _],
      code: Code,
      description: String,
      metadata: Map[String, String],
  ): Assertion = {
    inside(request)(isError(code, description, metadata))
  }
  protected def isError(
      expectedCode: Code,
      expectedDescription: String,
      metadata: Map[String, String],
  ): PartialFunction[Either[StatusRuntimeException, _], Assertion] = { case Left(err) =>
    err.getStatus should have(Symbol("code")(expectedCode))
    err.getStatus should have(Symbol("description")(expectedDescription))
    GrpcStatus
      .toProto(err.getStatus, err.getTrailers)
      .details
      .flatMap(_.unpack[error_details.ErrorInfo].metadata)
      .toMap should contain allElementsOf metadata
  }

}
