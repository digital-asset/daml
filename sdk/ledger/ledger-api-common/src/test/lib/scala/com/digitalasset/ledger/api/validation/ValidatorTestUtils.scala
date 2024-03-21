// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
