// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import brave.propagation
import com.daml.lf.data.Ref
import com.daml.ledger.api.domain
import com.daml.ledger.api.messages.transaction
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import org.scalatest._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
@SuppressWarnings(Array("org.wartremover.warts.Any"))
trait ValidatorTestUtils extends Matchers with Inside with OptionValues { self: Suite =>

  protected val traceIdHigh = 1L
  protected val traceId = 2L
  protected val spanId = 3L
  protected val parentSpanId = Some(4L)
  protected val sampled = true
  protected val includedModule = "includedModule"
  protected val includedTemplate = "includedTemplate"
  protected val expectedLedgerId = "expectedLedgerId"
  protected val packageId = Ref.PackageId.assertFromString("packageId")
  protected val absoluteOffset = Ref.LedgerString.assertFromString("42")
  protected val party = Ref.Party.assertFromString("party")
  protected val verbose = false
  protected val eventId = "eventId"
  protected val transactionId = "42"
  protected val offsetOrdering = Ordering.by[domain.LedgerOffset.Absolute, Int](_.value.toInt)
  protected val ledgerEnd = domain.LedgerOffset.Absolute(Ref.LedgerString.assertFromString("1000"))

  protected def hasExpectedFilters(req: transaction.GetTransactionsRequest) = {
    val filtersByParty = req.filter.filtersByParty
    filtersByParty should have size 1
    inside(filtersByParty.headOption.value) {
      case (p, filters) =>
        p shouldEqual party
        filters shouldEqual domain.Filters(
          Some(domain.InclusiveFilters(Set(Ref.Identifier(
            Ref.PackageId.assertFromString(packageId),
            Ref.QualifiedName(
              Ref.DottedName.assertFromString(includedModule),
              Ref.DottedName.assertFromString(includedTemplate))
          )))))
    }
  }

  protected def hasExpectedTraceContext(req: transaction.GetTransactionsRequest) = {
    inside(req.traceContext.value) {
      case e => isExpectedTraceContext(e)
    }
  }

  protected def isExpectedTraceContext(e: propagation.TraceContext) = {
    e.traceIdHigh() shouldEqual traceIdHigh
    e.traceId() shouldEqual traceId
    e.spanId() shouldEqual spanId
    Option(e.parentId()) shouldEqual parentSpanId
    e.sampled() shouldEqual sampled
  }

  protected def requestMustFailWith(
      request: Future[_],
      code: Code,
      description: String): Future[Assertion] = {
    val f = request.map(Right(_)).recover { case ex: StatusRuntimeException => Left(ex) }
    f.map(inside(_)(isError(code, description)))
  }

  protected def requestMustFailWith(
      request: Either[StatusRuntimeException, _],
      code: Code,
      description: String): Assertion = {
    inside(request)(isError(code, description))
  }
  protected def isError(expectedCode: Code, expectedDescription: String)
    : PartialFunction[Either[StatusRuntimeException, _], Assertion] = {

    case Left(err) =>
      err.getStatus should have('code (expectedCode))
      err.getStatus should have('description (expectedDescription))

  }

}
