// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.grpc.GrpcStatus
import com.daml.lf.data.Ref
import com.daml.lf.value.Value.ContractId
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.{InterfaceFilter, TemplateFilter}
import com.digitalasset.canton.ledger.api.messages.transaction
import com.google.rpc.error_details
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import org.scalatest.*
import org.scalatest.matchers.should.Matchers

trait ValidatorTestUtils extends Matchers with Inside with OptionValues {
  self: Suite =>

  protected val includedModule = "includedModule"
  protected val includedTemplate = "includedTemplate"
  protected val expectedLedgerId = "expectedLedgerId"
  protected val expectedApplicationId = "expectedApplicationId"
  protected val templateQualifiedName =
    Ref.QualifiedName.assertFromString(s"$includedModule:$includedTemplate")
  protected val packageId = Ref.PackageId.assertFromString("packageId")
  protected val packageId2 = Ref.PackageId.assertFromString("packageId2")
  protected val absoluteOffset = Ref.LedgerString.assertFromString("0042")
  protected val party = Ref.Party.assertFromString("party")
  protected val party2 = Ref.Party.assertFromString("party2")
  protected val verbose = false
  protected val eventId = "eventId"
  protected val transactionId = "42"
  protected val ledgerEnd = domain.LedgerOffset.Absolute(Ref.LedgerString.assertFromString("1000"))
  protected val contractId = ContractId.V1.assertFromString("00" * 32 + "0001")
  protected val moduleName = Ref.ModuleName.assertFromString(includedModule)
  protected val dottedName = Ref.DottedName.assertFromString(includedTemplate)
  protected val refTemplateId = Ref.Identifier(packageId, templateQualifiedName)
  protected val refTemplateId2 = Ref.Identifier(packageId2, templateQualifiedName)

  private val expectedTemplateIds = Set(
    Ref.Identifier(
      Ref.PackageId.assertFromString(packageId),
      Ref.QualifiedName(
        Ref.DottedName.assertFromString(includedModule),
        Ref.DottedName.assertFromString(includedTemplate),
      ),
    )
  )

  protected def hasExpectedFilters(
      req: transaction.GetTransactionsRequest,
      expectedTemplateIds: Set[Ref.Identifier] = expectedTemplateIds,
  ) = {
    val filtersByParty = req.filter.filtersByParty
    filtersByParty should have size 1
    inside(filtersByParty.headOption.value) { case (p, filters) =>
      p shouldEqual party
      filters shouldEqual domain.Filters(
        Some(
          domain.InclusiveFilters(
            templateFilters =
              expectedTemplateIds.map(TemplateFilter(_, includeCreatedEventBlob = false)),
            interfaceFilters = Set(
              InterfaceFilter(
                interfaceId = Ref.Identifier(
                  Ref.PackageId.assertFromString(packageId),
                  Ref.QualifiedName(
                    Ref.DottedName.assertFromString(includedModule),
                    Ref.DottedName.assertFromString(includedTemplate),
                  ),
                ),
                includeView = true,
                includeCreatedEventBlob = true,
              )
            ),
          )
        )
      )
    }
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
