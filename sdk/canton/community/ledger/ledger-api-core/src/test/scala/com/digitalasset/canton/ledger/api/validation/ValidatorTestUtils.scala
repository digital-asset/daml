// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.grpc.GrpcStatus
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.messages.update
import com.digitalasset.canton.ledger.api.{CumulativeFilter, InterfaceFilter, TemplateFilter}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.rpc.error_details
import com.google.rpc.error_details.RetryInfo
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import org.scalatest.*
import org.scalatest.matchers.should.Matchers

import java.time.Duration

trait ValidatorTestUtils extends Matchers with Inside with OptionValues with EitherValues {
  self: Suite =>

  protected val includedModule = "includedModule"
  protected val includedTemplate = "includedTemplate"
  protected val expectedUserId = "expectedUserId"
  protected val packageName = Ref.PackageName.assertFromString("somePackageName")
  protected val packageNameRefEncoded = Ref.PackageRef.Name(packageName).toString
  protected val templateQualifiedName =
    Ref.QualifiedName.assertFromString(s"$includedModule:$includedTemplate")
  protected val packageId = Ref.PackageId.assertFromString("packageId")
  protected val packageId2 = Ref.PackageId.assertFromString("packageId2")
  protected val offsetLong = 42L
  protected val offset = Some(Offset.tryFromLong(offsetLong))
  protected val party = Ref.Party.assertFromString("party")
  protected val party2 = Ref.Party.assertFromString("party2")
  protected val verbose = false
  protected val updateId = "42"
  protected val ledgerEnd = Some(Offset.tryFromLong(1000))
  protected val contractId = ContractId.V1.assertFromString("00" * 32 + "0001")
  protected val moduleName = Ref.ModuleName.assertFromString(includedModule)
  protected val dottedName = Ref.DottedName.assertFromString(includedTemplate)
  protected val refTemplateId = Ref.Identifier(packageId, templateQualifiedName)
  protected val refTemplateId2 = Ref.Identifier(packageId2, templateQualifiedName)

  private val expectedTemplates = Set(
    Ref.TypeConRef(
      Ref.PackageRef.Id(Ref.PackageId.assertFromString(packageId)),
      Ref.QualifiedName(
        Ref.DottedName.assertFromString(includedModule),
        Ref.DottedName.assertFromString(includedTemplate),
      ),
    )
  )

  protected def hasExpectedFilters(
      req: update.GetUpdatesRequest,
      expectedTemplates: Set[Ref.TypeConRef] = expectedTemplates,
  ): Assertion = {
    val filtersByParty = req.updateFormat.includeTransactions.value.eventFormat.filtersByParty
    filtersByParty should have size 1
    inside(filtersByParty.headOption.value) { case (p, filters) =>
      p shouldEqual party
      filters shouldEqual
        CumulativeFilter(
          templateFilters =
            expectedTemplates.map(TemplateFilter(_, includeCreatedEventBlob = false)),
          interfaceFilters = Set(
            InterfaceFilter(
              interfaceTypeRef = Ref.TypeConRef(
                Ref.PackageRef.assertFromString(packageId),
                Ref.QualifiedName(
                  Ref.DottedName.assertFromString(includedModule),
                  Ref.DottedName.assertFromString(includedTemplate),
                ),
              ),
              includeView = true,
              includeCreatedEventBlob = true,
            )
          ),
          templateWildcardFilter = None,
        )
    }
  }

  protected def requestMustFailWith(
      request: Either[StatusRuntimeException, _],
      code: Code,
      description: String,
      metadata: Map[String, String] = Map.empty,
      retryDelay: Seq[Duration] = Seq.empty,
  ): Assertion =
    inside(request)(isError(code, description, metadata, retryDelay))
  protected def isError(
      expectedCode: Code,
      expectedDescription: String,
      metadata: Map[String, String],
      retryDelay: Seq[Duration],
  ): PartialFunction[Either[StatusRuntimeException, _], Assertion] = { case Left(err) =>
    err.getStatus should have(Symbol("code")(expectedCode))
    err.getStatus should have(Symbol("description")(expectedDescription))
    val details = GrpcStatus
      .toProto(err.getStatus, err.getTrailers)
      .details
    val errorInfoMetadata: Map[String, String] = details
      .collect {
        case any if any.is[error_details.ErrorInfo] =>
          any.unpack[error_details.ErrorInfo].metadata
      }
      .flatten
      .toMap
    errorInfoMetadata should contain allElementsOf metadata
    val retryInfoMetadata: Seq[Duration] = details.collect {
      case any if any.is[error_details.RetryInfo] =>
        val retryDelay = any
          .unpack[RetryInfo]
          .getRetryDelay
        ProtoConverter.DurationConverter.fromProtoPrimitive(retryDelay).value
    }
    retryInfoMetadata should contain allElementsOf retryDelay
  }

}
