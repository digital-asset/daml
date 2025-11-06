// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter as ProtoCumulativeFilter,
  InterfaceFilter as ProtoInterfaceFilter,
  TemplateFilter as ProtoTemplateFilter,
  *,
}
import com.daml.ledger.api.v2.update_service.{
  GetUpdateByIdRequest,
  GetUpdateByOffsetRequest,
  GetUpdatesRequest,
}
import com.daml.ledger.api.v2.value.Identifier
import com.digitalasset.canton.ledger.api.{CumulativeFilter, InterfaceFilter, TemplateFilter}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NoLogging}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.TypeConRef
import io.grpc.Status.Code.*
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AnyWordSpec

class UpdateServiceRequestValidatorTest
    extends AnyWordSpec
    with ValidatorTestUtils
    with MockitoSugar {
  private implicit val noLogging: ErrorLoggingContext = NoLogging

  private val templateId = Identifier(packageId, includedModule, includedTemplate)

  private def getFiltersByParty(templateIdsForParty: Seq[Identifier]): Map[String, Filters] =
    Map(
      party ->
        Filters(
          templateIdsForParty
            .map(tId =>
              ProtoCumulativeFilter(
                IdentifierFilter.TemplateFilter(
                  ProtoTemplateFilter(Some(tId), includeCreatedEventBlob = false)
                )
              )
            )
            ++
              Seq(
                ProtoCumulativeFilter(
                  IdentifierFilter.InterfaceFilter(
                    ProtoInterfaceFilter(
                      interfaceId = Some(
                        Identifier(
                          packageId,
                          moduleName = includedModule,
                          entityName = includedTemplate,
                        )
                      ),
                      includeInterfaceView = true,
                      includeCreatedEventBlob = true,
                    )
                  )
                )
              )
        )
    )

  private def updatesReqBuilder(
      transactionTemplateIdsO: Option[Seq[Identifier]],
      reassignmentsTemplateIdsO: Option[Seq[Identifier]] = None,
  ) =
    GetUpdatesRequest(
      beginExclusive = 0L,
      endInclusive = Some(offsetLong),
      updateFormat = Some(
        UpdateFormat(
          includeTransactions = transactionTemplateIdsO
            .map(getFiltersByParty)
            .map(filtersByParty =>
              TransactionFormat(
                eventFormat = Some(
                  EventFormat(
                    filtersByParty = filtersByParty,
                    filtersForAnyParty = None,
                    verbose = verbose,
                  )
                ),
                transactionShape = TransactionShape.TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
          includeReassignments = reassignmentsTemplateIdsO
            .map(getFiltersByParty)
            .map(filtersByParty =>
              EventFormat(
                filtersByParty = filtersByParty,
                filtersForAnyParty = None,
                verbose = false,
              )
            ),
          includeTopologyEvents = None,
        )
      ),
    )

  private val txReq = updatesReqBuilder(Some(Seq(templateId)))
  private val reassignmentsReq = updatesReqBuilder(
    transactionTemplateIdsO = None,
    reassignmentsTemplateIdsO = Some(Seq(templateId)),
  )
  private val txReqWithPackageNameScoping = updatesReqBuilder(
    Some(Seq(templateId.copy(packageId = Ref.PackageRef.Name(packageName).toString)))
  )

  private val txByOffsetReq =
    GetUpdateByOffsetRequest(
      offset = offsetLong,
      updateFormat = Some(
        UpdateFormat(
          includeTransactions = Some(
            TransactionFormat(
              eventFormat = Some(
                EventFormat(
                  filtersByParty = Map(party -> Filters(Nil)),
                  filtersForAnyParty = None,
                  verbose = false,
                )
              ),
              transactionShape = TransactionShape.TRANSACTION_SHAPE_ACS_DELTA,
            )
          ),
          includeReassignments = None,
          includeTopologyEvents = None,
        )
      ),
    )

  private val updateByIdReq =
    GetUpdateByIdRequest(
      updateId = updateId.toHexString,
      updateFormat = Some(
        UpdateFormat(
          includeTransactions = Some(
            TransactionFormat(
              eventFormat = Some(
                EventFormat(
                  filtersByParty = Map(party -> Filters(Nil)),
                  filtersForAnyParty = None,
                  verbose = false,
                )
              ),
              transactionShape = TransactionShape.TRANSACTION_SHAPE_ACS_DELTA,
            )
          ),
          includeReassignments = None,
          includeTopologyEvents = None,
        )
      ),
    )

  "UpdateRequestValidation" when {

    "validating regular requests" should {

      "accept simple requests" in {
        inside(UpdateServiceRequestValidator.validate(txReq, ledgerEnd)) { case Right(req) =>
          req.startExclusive shouldBe None
          req.endInclusive shouldBe offset
          val filtersByParty =
            req.updateFormat.includeTransactions.map(_.eventFormat.filtersByParty).value
          filtersByParty should have size 1
          hasExpectedFilters(req)
          req.updateFormat.includeTransactions.value.eventFormat.verbose shouldEqual verbose
        }

      }

      "accept simple requests for reassignments" in {
        inside(UpdateServiceRequestValidator.validate(reassignmentsReq, ledgerEnd)) {
          case Right(req) =>
            req.startExclusive shouldBe None
            req.endInclusive shouldBe offset
            val filtersByParty =
              req.updateFormat.includeReassignments.value.filtersByParty
            filtersByParty should have size 1
            req.updateFormat.includeReassignments.value.verbose shouldEqual verbose
        }

      }

      "return the correct error without filters in the event format of transactions" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validate(
            txReq.update(
              _.updateFormat.includeTransactions.eventFormat.filtersByParty := Map.empty
            ),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: filtersByParty and filtersForAnyParty cannot be empty simultaneously",
          metadata = Map.empty,
        )
      }

      "return the correct error without filters in the event format of reassignments" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validate(
            reassignmentsReq.update(
              _.updateFormat.includeReassignments.filtersByParty := Map.empty
            ),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: filtersByParty and filtersForAnyParty cannot be empty simultaneously",
          metadata = Map.empty,
        )
      }

      "return the correct error on empty interfaceId in interfaceFilter" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validate(
            txReq.update(
              _.updateFormat.includeTransactions.eventFormat.filtersByParty.modify(_.map {
                case (p, f) =>
                  p -> f.update(
                    _.cumulative := Seq(
                      ProtoCumulativeFilter(
                        IdentifierFilter.InterfaceFilter(
                          ProtoInterfaceFilter(
                            interfaceId = None,
                            includeInterfaceView = true,
                            includeCreatedEventBlob = false,
                          )
                        )
                      )
                    )
                  )
              })
            ),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: interfaceId",
          metadata = Map.empty,
        )
      }

      "return the correct error on empty templateId in templateFilter" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validate(
            txReq.update(
              _.updateFormat.includeTransactions.eventFormat.filtersByParty.modify(_.map {
                case (p, f) =>
                  p -> f.update(
                    _.cumulative := Seq(
                      ProtoCumulativeFilter(
                        IdentifierFilter.TemplateFilter(
                          ProtoTemplateFilter(templateId = None, includeCreatedEventBlob = true)
                        )
                      )
                    )
                  )
              })
            ),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: templateId",
          metadata = Map.empty,
        )
      }

      "return the correct error on unspecified transaction shape" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validate(
            txReq.update(
              _.updateFormat.includeTransactions.transactionShape := TransactionShape.TRANSACTION_SHAPE_UNSPECIFIED
            ),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: transaction_shape",
          metadata = Map.empty,
        )
      }

      "return the correct error on unrecognized transaction shape" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validate(
            txReq.update(
              _.updateFormat.includeTransactions.transactionShape :=
                TransactionShape.Unrecognized(unrecognizedValue = 4)
            ),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: transaction_shape is defined with invalid value 4",
          metadata = Map.empty,
        )
      }

      "return the correct error on invalid party for topology events" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validate(
            txReq.update(
              _.updateFormat.includeTopologyEvents.includeParticipantAuthorizationEvents.parties := Seq(
                "notParseableString@"
              )
            ),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field parties: non " +
              "expected character 0x40 in Daml-LF Party \"notParseableString@\"",
          metadata = Map.empty,
        )
      }

      "return the correct error when begin offset is after ledger end" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validate(
            txReq.withBeginExclusive(ledgerEnd.value.unwrap + 10L),
            ledgerEnd,
          ),
          code = OUT_OF_RANGE,
          description =
            s"OFFSET_AFTER_LEDGER_END(12,0): Begin offset (${ledgerEnd.value.unwrap + 10L}) is after ledger end (${ledgerEnd.value.unwrap})",
          metadata = Map.empty,
        )
      }

      "return the correct error when end offset is after ledger end" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validate(
            txReq.withEndInclusive(ledgerEnd.value.unwrap + 10),
            ledgerEnd,
          ),
          code = OUT_OF_RANGE,
          description =
            s"OFFSET_AFTER_LEDGER_END(12,0): End offset (${ledgerEnd.value.unwrap + 10L}) is after ledger end (${ledgerEnd.value.unwrap})",
          metadata = Map.empty,
        )
      }

      "return the correct error when begin offset is negative" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validate(
            txReq.withBeginExclusive(-100L),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "NEGATIVE_OFFSET(8,0): Offset -100 in begin_exclusive is a negative integer: " +
              "the offset in begin_exclusive field has to be a non-negative integer (>=0)",
          metadata = Map.empty,
        )
      }

      "return the correct error when end offset is zero" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validate(
            txReq.withEndInclusive(0L),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "NON_POSITIVE_OFFSET(8,0): Offset 0 in end_inclusive is not a positive integer: " +
              "the offset has to be either a positive integer (>0) or not defined at all",
          metadata = Map.empty,
        )
      }

      "return the correct error when end offset is negative" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validate(
            txReq.withEndInclusive(-100L),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "NON_POSITIVE_OFFSET(8,0): Offset -100 in end_inclusive is not a positive integer: " +
              "the offset has to be either a positive integer (>0) or not defined at all",
          metadata = Map.empty,
        )
      }

      "tolerate missing end" in {
        inside(
          UpdateServiceRequestValidator.validate(
            txReq.update(_.optionalEndInclusive := None),
            ledgerEnd,
          )
        ) { case Right(req) =>
          req.startExclusive shouldEqual None
          req.endInclusive shouldEqual None
          val filtersByParty =
            req.updateFormat.includeTransactions.map(_.eventFormat.filtersByParty).value
          filtersByParty should have size 1
          hasExpectedFilters(req)
          req.updateFormat.includeTransactions.value.eventFormat.verbose shouldEqual verbose
        }
      }

      "tolerate empty filters_inclusive" in {
        inside(
          UpdateServiceRequestValidator.validate(
            txReq.update(
              _.updateFormat.includeTransactions.eventFormat.filtersByParty.modify(_.map {
                case (p, f) =>
                  p -> f.update(_.cumulative := Seq(ProtoCumulativeFilter.defaultInstance))
              })
            ),
            ledgerEnd,
          )
        ) { case Right(req) =>
          req.startExclusive shouldEqual None
          req.endInclusive shouldEqual offset
          val filtersByParty =
            req.updateFormat.includeTransactions.map(_.eventFormat.filtersByParty).value
          filtersByParty should have size 1
          inside(filtersByParty.headOption.value) { case (p, filters) =>
            p shouldEqual party
            filters shouldEqual CumulativeFilter.templateWildcardFilter()
          }
          req.updateFormat.includeTransactions.value.eventFormat.verbose shouldEqual verbose
        }
      }

      "tolerate missing filters_inclusive" in {
        inside(
          UpdateServiceRequestValidator.validate(
            txReq.update(
              _.updateFormat.includeTransactions.eventFormat.filtersByParty.modify(_.map {
                case (p, f) =>
                  p -> f.update(_.cumulative := Seq())
              })
            ),
            ledgerEnd,
          )
        ) { case Right(req) =>
          req.startExclusive shouldEqual None
          req.endInclusive shouldEqual offset
          val filtersByParty =
            req.updateFormat.includeTransactions.map(_.eventFormat.filtersByParty).value
          filtersByParty should have size 1
          inside(filtersByParty.headOption.value) { case (p, filters) =>
            p shouldEqual party
            filters shouldEqual CumulativeFilter.templateWildcardFilter()
          }
          req.updateFormat.includeTransactions.value.eventFormat.verbose shouldEqual verbose
        }
      }

      "tolerate all fields filled out" in {
        inside(UpdateServiceRequestValidator.validate(txReq, ledgerEnd)) { case Right(req) =>
          req.startExclusive shouldEqual None
          req.endInclusive shouldEqual offset
          hasExpectedFilters(req)
          req.updateFormat.includeTransactions.value.eventFormat.verbose shouldEqual verbose
        }
      }

      "allow package-name scoped templates" in {
        inside(
          UpdateServiceRequestValidator.validate(txReqWithPackageNameScoping, ledgerEnd)
        ) { case Right(req) =>
          req.startExclusive shouldEqual None
          req.endInclusive shouldEqual offset
          hasExpectedFilters(
            req,
            expectedTemplates =
              Set(Ref.TypeConRef(Ref.PackageRef.Name(packageName), templateQualifiedName)),
          )
          req.updateFormat.includeTransactions.value.eventFormat.verbose shouldEqual verbose
        }
      }

      "still allow populated packageIds in templateIds (for backwards compatibility)" in {
        inside(UpdateServiceRequestValidator.validate(txReq, ledgerEnd)) { case Right(req) =>
          req.startExclusive shouldEqual None
          req.endInclusive shouldEqual offset
          hasExpectedFilters(req)
          req.updateFormat.includeTransactions.value.eventFormat.verbose shouldEqual verbose
        }
      }

      "current definition populate the right api request" in {
        val result = UpdateServiceRequestValidator.validate(
          updatesReqBuilder(Some(Seq.empty)).update(
            _.updateFormat.includeTransactions.eventFormat.filtersByParty :=
              Map(
                party -> Filters(
                  Seq(
                    ProtoCumulativeFilter(
                      IdentifierFilter.InterfaceFilter(
                        ProtoInterfaceFilter(
                          interfaceId = Some(templateId),
                          includeInterfaceView = true,
                          includeCreatedEventBlob = true,
                        )
                      )
                    )
                  )
                    ++
                      Seq(
                        ProtoCumulativeFilter(
                          IdentifierFilter.TemplateFilter(
                            ProtoTemplateFilter(Some(templateId), includeCreatedEventBlob = true)
                          )
                        )
                      )
                )
              )
          ),
          ledgerEnd,
        )
        result.map(
          _.updateFormat.includeTransactions.value.eventFormat.filtersByParty
        ) shouldBe Right(
          Map(
            party ->
              CumulativeFilter(
                templateFilters = Set(
                  TemplateFilter(
                    TypeConRef.assertFromString("packageId:includedModule:includedTemplate"),
                    includeCreatedEventBlob = true,
                  )
                ),
                interfaceFilters = Set(
                  InterfaceFilter(
                    interfaceTypeRef = Ref.TypeConRef.assertFromString(
                      "packageId:includedModule:includedTemplate"
                    ),
                    includeView = true,
                    includeCreatedEventBlob = true,
                  )
                ),
                templateWildcardFilter = None,
              )
          )
        )
      }
    }

    "validating transaction by id requests" should {

      "fail on empty updateId" in {
        requestMustFailWith(
          request =
            UpdateServiceRequestValidator.validateUpdateById(updateByIdReq.withUpdateId("")),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: update_id",
          metadata = Map.empty,
        )
      }

      "fail on empty update format" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validateUpdateById(
            updateByIdReq.clearUpdateFormat
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: update_format",
          metadata = Map.empty,
        )
      }
    }

    "validating transaction by offset requests" should {

      "fail on zero offset" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validateUpdateByOffset(
            txByOffsetReq.withOffset(0)
          ),
          code = INVALID_ARGUMENT,
          description =
            "NON_POSITIVE_OFFSET(8,0): Offset 0 in offset is not a positive integer: the offset has to be a positive integer (>0)",
          metadata = Map.empty,
        )
      }

      "fail on negative offset" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validateUpdateByOffset(
            txByOffsetReq.withOffset(-21)
          ),
          code = INVALID_ARGUMENT,
          description =
            "NON_POSITIVE_OFFSET(8,0): Offset -21 in offset is not a positive integer: the offset has to be a positive integer (>0)",
          metadata = Map.empty,
        )
      }

      "fail on empty update format" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validateUpdateByOffset(
            txByOffsetReq.clearUpdateFormat
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: update_format",
          metadata = Map.empty,
        )
      }

    }
  }
}
