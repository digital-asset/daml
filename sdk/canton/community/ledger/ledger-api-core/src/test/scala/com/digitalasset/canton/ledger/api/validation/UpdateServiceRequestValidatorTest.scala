// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter as ProtoCumulativeFilter,
  Filters,
  InterfaceFilter as ProtoInterfaceFilter,
  TemplateFilter as ProtoTemplateFilter,
  *,
}
import com.daml.ledger.api.v2.update_service.{
  GetTransactionByIdRequest,
  GetTransactionByOffsetRequest,
  GetUpdatesRequest,
}
import com.daml.ledger.api.v2.value.Identifier
import com.digitalasset.canton.ledger.api.{CumulativeFilter, InterfaceFilter, TemplateFilter}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.TypeConRef
import io.grpc.Status.Code.*
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AnyWordSpec

class UpdateServiceRequestValidatorTest
    extends AnyWordSpec
    with ValidatorTestUtils
    with MockitoSugar {
  private implicit val noLogging: ContextualizedErrorLogger = NoLogging

  private val templateId = Identifier(packageId, includedModule, includedTemplate)

  private def txReqBuilderLegacy(templateIdsForParty: Seq[Identifier]) = GetUpdatesRequest(
    beginExclusive = 0L,
    endInclusive = Some(offsetLong),
    filter = Some(
      TransactionFilter(
        Map(
          party ->
            Filters(
              templateIdsForParty
                .map(tId =>
                  ProtoCumulativeFilter(
                    IdentifierFilter.TemplateFilter(ProtoTemplateFilter(Some(tId)))
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
      )
    ),
    verbose = verbose,
  )

  private def getFiltersByParty(templateIdsForParty: Seq[Identifier]): Map[String, Filters] =
    Map(
      party ->
        Filters(
          templateIdsForParty
            .map(tId =>
              ProtoCumulativeFilter(
                IdentifierFilter.TemplateFilter(ProtoTemplateFilter(Some(tId)))
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
                    verbose = verbose,
                  )
                ),
                transactionShape = TransactionShape.TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
          includeReassignments = reassignmentsTemplateIdsO
            .map(getFiltersByParty)
            .map(filtersByParty => EventFormat(filtersByParty = filtersByParty)),
        )
      ),
    )

  private val txReqLegacy = txReqBuilderLegacy(Seq(templateId))
  private val txReqLegacyWithPackageNameScoping = txReqBuilderLegacy(
    Seq(templateId.copy(packageId = Ref.PackageRef.Name(packageName).toString))
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
    GetTransactionByOffsetRequest(offsetLong, Seq(party))

  private val txByIdReq =
    GetTransactionByIdRequest(updateId, Seq(party))

  "UpdateRequestValidation" when {

    // TODO(#23504) remove
    "validating regular legacy requests" should {

      "accept simple requests" in {
        inside(UpdateServiceRequestValidator.validate(txReqLegacy, ledgerEnd)) { case Right(req) =>
          req.startExclusive shouldBe None
          req.endInclusive shouldBe offset
          val filtersByParty =
            req.updateFormat.includeTransactions.map(_.eventFormat.filtersByParty).value
          filtersByParty should have size 1
          hasExpectedFilters(req)
          req.updateFormat.includeTransactions.value.eventFormat.verbose shouldEqual verbose
        }

      }

      "return the correct error on missing filter" in {
        requestMustFailWith(
          UpdateServiceRequestValidator
            .validateForTrees(txReqLegacy.update(_.optionalFilter := None), ledgerEnd),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: filter",
          metadata = Map.empty,
        )
      }

      "return the correct error on missing filter/verbose and update_format" in {
        requestMustFailWith(
          UpdateServiceRequestValidator
            .validate(txReqLegacy.update(_.optionalFilter := None), ledgerEnd),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: Either filter/verbose or update_format is required. Please use either backwards compatible arguments (filter and verbose) or update_format, but not both.",
          metadata = Map.empty,
        )
      }

      "return the correct error on defining both filter and update_format" in {
        requestMustFailWith(
          UpdateServiceRequestValidator
            .validate(
              txReqLegacy.update(_.optionalUpdateFormat := txReq.updateFormat, _.verbose := false),
              ledgerEnd,
            ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: Both filter/verbose and update_format is specified. Please use either backwards compatible arguments (filter and verbose) or update_format, but not both.",
          metadata = Map.empty,
        )
      }

      "return the correct error on defining both verbose and update_format" in {
        requestMustFailWith(
          UpdateServiceRequestValidator
            .validate(
              txReq.update(_.verbose := true),
              ledgerEnd,
            ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: Both filter/verbose and update_format is specified. Please use either backwards compatible arguments (filter and verbose) or update_format, but not both.",
          metadata = Map.empty,
        )
      }

      "return the correct error on empty filter" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validate(
            txReqLegacy.update(_.filter.filtersByParty := Map.empty),
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
            txReqLegacy.update(_.filter.filtersByParty.modify(_.map { case (p, f) =>
              p -> f.update(
                _.cumulative := Seq(
                  ProtoCumulativeFilter(
                    IdentifierFilter.InterfaceFilter(ProtoInterfaceFilter(None, true))
                  )
                )
              )
            })),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: interfaceId",
          metadata = Map.empty,
        )
      }

      "tolerate empty filters_inclusive" in {
        inside(
          UpdateServiceRequestValidator.validate(
            txReqLegacy.update(_.filter.filtersByParty.modify(_.map { case (p, f) =>
              p -> f.update(_.cumulative := Seq(ProtoCumulativeFilter.defaultInstance))
            })),
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
            txReqLegacy.update(_.filter.filtersByParty.modify(_.map { case (p, f) =>
              p -> f.update(_.cumulative := Seq())
            })),
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
        inside(UpdateServiceRequestValidator.validate(txReqLegacy, ledgerEnd)) { case Right(req) =>
          req.startExclusive shouldEqual None
          req.endInclusive shouldEqual offset
          hasExpectedFilters(req)
          req.updateFormat.includeTransactions.value.eventFormat.verbose shouldEqual verbose
        }
      }

      "allow package-name scoped templates" in {
        inside(
          UpdateServiceRequestValidator.validate(txReqLegacyWithPackageNameScoping, ledgerEnd)
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
        inside(UpdateServiceRequestValidator.validate(txReqLegacy, ledgerEnd)) { case Right(req) =>
          req.startExclusive shouldEqual None
          req.endInclusive shouldEqual offset
          hasExpectedFilters(req)
          req.updateFormat.includeTransactions.value.eventFormat.verbose shouldEqual verbose
        }
      }

      "current definition populate the right api request" in {
        val result = UpdateServiceRequestValidator.validate(
          txReqBuilderLegacy(Seq.empty).copy(
            filter = Some(
              TransactionFilter(
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
                              ProtoTemplateFilter(Some(templateId), true)
                            )
                          )
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
                    true,
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
                          ProtoInterfaceFilter(interfaceId = None, includeInterfaceView = true)
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
                            ProtoTemplateFilter(Some(templateId), true)
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
                    true,
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

      "fail on empty transactionId" in {
        requestMustFailWith(
          request =
            UpdateServiceRequestValidator.validateTransactionById(txByIdReq.withUpdateId("")),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: update_id",
          metadata = Map.empty,
        )
      }

      "fail on empty requesting parties" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validateTransactionById(
            txByIdReq.withRequestingParties(Nil)
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: requesting_parties",
          metadata = Map.empty,
        )
      }

    }

    "validating transaction by offset requests" should {

      "fail on zero offset" in {
        requestMustFailWith(
          request =
            UpdateServiceRequestValidator.validateTransactionByOffset(txByOffsetReq.withOffset(0)),
          code = INVALID_ARGUMENT,
          description =
            "NON_POSITIVE_OFFSET(8,0): Offset 0 in offset is not a positive integer: the offset has to be a positive integer (>0)",
          metadata = Map.empty,
        )
      }

      "fail on negative offset" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validateTransactionByOffset(
            txByOffsetReq.withOffset(-21)
          ),
          code = INVALID_ARGUMENT,
          description =
            "NON_POSITIVE_OFFSET(8,0): Offset -21 in offset is not a positive integer: the offset has to be a positive integer (>0)",
          metadata = Map.empty,
        )
      }

      "fail on empty requesting parties" in {
        requestMustFailWith(
          request = UpdateServiceRequestValidator.validateTransactionByOffset(
            txByOffsetReq.withRequestingParties(Nil)
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: requesting_parties",
          metadata = Map.empty,
        )
      }

    }
  }
}
