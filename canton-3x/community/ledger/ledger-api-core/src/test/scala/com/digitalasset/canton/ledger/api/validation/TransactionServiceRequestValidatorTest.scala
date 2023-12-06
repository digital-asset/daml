// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary
import com.daml.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  InterfaceFilter,
  TemplateFilter,
  TransactionFilter,
}
import com.daml.ledger.api.v1.transaction_service.{
  GetLedgerEndRequest,
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionsRequest,
}
import com.daml.ledger.api.v1.value.Identifier
import com.daml.lf.data.Ref
import com.digitalasset.canton.ledger.api.domain
import io.grpc.Status.Code.*
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AnyWordSpec

class TransactionServiceRequestValidatorTest
    extends AnyWordSpec
    with ValidatorTestUtils
    with MockitoSugar {
  private implicit val noLogging: ContextualizedErrorLogger = NoLogging

  private val templateId = Identifier(packageId, includedModule, includedTemplate)

  private def txReqBuilder(templateIdsForParty: Seq[Identifier]) = GetTransactionsRequest(
    expectedLedgerId,
    Some(LedgerOffset(LedgerOffset.Value.Boundary(LedgerBoundary.LEDGER_BEGIN))),
    Some(LedgerOffset(LedgerOffset.Value.Absolute(absoluteOffset))),
    Some(
      TransactionFilter(
        Map(
          party ->
            Filters(
              Some(
                InclusiveFilters(
                  templateFilters = templateIdsForParty.map(tId => TemplateFilter(Some(tId))),
                  interfaceFilters = Seq(
                    InterfaceFilter(
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
                  ),
                )
              )
            )
        )
      )
    ),
    verbose,
  )
  private val txReq = txReqBuilder(Seq(templateId))
  private val txReqWithMissingPackageId = txReqBuilder(Seq(templateId.copy(packageId = "")))

  private val txTreeReq = GetTransactionsRequest(
    expectedLedgerId,
    Some(LedgerOffset(LedgerOffset.Value.Boundary(LedgerBoundary.LEDGER_BEGIN))),
    Some(LedgerOffset(LedgerOffset.Value.Absolute(absoluteOffset))),
    Some(TransactionFilter(Map(party -> Filters.defaultInstance))),
    verbose,
  )

  private val endReq = GetLedgerEndRequest(expectedLedgerId)

  private val txByEvIdReq =
    GetTransactionByEventIdRequest(expectedLedgerId, eventId, Seq(party))

  private val txByIdReq =
    GetTransactionByIdRequest(expectedLedgerId, transactionId, Seq(party))

  private val transactionFilterValidator = new TransactionFilterValidator(
    upgradingEnabled = false,
    resolveTemplateIds = _ => fail("Code path should not be exercised"),
  )

  private val validator = new TransactionServiceRequestValidator(
    domain.LedgerId(expectedLedgerId),
    new PartyValidator(PartyNameChecker.AllowAllParties),
    transactionFilterValidator,
  )

  private val transactionFilterValidatorUpgradingEnabled = new TransactionFilterValidator(
    upgradingEnabled = true,
    resolveTemplateIds = {
      case `templateQualifiedName` =>
        _ => Right(Set(refTemplateId, refTemplateId2))
      case _ =>
        _ =>
          Left(
            io.grpc.Status.NOT_FOUND
              .augmentDescription("template qualified name not resolved!")
              .asRuntimeException()
          )
    },
  )

  private val validatorUpgradingEnabled = new TransactionServiceRequestValidator(
    domain.LedgerId(expectedLedgerId),
    new PartyValidator(PartyNameChecker.AllowAllParties),
    transactionFilterValidatorUpgradingEnabled,
  )

  "TransactionRequestValidation" when {

    "validating regular requests" should {

      "accept requests with empty ledger ID" in {
        inside(validator.validate(txReq.withLedgerId(""), ledgerEnd)) { case Right(req) =>
          req.ledgerId shouldEqual None
          req.startExclusive shouldEqual domain.LedgerOffset.LedgerBegin
          req.endInclusive shouldEqual Some(domain.LedgerOffset.Absolute(absoluteOffset))
          val filtersByParty = req.filter.filtersByParty
          filtersByParty should have size 1
          hasExpectedFilters(req)
          req.verbose shouldEqual verbose
        }

      }

      "return the correct error on missing filter" in {
        requestMustFailWith(
          validator.validate(txReq.update(_.optionalFilter := None), ledgerEnd),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: filter",
          metadata = Map.empty,
        )
      }

      "return the correct error on empty filter" in {
        requestMustFailWith(
          request = validator.validate(
            txReq.update(_.filter.filtersByParty := Map.empty),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: filtersByParty cannot be empty",
          metadata = Map.empty,
        )
      }

      "return the correct error on empty interfaceId in interfaceFilter" in {
        requestMustFailWith(
          request = validator.validate(
            txReq.update(_.filter.filtersByParty.modify(_.map { case (p, f) =>
              p -> f.update(_.inclusive := InclusiveFilters(Nil, Seq(InterfaceFilter(None, true))))
            })),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: interfaceId",
          metadata = Map.empty,
        )
      }

      "return the correct error on missing begin" in {
        requestMustFailWith(
          request = validator.validate(txReq.update(_.optionalBegin := None), ledgerEnd),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: begin",
          metadata = Map.empty,
        )
      }

      "return the correct error on empty begin " in {
        requestMustFailWith(
          request = validator.validate(txReq.update(_.begin := LedgerOffset()), ledgerEnd),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: begin.(boundary|value)",
          metadata = Map.empty,
        )
      }

      "return the correct error on empty end " in {
        requestMustFailWith(
          request = validator.validate(txReq.withEnd(LedgerOffset()), ledgerEnd),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: end.(boundary|value)",
          metadata = Map.empty,
        )
      }

      "return the correct error on unknown begin boundary" in {
        requestMustFailWith(
          request = validator.validate(
            txReq.withBegin(
              LedgerOffset(LedgerOffset.Value.Boundary(LedgerBoundary.Unrecognized(7)))
            ),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown ledger boundary value '7' in field begin.boundary",
          metadata = Map.empty,
        )
      }

      "return the correct error on unknown end boundary" in {
        requestMustFailWith(
          request = validator.validate(
            txReq.withEnd(
              LedgerOffset(LedgerOffset.Value.Boundary(LedgerBoundary.Unrecognized(7)))
            ),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown ledger boundary value '7' in field end.boundary",
          metadata = Map.empty,
        )
      }

      "return the correct error when begin offset is after ledger end" in {
        requestMustFailWith(
          request = validator.validate(
            txReq.withBegin(
              LedgerOffset(LedgerOffset.Value.Absolute((ledgerEnd.value.toInt + 1).toString))
            ),
            ledgerEnd,
          ),
          code = OUT_OF_RANGE,
          description =
            "OFFSET_AFTER_LEDGER_END(12,0): Begin offset (1001) is after ledger end (1000)",
          metadata = Map.empty,
        )
      }

      "return the correct error when end offset is after ledger end" in {
        requestMustFailWith(
          request = validator.validate(
            txReq.withEnd(
              LedgerOffset(LedgerOffset.Value.Absolute((ledgerEnd.value.toInt + 1).toString))
            ),
            ledgerEnd,
          ),
          code = OUT_OF_RANGE,
          description =
            "OFFSET_AFTER_LEDGER_END(12,0): End offset (1001) is after ledger end (1000)",
          metadata = Map.empty,
        )
      }

      "tolerate missing end" in {
        inside(validator.validate(txReq.update(_.optionalEnd := None), ledgerEnd)) {
          case Right(req) =>
            req.ledgerId shouldEqual Some(expectedLedgerId)
            req.startExclusive shouldEqual domain.LedgerOffset.LedgerBegin
            req.endInclusive shouldEqual None
            val filtersByParty = req.filter.filtersByParty
            filtersByParty should have size 1
            hasExpectedFilters(req)
            req.verbose shouldEqual verbose
        }
      }

      "tolerate empty filters_inclusive" in {
        inside(
          validator.validate(
            txReq.update(_.filter.filtersByParty.modify(_.map { case (p, f) =>
              p -> f.update(_.inclusive := InclusiveFilters(Nil, Nil))
            })),
            ledgerEnd,
          )
        ) { case Right(req) =>
          req.ledgerId shouldEqual Some(expectedLedgerId)
          req.startExclusive shouldEqual domain.LedgerOffset.LedgerBegin
          req.endInclusive shouldEqual Some(domain.LedgerOffset.Absolute(absoluteOffset))
          val filtersByParty = req.filter.filtersByParty
          filtersByParty should have size 1
          inside(filtersByParty.headOption.value) { case (p, filters) =>
            p shouldEqual party
            filters shouldEqual domain.Filters(Some(domain.InclusiveFilters(Set(), Set())))
          }
          req.verbose shouldEqual verbose
        }
      }

      "tolerate missing filters_inclusive" in {
        inside(
          validator.validate(
            txReq.update(_.filter.filtersByParty.modify(_.map { case (p, f) =>
              p -> f.update(_.optionalInclusive := None)
            })),
            ledgerEnd,
          )
        ) { case Right(req) =>
          req.ledgerId shouldEqual Some(expectedLedgerId)
          req.startExclusive shouldEqual domain.LedgerOffset.LedgerBegin
          req.endInclusive shouldEqual Some(domain.LedgerOffset.Absolute(absoluteOffset))
          val filtersByParty = req.filter.filtersByParty
          filtersByParty should have size 1
          inside(filtersByParty.headOption.value) { case (p, filters) =>
            p shouldEqual party
            filters shouldEqual domain.Filters(None)
          }
          req.verbose shouldEqual verbose
        }
      }

      "tolerate all fields filled out" in {
        inside(validator.validate(txReq, ledgerEnd)) { case Right(req) =>
          req.ledgerId shouldEqual Some(expectedLedgerId)
          req.startExclusive shouldEqual domain.LedgerOffset.LedgerBegin
          req.endInclusive shouldEqual Some(domain.LedgerOffset.Absolute(absoluteOffset))
          hasExpectedFilters(req)
          req.verbose shouldEqual verbose
        }
      }

      "when upgrading enabled" should {
        "still allow populated packageIds in templateIds (for backwards compatibility)" in {
          inside(validatorUpgradingEnabled.validate(txReq, ledgerEnd)) { case Right(req) =>
            req.ledgerId shouldEqual Some(expectedLedgerId)
            req.startExclusive shouldEqual domain.LedgerOffset.LedgerBegin
            req.endInclusive shouldEqual Some(domain.LedgerOffset.Absolute(absoluteOffset))
            hasExpectedFilters(req)
            req.verbose shouldEqual verbose
          }
        }

        "resolve missing packageIds" in {
          inside(validatorUpgradingEnabled.validate(txReqWithMissingPackageId, ledgerEnd)) {
            case Right(req) =>
              req.ledgerId shouldEqual Some(expectedLedgerId)
              req.startExclusive shouldEqual domain.LedgerOffset.LedgerBegin
              req.endInclusive shouldEqual Some(domain.LedgerOffset.Absolute(absoluteOffset))
              val expectedTemplateIds =
                Iterator(packageId, packageId2)
                  .map(pkgId =>
                    Ref.Identifier(
                      Ref.PackageId.assertFromString(pkgId),
                      Ref.QualifiedName(
                        Ref.DottedName.assertFromString(includedModule),
                        Ref.DottedName.assertFromString(includedTemplate),
                      ),
                    )
                  )
                  .toSet

              hasExpectedFilters(req, expectedTemplateIds)
              req.verbose shouldEqual verbose
          }
        }

        "forward resolution error from resolver" in {
          requestMustFailWith(
            request = validatorUpgradingEnabled.validate(
              txReqBuilder(Seq(Identifier("", "unknownModule", "unknownEntity"))),
              ledgerEnd,
            ),
            code = NOT_FOUND,
            description = "template qualified name not resolved!",
            metadata = Map.empty,
          )
        }
      }

      "not allow mixed (deprecated and current) definitions between parties: one has templateIds, the other has templateFilters" in {
        requestMustFailWith(
          request = validator.validate(
            txReqBuilder(Seq.empty).copy(
              filter = Some(
                TransactionFilter(
                  Map(
                    // deprecated
                    party -> Filters(
                      Some(
                        InclusiveFilters(
                          templateIds = Seq(templateId),
                          interfaceFilters = Seq.empty,
                          templateFilters = Seq.empty,
                        )
                      )
                    ),
                    // current
                    party2 -> Filters(
                      Some(
                        InclusiveFilters(
                          templateIds = Seq.empty,
                          interfaceFilters = Seq.empty,
                          templateFilters = Seq(TemplateFilter(Some(templateId), false)),
                        )
                      )
                    ),
                  )
                )
              )
            ),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Transaction filter should be defined entirely either with deprecated fields, or with non-deprecated fields. Mixed definition is not allowed.",
          metadata = Map.empty,
        )
      }

      "not allow mixed (deprecated and current) definitions between parties: one has templateIds, the other has interfaceFilter.includeCreatedEventBlob" in {
        requestMustFailWith(
          request = validator.validate(
            txReqBuilder(Seq.empty).copy(
              filter = Some(
                TransactionFilter(
                  Map(
                    // deprecated
                    party -> Filters(
                      Some(
                        InclusiveFilters(
                          templateIds = Seq(templateId),
                          templateFilters = Seq.empty,
                        )
                      )
                    ),
                    // current
                    party2 -> Filters(
                      Some(
                        InclusiveFilters(
                          templateIds = Seq.empty,
                          interfaceFilters = Seq(
                            InterfaceFilter(
                              interfaceId = Some(templateId),
                              includeInterfaceView = false,
                              includeCreatedEventBlob = true,
                            )
                          ),
                          templateFilters = Seq.empty,
                        )
                      )
                    ),
                  )
                )
              )
            ),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Transaction filter should be defined entirely either with deprecated fields, or with non-deprecated fields. Mixed definition is not allowed.",
          metadata = Map.empty,
        )
      }

      "not allow mixed (deprecated and current) definitions within one InclusiveFilter: templateIds and templateFilters" in {
        requestMustFailWith(
          request = validator.validate(
            txReqBuilder(Seq.empty).copy(
              filter = Some(
                TransactionFilter(
                  Map(
                    party -> Filters(
                      Some(
                        InclusiveFilters(
                          templateIds = Seq(templateId),
                          interfaceFilters = Seq.empty,
                          templateFilters = Seq(TemplateFilter(Some(templateId), false)),
                        )
                      )
                    )
                  )
                )
              )
            ),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Transaction filter should be defined entirely either with deprecated fields, or with non-deprecated fields. Mixed definition is not allowed.",
          metadata = Map.empty,
        )
      }

      "current definition populate the right domain request" in {
        val result = validator.validate(
          txReqBuilder(Seq.empty).copy(
            filter = Some(
              TransactionFilter(
                Map(
                  party -> Filters(
                    Some(
                      InclusiveFilters(
                        templateIds = Seq.empty,
                        interfaceFilters = Seq(
                          InterfaceFilter(
                            interfaceId = Some(templateId),
                            includeInterfaceView = true,
                            includeCreatedEventBlob = true,
                          )
                        ),
                        templateFilters = Seq(TemplateFilter(Some(templateId), true)),
                      )
                    )
                  )
                )
              )
            )
          ),
          ledgerEnd,
        )
        result.map(_.filter.filtersByParty) shouldBe Right(
          Map(
            party -> domain.Filters(
              Some(
                domain.InclusiveFilters(
                  templateFilters = Set(
                    domain.TemplateFilter(
                      Ref.Identifier.assertFromString("packageId:includedModule:includedTemplate"),
                      true,
                    )
                  ),
                  interfaceFilters = Set(
                    domain.InterfaceFilter(
                      interfaceId = Ref.Identifier.assertFromString(
                        "packageId:includedModule:includedTemplate"
                      ),
                      includeView = true,
                      includeCreatedEventBlob = true,
                    )
                  ),
                )
              )
            )
          )
        )
      }
    }

    "validating tree requests" should {

      "tolerate missing filters_inclusive" in {
        inside(validator.validateTree(txTreeReq, ledgerEnd)) { case Right(req) =>
          req.ledgerId shouldEqual Some(expectedLedgerId)
          req.startExclusive shouldEqual domain.LedgerOffset.LedgerBegin
          req.endInclusive shouldEqual Some(domain.LedgerOffset.Absolute(absoluteOffset))
          req.parties should have size 1
          req.parties.headOption.value shouldEqual party
          req.verbose shouldEqual verbose
        }
      }

      "not tolerate having filters_inclusive" in {
        requestMustFailWith(
          request = validator.validateTree(
            txTreeReq.update(_.filter.filtersByParty.modify(_.map { case (p, f) =>
              p -> f.update(_.optionalInclusive := Some(InclusiveFilters()))
            })),
            ledgerEnd,
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: party attempted subscription for templates []. Template filtration is not supported on GetTransactionTrees RPC. To get filtered data, use the GetTransactions RPC.",
          metadata = Map.empty,
        )
      }

      "return the correct error when begin offset is after ledger end" in {
        requestMustFailWith(
          request = validator.validateTree(
            txTreeReq.withBegin(
              LedgerOffset(LedgerOffset.Value.Absolute((ledgerEnd.value.toInt + 1).toString))
            ),
            ledgerEnd,
          ),
          code = OUT_OF_RANGE,
          description =
            "OFFSET_AFTER_LEDGER_END(12,0): Begin offset (1001) is after ledger end (1000)",
          metadata = Map.empty,
        )
      }

      "return the correct error when end offset is after ledger end" in {
        requestMustFailWith(
          request = validator.validateTree(
            txTreeReq.withEnd(
              LedgerOffset(LedgerOffset.Value.Absolute((ledgerEnd.value.toInt + 1).toString))
            ),
            ledgerEnd,
          ),
          code = OUT_OF_RANGE,
          description =
            "OFFSET_AFTER_LEDGER_END(12,0): End offset (1001) is after ledger end (1000)",
          metadata = Map.empty,
        )
      }
    }

    "validating ledger end requests" should {

      "fail on ledger ID mismatch" in {
        requestMustFailWith(
          request = validator.validateLedgerEnd(endReq.withLedgerId("mismatchedLedgerId")),
          code = NOT_FOUND,
          description =
            "LEDGER_ID_MISMATCH(11,0): Ledger ID 'mismatchedLedgerId' not found. Actual Ledger ID is 'expectedLedgerId'.",
          metadata = Map.empty,
        )
      }

      "succeed validating a correct request" in {
        inside(validator.validateLedgerEnd(endReq)) { case Right(_) =>
          succeed
        }
      }
    }

    "validating transaction by id requests" should {

      "fail on ledger ID mismatch" in {
        requestMustFailWith(
          request = validator.validateTransactionById(txByIdReq.withLedgerId("mismatchedLedgerId")),
          code = NOT_FOUND,
          description =
            "LEDGER_ID_MISMATCH(11,0): Ledger ID 'mismatchedLedgerId' not found. Actual Ledger ID is 'expectedLedgerId'.",
          metadata = Map.empty,
        )
      }

      "fail on empty transactionId" in {
        requestMustFailWith(
          request = validator.validateTransactionById(txByIdReq.withTransactionId("")),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: transaction_id",
          metadata = Map.empty,
        )
      }

      "fail on empty requesting parties" in {
        requestMustFailWith(
          request = validator.validateTransactionById(txByIdReq.withRequestingParties(Nil)),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: requesting_parties",
          metadata = Map.empty,
        )
      }

      "return passed ledger ID" in {
        inside(validator.validateTransactionById(txByIdReq)) { case Right(out) =>
          out should have(Symbol("ledgerId")(Some(expectedLedgerId)))
        }
      }

    }

    "validating transaction by event id requests" should {

      "fail on ledger ID mismatch" in {
        requestMustFailWith(
          request =
            validator.validateTransactionByEventId(txByEvIdReq.withLedgerId("mismatchedLedgerId")),
          code = NOT_FOUND,
          description =
            "LEDGER_ID_MISMATCH(11,0): Ledger ID 'mismatchedLedgerId' not found. Actual Ledger ID is 'expectedLedgerId'.",
          metadata = Map.empty,
        )
      }

      "fail on empty eventId" in {
        requestMustFailWith(
          request = validator.validateTransactionByEventId(txByEvIdReq.withEventId("")),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: event_id",
          metadata = Map.empty,
        )
      }

      "fail on empty requesting parties" in {
        requestMustFailWith(
          request = validator.validateTransactionByEventId(txByEvIdReq.withRequestingParties(Nil)),
          code = INVALID_ARGUMENT,
          description =
            "MISSING_FIELD(8,0): The submitted command is missing a mandatory field: requesting_parties",
          metadata = Map.empty,
        )
      }

      "return passed ledger ID" in {
        inside(
          validator.validateTransactionByEventId(txByEvIdReq)
        ) { case Right(out) =>
          out should have(Symbol("ledgerId")(Some(expectedLedgerId)))
        }
      }

    }

    "applying party name checks" should {

      val partyRestrictiveValidator = new TransactionServiceRequestValidator(
        domain.LedgerId(expectedLedgerId),
        new PartyValidator(PartyNameChecker.AllowPartySet(Set(party))),
        transactionFilterValidator,
      )

      val partyWithUnknowns = List("party", "Alice", "Bob")
      val filterWithUnknown =
        TransactionFilter(partyWithUnknowns.map(_ -> Filters.defaultInstance).toMap)
      val filterWithKnown =
        TransactionFilter(Map(party -> Filters.defaultInstance))

      "reject transaction requests for unknown parties" in {
        requestMustFailWith(
          request =
            partyRestrictiveValidator.validate(txReq.withFilter(filterWithUnknown), ledgerEnd),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown parties: [Alice, Bob]",
          metadata = Map.empty,
        )
      }

      "reject transaction tree requests for unknown parties" in {
        requestMustFailWith(
          request = partyRestrictiveValidator
            .validateTree(txTreeReq.withFilter(filterWithUnknown), ledgerEnd),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown parties: [Alice, Bob]",
          metadata = Map.empty,
        )
      }

      "reject transaction by id requests for unknown parties" in {
        requestMustFailWith(
          request = partyRestrictiveValidator.validateTransactionById(
            txByIdReq.withRequestingParties(partyWithUnknowns)
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown parties: [Alice, Bob]",
          metadata = Map.empty,
        )
      }

      "reject transaction by event id requests for unknown parties" in {
        requestMustFailWith(
          request = partyRestrictiveValidator.validateTransactionById(
            txByIdReq.withRequestingParties(partyWithUnknowns)
          ),
          code = INVALID_ARGUMENT,
          description =
            "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Unknown parties: [Alice, Bob]",
          metadata = Map.empty,
        )
      }

      "accept transaction requests for known parties" in {
        partyRestrictiveValidator.validate(
          txReq.withFilter(filterWithKnown),
          ledgerEnd,
        ) shouldBe a[Right[_, _]]
      }

      "accept transaction tree requests for known parties" in {
        partyRestrictiveValidator.validateTree(
          txTreeReq.withFilter(filterWithKnown),
          ledgerEnd,
        ) shouldBe a[Right[_, _]]
      }

      "accept transaction by id requests for known parties" in {
        partyRestrictiveValidator.validateTransactionById(
          txByIdReq.withRequestingParties(List("party"))
        ) shouldBe a[Right[_, _]]
      }

      "accept transaction by event id requests for known parties" in {
        partyRestrictiveValidator.validateTransactionById(
          txByIdReq.withRequestingParties(List("party"))
        ) shouldBe a[Right[_, _]]
      }
    }
  }
}
