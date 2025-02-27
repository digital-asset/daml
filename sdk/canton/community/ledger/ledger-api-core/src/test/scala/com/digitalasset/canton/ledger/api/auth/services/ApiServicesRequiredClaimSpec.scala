// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.admin.party_management_service.{
  PartyDetails,
  UpdatePartyDetailsRequest,
}
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamRequest
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdRequest
import com.daml.ledger.api.v2.state_service.GetActiveContractsRequest
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  Filters,
  ParticipantAuthorizationTopologyFormat,
  TopologyFormat,
  TransactionFilter,
  TransactionFormat,
  UpdateFormat,
}
import com.daml.ledger.api.v2.update_service.GetUpdatesRequest
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.auth.RequiredClaim
import com.digitalasset.canton.ledger.api.auth.RequiredClaims
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import scalapb.lenses.Lens

class ApiServicesRequiredClaimSpec extends AsyncFlatSpec with BaseTest with Matchers {

  behavior of "CommandCompletionServiceAuthorization.completionStreamClaims"

  it should "compute the correct claims in the happy path" in {
    val result = CommandCompletionServiceAuthorization.completionStreamClaims(
      CompletionStreamRequest(
        applicationId = "qwe",
        parties = Seq("a", "b", "c"),
        beginExclusive = 1234L,
      )
    )
    result should have size (4)
    result.collect(readAs) should contain theSameElementsAs List(
      RequiredClaim.ReadAs[CompletionStreamRequest]("a"),
      RequiredClaim.ReadAs[CompletionStreamRequest]("b"),
      RequiredClaim.ReadAs[CompletionStreamRequest]("c"),
    )
    result
      .collectFirst(matchApplicationId)
      .value
      .skipApplicationIdValidationForAnyPartyReaders shouldBe true
  }

  it should "compute the correct claims if empty parties" in {
    val result = CommandCompletionServiceAuthorization.completionStreamClaims(
      CompletionStreamRequest(
        applicationId = "qwe",
        parties = Nil,
        beginExclusive = 1234L,
      )
    )
    result should have size (1)
    result.collect(readAs) shouldBe Nil
    result
      .collectFirst(matchApplicationId)
      .value
      .skipApplicationIdValidationForAnyPartyReaders shouldBe true
  }

  behavior of "PartyManagementServiceAuthorization.updatePartyDetailsClaims"

  it should "compute the correct claims in the happy path" in {
    val result = PartyManagementServiceAuthorization.updatePartyDetailsClaims(
      UpdatePartyDetailsRequest(
        partyDetails = Some(
          PartyDetails(
            party = "abc",
            isLocal = true,
            localMetadata = None,
            identityProviderId = "ABC",
          )
        ),
        updateMask = None,
      )
    )
    result should have size (2)
    result.collectFirst(adminOrIdp).isDefined shouldBe true
    result.collectFirst(matchIdentityProviderId).isDefined shouldBe true
  }

  it should "compute the correct claims if no party details provided" in {
    val result = PartyManagementServiceAuthorization.updatePartyDetailsClaims(
      UpdatePartyDetailsRequest(
        partyDetails = None,
        updateMask = None,
      )
    )
    result should have size (1)
    result.collectFirst(adminOrIdp).isDefined shouldBe true
  }

  behavior of "StateServiceAuthorization.getActiveContractsClaims"

  it should "compute the correct claims in the happy path" in {
    StateServiceAuthorization.getActiveContractsClaims(
      GetActiveContractsRequest(
        filter = None,
        verbose = true,
        activeAtOffset = 15,
        eventFormat = Some(
          EventFormat(
            filtersByParty = Map(
              "a" -> Filters(),
              "b" -> Filters(),
              "c" -> Filters(),
            ),
            filtersForAnyParty = Some(Filters()),
            verbose = true,
          )
        ),
      )
    ) should contain theSameElementsAs RequiredClaims[GetActiveContractsRequest](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.ReadAsAnyParty(),
    )
  }

  it should "compute the correct claims if no filtersForAnyParty" in {
    StateServiceAuthorization.getActiveContractsClaims(
      GetActiveContractsRequest(
        filter = None,
        verbose = true,
        activeAtOffset = 15,
        eventFormat = Some(
          EventFormat(
            filtersByParty = Map(
              "a" -> Filters(),
              "b" -> Filters(),
              "c" -> Filters(),
            ),
            filtersForAnyParty = None,
            verbose = true,
          )
        ),
      )
    ) should contain theSameElementsAs RequiredClaims[GetActiveContractsRequest](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
    )
  }

  it should "compute the correct claims if no filtersByParty" in {
    StateServiceAuthorization.getActiveContractsClaims(
      GetActiveContractsRequest(
        filter = None,
        verbose = true,
        activeAtOffset = 15,
        eventFormat = Some(
          EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = None,
            verbose = true,
          )
        ),
      )
    ) shouldBe Nil
  }

  it should "compute the correct claims if no eventFormat" in {
    StateServiceAuthorization.getActiveContractsClaims(
      GetActiveContractsRequest(
        filter = None,
        verbose = true,
        activeAtOffset = 15,
        eventFormat = None,
      )
    ) shouldBe Nil
  }

  // TODO(i23504) Remove
  it should "compute the aggregated claims if both legacy and new usage" in {
    StateServiceAuthorization.getActiveContractsClaims(
      GetActiveContractsRequest(
        filter = Some(
          TransactionFilter(
            filtersByParty = Map(
              "1" -> Filters(),
              "2" -> Filters(),
              "3" -> Filters(),
            ),
            filtersForAnyParty = Some(Filters()),
          )
        ),
        verbose = true,
        activeAtOffset = 15,
        eventFormat = Some(
          EventFormat(
            filtersByParty = Map(
              "a" -> Filters(),
              "b" -> Filters(),
              "c" -> Filters(),
            ),
            filtersForAnyParty = Some(Filters()),
            verbose = true,
          )
        ),
      )
    ) should contain theSameElementsAs RequiredClaims[GetActiveContractsRequest](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.ReadAsAnyParty(),
      RequiredClaim.ReadAs("1"),
      RequiredClaim.ReadAs("2"),
      RequiredClaim.ReadAs("3"),
      RequiredClaim.ReadAsAnyParty(),
    )
  }

  // TODO(i23504) Remove
  it should "compute the aggregated claims if both legacy and new usage, if one of the any party filters missing" in {
    StateServiceAuthorization.getActiveContractsClaims(
      GetActiveContractsRequest(
        filter = Some(
          TransactionFilter(
            filtersByParty = Map(
              "1" -> Filters(),
              "2" -> Filters(),
              "3" -> Filters(),
            ),
            filtersForAnyParty = None,
          )
        ),
        verbose = true,
        activeAtOffset = 15,
        eventFormat = Some(
          EventFormat(
            filtersByParty = Map(
              "a" -> Filters(),
              "b" -> Filters(),
              "c" -> Filters(),
            ),
            filtersForAnyParty = Some(Filters()),
            verbose = true,
          )
        ),
      )
    ) should contain theSameElementsAs RequiredClaims[GetActiveContractsRequest](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.ReadAsAnyParty(),
      RequiredClaim.ReadAs("1"),
      RequiredClaim.ReadAs("2"),
      RequiredClaim.ReadAs("3"),
    )
  }

  // TODO(i23504) Remove
  it should "compute the aggregated claims if both legacy and new usage, if only one any party filters, and one by party filters" in {
    StateServiceAuthorization.getActiveContractsClaims(
      GetActiveContractsRequest(
        filter = Some(
          TransactionFilter(
            filtersByParty = Map(
              "1" -> Filters(),
              "2" -> Filters(),
              "3" -> Filters(),
            ),
            filtersForAnyParty = None,
          )
        ),
        verbose = true,
        activeAtOffset = 15,
        eventFormat = Some(
          EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = Some(Filters()),
            verbose = true,
          )
        ),
      )
    ) should contain theSameElementsAs RequiredClaims[GetActiveContractsRequest](
      RequiredClaim.ReadAsAnyParty(),
      RequiredClaim.ReadAs("1"),
      RequiredClaim.ReadAs("2"),
      RequiredClaim.ReadAs("3"),
    )
  }

  // TODO(i23504) Remove
  it should "compute the aggregated claims if both legacy and new usage, if all empty" in {
    StateServiceAuthorization.getActiveContractsClaims(
      GetActiveContractsRequest(
        filter = Some(
          TransactionFilter(
            filtersByParty = Map.empty,
            filtersForAnyParty = None,
          )
        ),
        verbose = true,
        activeAtOffset = 15,
        eventFormat = Some(
          EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = None,
            verbose = true,
          )
        ),
      )
    ) shouldBe Nil
  }

  behavior of "UpdateServiceAuthorization.getUpdatesClaims"

  it should "compute the correct claims in the happy path" in {
    UpdateServiceAuthorization.getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        filter = None,
        verbose = true,
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = Some(
                  EventFormat(
                    filtersByParty = Map(
                      "a" -> Filters(),
                      "b" -> Filters(),
                    ),
                    filtersForAnyParty = Some(Filters()),
                    verbose = true,
                  )
                ),
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            includeReassignments = Some(
              EventFormat(
                filtersByParty = Map(
                  "c" -> Filters(),
                  "d" -> Filters(),
                ),
                filtersForAnyParty = Some(Filters()),
                verbose = true,
              )
            ),
            includeTopologyEvents = Some(
              TopologyFormat(Some(ParticipantAuthorizationTopologyFormat(parties = Seq("e", "f"))))
            ),
          )
        ),
      )
    ) should contain theSameElementsAs RequiredClaims[GetActiveContractsRequest](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.ReadAs("d"),
      RequiredClaim.ReadAs("e"),
      RequiredClaim.ReadAs("f"),
      RequiredClaim.ReadAsAnyParty(),
    )
  }

  it should "compute the correct claims if no party wildcards exist" in {
    UpdateServiceAuthorization.getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        filter = None,
        verbose = true,
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = Some(
                  EventFormat(
                    filtersByParty = Map(
                      "a" -> Filters(),
                      "b" -> Filters(),
                    ),
                    filtersForAnyParty = None,
                    verbose = true,
                  )
                ),
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            includeReassignments = Some(
              EventFormat(
                filtersByParty = Map(
                  "c" -> Filters(),
                  "d" -> Filters(),
                ),
                filtersForAnyParty = None,
                verbose = true,
              )
            ),
            includeTopologyEvents = Some(
              TopologyFormat(Some(ParticipantAuthorizationTopologyFormat(parties = Seq("e", "f"))))
            ),
          )
        ),
      )
    ) should contain theSameElementsAs RequiredClaims[GetActiveContractsRequest](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.ReadAs("d"),
      RequiredClaim.ReadAs("e"),
      RequiredClaim.ReadAs("f"),
    )
  }

  it should "compute the correct claims if no filtersByParty in transactions and reassignments exists" in {
    val eventFormatO = Some(
      EventFormat(
        filtersByParty = Map.empty,
        filtersForAnyParty = None,
        verbose = true,
      )
    )
    UpdateServiceAuthorization.getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        filter = None,
        verbose = true,
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = eventFormatO,
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            includeReassignments = eventFormatO,
            includeTopologyEvents = Some(
              TopologyFormat(Some(ParticipantAuthorizationTopologyFormat(parties = Seq("e", "f"))))
            ),
          )
        ),
      )
    ) should contain theSameElementsAs RequiredClaims[GetActiveContractsRequest](
      RequiredClaim.ReadAs("e"),
      RequiredClaim.ReadAs("f"),
    )
  }

  it should "compute the correct claims if no filtersByParty in transactions and reassignments exists and topology format is empty" in {
    val eventFormatO = Some(
      EventFormat(
        filtersByParty = Map.empty,
        filtersForAnyParty = None,
        verbose = true,
      )
    )
    UpdateServiceAuthorization.getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        filter = None,
        verbose = true,
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = eventFormatO,
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            includeReassignments = eventFormatO,
            includeTopologyEvents = Some(
              TopologyFormat(None)
            ),
          )
        ),
      )
    ) shouldBe Nil
  }

  it should "compute the correct claims if no updateFormat exists" in {
    UpdateServiceAuthorization.getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        filter = None,
        updateFormat = None,
      )
    ) shouldBe Nil
  }

  it should "compute the correct claims for topology format without wildcard" in {
    UpdateServiceAuthorization.getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        filter = None,
        verbose = true,
        updateFormat = Some(
          UpdateFormat(
            includeTopologyEvents = Some(
              TopologyFormat(Some(ParticipantAuthorizationTopologyFormat(parties = Seq("e", "f"))))
            )
          )
        ),
      )
    ) should contain theSameElementsAs RequiredClaims[GetActiveContractsRequest](
      RequiredClaim.ReadAs("e"),
      RequiredClaim.ReadAs("f"),
    )
  }

  it should "compute the correct claims for transactions format with wildcard" in {
    UpdateServiceAuthorization.getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        filter = None,
        verbose = true,
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = Some(
                  EventFormat(
                    filtersForAnyParty = Some(Filters())
                  )
                ),
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            )
          )
        ),
      )
    ) should contain theSameElementsAs RequiredClaims[GetActiveContractsRequest](
      RequiredClaim.ReadAsAnyParty()
    )
  }

  it should "compute the correct claims for reassignments with wildcard" in {
    UpdateServiceAuthorization.getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        filter = None,
        verbose = true,
        updateFormat = Some(
          UpdateFormat(
            includeReassignments = Some(
              EventFormat(
                filtersForAnyParty = Some(Filters())
              )
            )
          )
        ),
      )
    ) should contain theSameElementsAs RequiredClaims[GetActiveContractsRequest](
      RequiredClaim.ReadAsAnyParty()
    )
  }

  it should "compute the correct claims for topology format with wildcard" in {
    UpdateServiceAuthorization.getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        filter = None,
        verbose = true,
        updateFormat = Some(
          UpdateFormat(
            includeTopologyEvents = Some(
              TopologyFormat(Some(ParticipantAuthorizationTopologyFormat(parties = Seq.empty)))
            )
          )
        ),
      )
    ) should contain theSameElementsAs RequiredClaims[GetActiveContractsRequest](
      RequiredClaim.ReadAsAnyParty()
    )
  }

  // TODO(i23504) Remove
  it should "compute the aggregated claims if legacy, happy path" in {
    UpdateServiceAuthorization.getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        filter = Some(
          TransactionFilter(
            filtersByParty = Map(
              "1" -> Filters(),
              "2" -> Filters(),
              "3" -> Filters(),
            ),
            filtersForAnyParty = Some(Filters()),
          )
        ),
        verbose = true,
        updateFormat = None,
      )
    ) should contain theSameElementsAs RequiredClaims[GetActiveContractsRequest](
      RequiredClaim.ReadAs("1"),
      RequiredClaim.ReadAs("2"),
      RequiredClaim.ReadAs("3"),
      RequiredClaim.ReadAsAnyParty(),
    )
  }

  // TODO(i23504) Remove
  it should "compute the aggregated claims if legacy, if the any party filters are missing" in {
    UpdateServiceAuthorization.getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        filter = Some(
          TransactionFilter(
            filtersByParty = Map(
              "1" -> Filters(),
              "2" -> Filters(),
              "3" -> Filters(),
            ),
            filtersForAnyParty = None,
          )
        ),
        verbose = true,
        updateFormat = None,
      )
    ) should contain theSameElementsAs RequiredClaims[GetActiveContractsRequest](
      RequiredClaim.ReadAs("1"),
      RequiredClaim.ReadAs("2"),
      RequiredClaim.ReadAs("3"),
    )
  }

  // TODO(i23504) Remove
  it should "compute the aggregated claims if legacy, if only any party filters exist" in {
    UpdateServiceAuthorization.getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        filter = Some(
          TransactionFilter(
            filtersForAnyParty = Some(Filters())
          )
        ),
        verbose = true,
        updateFormat = None,
      )
    ) should contain theSameElementsAs RequiredClaims[GetActiveContractsRequest](
      RequiredClaim.ReadAsAnyParty()
    )
  }

  behavior of "UserManagementServiceAuthorization.userReaderClaims"

  it should "compute the correct claims in the happy path" in {
    val userIdL = Lens.unit[String]
    val identityProviderIdL = Lens.unit[String]
    UserManagementServiceAuthorization.userReaderClaims[String](
      userIdL = userIdL,
      identityProviderIdL = identityProviderIdL,
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.MatchUserId(userIdL),
      RequiredClaim.MatchIdentityProviderId(identityProviderIdL),
    )
  }

  behavior of "EventQueryServiceAuthorization.getEventsByContractIdClaims"

  it should "compute the correct claims in the happy path" in {
    val result = EventQueryServiceAuthorization.getEventsByContractIdClaims(
      GetEventsByContractIdRequest(
        requestingParties = Seq("a", "b", "c"),
        eventFormat = Some(
          EventFormat(
            filtersByParty = Map(
              "A" -> Filters(),
              "B" -> Filters(),
              "C" -> Filters(),
            ),
            filtersForAnyParty = Some(Filters()),
            verbose = true,
          )
        ),
      )
    )
    result should contain theSameElementsAs RequiredClaims[GetEventsByContractIdRequest](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.ReadAs("A"),
      RequiredClaim.ReadAs("B"),
      RequiredClaim.ReadAs("C"),
      RequiredClaim.ReadAsAnyParty(),
    )
  }

  def readAs[Req]: PartialFunction[RequiredClaim[Req], RequiredClaim.ReadAs[Req]] = {
    case readAs: RequiredClaim.ReadAs[Req] => readAs
  }

  def admin[Req]: PartialFunction[RequiredClaim[Req], RequiredClaim.Admin[Req]] = {
    case admin: RequiredClaim.Admin[Req] => admin
  }

  def adminOrIdp[Req]: PartialFunction[RequiredClaim[Req], RequiredClaim.AdminOrIdpAdmin[Req]] = {
    case adminOrIdp: RequiredClaim.AdminOrIdpAdmin[Req] => adminOrIdp
  }

  def matchApplicationId[Req]
      : PartialFunction[RequiredClaim[Req], RequiredClaim.MatchApplicationId[Req]] = {
    case matchApplicationId: RequiredClaim.MatchApplicationId[Req] => matchApplicationId
  }

  def matchIdentityProviderId[Req]
      : PartialFunction[RequiredClaim[Req], RequiredClaim.MatchIdentityProviderId[Req]] = {
    case matchIdentityProviderId: RequiredClaim.MatchIdentityProviderId[Req] =>
      matchIdentityProviderId
  }
}
