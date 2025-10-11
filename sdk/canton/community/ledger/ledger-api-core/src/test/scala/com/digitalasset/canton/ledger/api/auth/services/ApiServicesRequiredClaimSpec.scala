// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.admin.party_management_service.{
  PartyDetails,
  UpdatePartyDetailsRequest,
}
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamRequest
import com.daml.ledger.api.v2.command_service.SubmitAndWaitForTransactionRequest
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.interactive.interactive_submission_service.HashingSchemeVersion.HASHING_SCHEME_VERSION_V2
import com.daml.ledger.api.v2.interactive.interactive_submission_service.Metadata.SubmitterInfo
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  ExecuteSubmissionAndWaitForTransactionRequest,
  ExecuteSubmissionRequest,
  Metadata,
  PrepareSubmissionRequest,
  PreparedTransaction,
}
import com.daml.ledger.api.v2.state_service.GetActiveContractsRequest
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  Filters,
  ParticipantAuthorizationTopologyFormat,
  TopologyFormat,
  TransactionFormat,
  UpdateFormat,
}
import com.daml.ledger.api.v2.update_service.GetUpdatesRequest
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.auth.RequiredClaim
import com.digitalasset.canton.ledger.api.auth.RequiredClaims
import com.digitalasset.canton.ledger.api.auth.services.ApiServicesRequiredClaimSpec.{
  executeSubmissionAndWaitForTransactionRequest,
  executeSubmissionRequest,
  prepareSubmissionRequest,
  submitAndWaitForTransactionRequest,
}
import com.digitalasset.canton.serialization.ProtoConverter
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import scalapb.lenses.Lens

import java.util.UUID

class ApiServicesRequiredClaimSpec extends AsyncFlatSpec with BaseTest with Matchers {

  behavior of "CommandCompletionServiceAuthorization.completionStreamClaims"

  it should "compute the correct claims in the happy path" in {
    val result = CommandCompletionServiceAuthorization.completionStreamClaims(
      CompletionStreamRequest(
        userId = "qwe",
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
      .collectFirst(matchUserId)
      .value
      .skipUserIdValidationForAnyPartyReaders shouldBe true
  }

  it should "compute the correct claims if empty parties" in {
    val result = CommandCompletionServiceAuthorization.completionStreamClaims(
      CompletionStreamRequest(
        userId = "qwe",
        parties = Nil,
        beginExclusive = 1234L,
      )
    )
    result should have size (1)
    result.collect(readAs) shouldBe Nil
    result
      .collectFirst(matchUserId)
      .value
      .skipUserIdValidationForAnyPartyReaders shouldBe true
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
        activeAtOffset = 15,
        eventFormat = Some(
          EventFormat(
            filtersByParty = Map(
              "a" -> Filters(Nil),
              "b" -> Filters(Nil),
              "c" -> Filters(Nil),
            ),
            filtersForAnyParty = Some(Filters(Nil)),
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
        activeAtOffset = 15,
        eventFormat = Some(
          EventFormat(
            filtersByParty = Map(
              "a" -> Filters(Nil),
              "b" -> Filters(Nil),
              "c" -> Filters(Nil),
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
        activeAtOffset = 15,
        eventFormat = None,
      )
    ) shouldBe Nil
  }

  behavior of "InteractiveSubmissionServiceAuthorization.prepareSubmission"

  it should "compute the correct claims in the happy path" in {
    InteractiveSubmissionServiceAuthorization.getPreparedSubmissionClaims(
      prepareSubmissionRequest
    ) should contain theSameElementsAs RequiredClaims[PrepareSubmissionRequest](
      RequiredClaim.ReadAs("1"),
      RequiredClaim.ReadAs("2"),
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.MatchUserId(
        InteractiveSubmissionServiceAuthorization.userIdForPrepareSubmissionL
      ),
    )
  }

  behavior of "InteractiveSubmissionServiceAuthorization.executeSubmission"

  it should "compute the correct claims in the happy path" in {
    InteractiveSubmissionServiceAuthorization.getExecuteSubmissionClaims(
      executeSubmissionRequest,
      InteractiveSubmissionServiceAuthorization.preparedTransactionForExecuteSubmissionL,
      InteractiveSubmissionServiceAuthorization.userIdForExecuteSubmissionL,
    ) should contain theSameElementsAs RequiredClaims[ExecuteSubmissionRequest](
      RequiredClaim.ExecuteAs("1"),
      RequiredClaim.ExecuteAs("2"),
      RequiredClaim.MatchUserId(
        InteractiveSubmissionServiceAuthorization.userIdForExecuteSubmissionL
      ),
    )
  }

  behavior of "InteractiveSubmissionServiceAuthorization.executeSubmissionAndWaitForTransaction"

  it should "compute the correct claims in the happy path" in {
    InteractiveSubmissionServiceAuthorization.getExecuteSubmissionAndWaitForTransactionClaims(
      executeSubmissionAndWaitForTransactionRequest
    ) should contain theSameElementsAs RequiredClaims[
      ExecuteSubmissionAndWaitForTransactionRequest
    ](
      RequiredClaim.ExecuteAs("1"),
      RequiredClaim.ExecuteAs("2"),
      RequiredClaim.ReadAs("i"),
      RequiredClaim.ReadAs("ii"),
      RequiredClaim.ReadAsAnyParty(),
      RequiredClaim.MatchUserId(
        InteractiveSubmissionServiceAuthorization.userIdForExecuteSubmissionAndWaitForTransactionL
      ),
    )
  }

  behavior of "CommandServiceAuthorization.getSubmitAndWaitForTransactionClaims"

  it should "compute the correct claims in the happy path" in {
    CommandServiceAuthorization.getSubmitAndWaitForTransactionClaims(
      submitAndWaitForTransactionRequest
    ) should contain theSameElementsAs RequiredClaims[SubmitAndWaitForTransactionRequest](
      RequiredClaim.ActAs("1"),
      RequiredClaim.ActAs("2"),
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("i"),
      RequiredClaim.ReadAs("ii"),
      RequiredClaim.ReadAsAnyParty(),
      RequiredClaim.MatchUserId(CommandServiceAuthorization.userIdForTransactionL),
    )
  }

  it should "compute the correct claims if no filtersForAnyParty" in {
    CommandServiceAuthorization.getSubmitAndWaitForTransactionClaims(
      submitAndWaitForTransactionRequest.update(
        _.transactionFormat.eventFormat.modify(_.clearFiltersForAnyParty)
      )
    ) should contain theSameElementsAs RequiredClaims[SubmitAndWaitForTransactionRequest](
      RequiredClaim.ActAs("1"),
      RequiredClaim.ActAs("2"),
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("i"),
      RequiredClaim.ReadAs("ii"),
      RequiredClaim.MatchUserId(CommandServiceAuthorization.userIdForTransactionL),
    )
  }

  it should "compute the correct claims if no filtersByParty" in {
    CommandServiceAuthorization.getSubmitAndWaitForTransactionClaims(
      submitAndWaitForTransactionRequest.update(
        _.transactionFormat.eventFormat.modify(_.clearFiltersByParty)
      )
    ) should contain theSameElementsAs RequiredClaims[SubmitAndWaitForTransactionRequest](
      RequiredClaim.ActAs("1"),
      RequiredClaim.ActAs("2"),
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAsAnyParty(),
      RequiredClaim.MatchUserId(CommandServiceAuthorization.userIdForTransactionL),
    )
  }

  it should "compute the correct claims if no transactionFormat" in {
    CommandServiceAuthorization.getSubmitAndWaitForTransactionClaims(
      submitAndWaitForTransactionRequest.clearTransactionFormat
    ) should contain theSameElementsAs RequiredClaims[SubmitAndWaitForTransactionRequest](
      RequiredClaim.ActAs("1"),
      RequiredClaim.ActAs("2"),
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.MatchUserId(CommandServiceAuthorization.userIdForTransactionL),
    )
  }

  it should "compute the correct claims if no actAs" in {
    CommandServiceAuthorization.getSubmitAndWaitForTransactionClaims(
      submitAndWaitForTransactionRequest.update(_.commands.modify(_.clearActAs))
    ) should contain theSameElementsAs RequiredClaims[SubmitAndWaitForTransactionRequest](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("i"),
      RequiredClaim.ReadAs("ii"),
      RequiredClaim.ReadAsAnyParty(),
      RequiredClaim.MatchUserId(CommandServiceAuthorization.userIdForTransactionL),
    )
  }

  it should "compute the correct claims if no readAs" in {
    CommandServiceAuthorization.getSubmitAndWaitForTransactionClaims(
      submitAndWaitForTransactionRequest.update(_.commands.modify(_.clearReadAs))
    ) should contain theSameElementsAs RequiredClaims[SubmitAndWaitForTransactionRequest](
      RequiredClaim.ActAs("1"),
      RequiredClaim.ActAs("2"),
      RequiredClaim.ReadAs("i"),
      RequiredClaim.ReadAs("ii"),
      RequiredClaim.ReadAsAnyParty(),
      RequiredClaim.MatchUserId(CommandServiceAuthorization.userIdForTransactionL),
    )
  }

  it should "compute the correct claims if no actAs and no readAs" in {
    CommandServiceAuthorization.getSubmitAndWaitForTransactionClaims(
      submitAndWaitForTransactionRequest.update(
        _.commands.modify(_.clearReadAs),
        _.commands.modify(_.clearActAs),
      )
    ) should contain theSameElementsAs RequiredClaims[SubmitAndWaitForTransactionRequest](
      RequiredClaim.ReadAs("i"),
      RequiredClaim.ReadAs("ii"),
      RequiredClaim.ReadAsAnyParty(),
      RequiredClaim.MatchUserId(CommandServiceAuthorization.userIdForTransactionL),
    )
  }

  behavior of "UpdateServiceAuthorization.getUpdatesClaims"

  it should "compute the correct claims in the happy path" in {
    UpdateServiceAuthorization.getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = Some(
                  EventFormat(
                    filtersByParty = Map(
                      "a" -> Filters(Nil),
                      "b" -> Filters(Nil),
                    ),
                    filtersForAnyParty = Some(Filters(Nil)),
                    verbose = true,
                  )
                ),
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            includeReassignments = Some(
              EventFormat(
                filtersByParty = Map(
                  "c" -> Filters(Nil),
                  "d" -> Filters(Nil),
                ),
                filtersForAnyParty = Some(Filters(Nil)),
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
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = Some(
                  EventFormat(
                    filtersByParty = Map(
                      "a" -> Filters(Nil),
                      "b" -> Filters(Nil),
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
                  "c" -> Filters(Nil),
                  "d" -> Filters(Nil),
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
        updateFormat = None,
      )
    ) shouldBe Nil
  }

  it should "compute the correct claims for topology format without wildcard" in {
    UpdateServiceAuthorization.getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = None,
            includeReassignments = None,
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

  it should "compute the correct claims for transactions format with wildcard" in {
    UpdateServiceAuthorization.getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = Some(
                  EventFormat(
                    filtersByParty = Map.empty,
                    filtersForAnyParty = Some(Filters(Nil)),
                    verbose = false,
                  )
                ),
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            includeReassignments = None,
            includeTopologyEvents = None,
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
        updateFormat = Some(
          UpdateFormat(
            includeReassignments = Some(
              EventFormat(
                filtersByParty = Map.empty,
                filtersForAnyParty = Some(Filters(Nil)),
                verbose = false,
              )
            ),
            includeTransactions = None,
            includeTopologyEvents = None,
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
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = None,
            includeReassignments = None,
            includeTopologyEvents = Some(
              TopologyFormat(Some(ParticipantAuthorizationTopologyFormat(parties = Seq.empty)))
            ),
          )
        ),
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
      RequiredClaim.MatchUserIdForUserManagement(userIdL),
      RequiredClaim.MatchIdentityProviderId(identityProviderIdL),
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

  def matchUserId[Req]: PartialFunction[RequiredClaim[Req], RequiredClaim.MatchUserId[Req]] = {
    case matchUserId: RequiredClaim.MatchUserId[Req] => matchUserId
  }

  def matchIdentityProviderId[Req]
      : PartialFunction[RequiredClaim[Req], RequiredClaim.MatchIdentityProviderId[Req]] = {
    case matchIdentityProviderId: RequiredClaim.MatchIdentityProviderId[Req] =>
      matchIdentityProviderId
  }
}
object ApiServicesRequiredClaimSpec {
  val transactionFormat = TransactionFormat(
    eventFormat = Some(
      EventFormat(
        filtersByParty = Map(
          "i" -> Filters(Nil),
          "ii" -> Filters(Nil),
        ),
        filtersForAnyParty = Some(Filters(Nil)),
        verbose = true,
      )
    ),
    transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
  )
  val submitAndWaitForTransactionRequest =
    SubmitAndWaitForTransactionRequest(
      commands = Some(
        Commands.defaultInstance.copy(
          actAs = Seq("1", "2"),
          readAs = Seq("a", "b"),
          userId = "userId",
        )
      ),
      transactionFormat = Some(transactionFormat),
    )

  val prepareSubmissionRequest =
    PrepareSubmissionRequest(
      userId = "userId",
      commandId = "commandId",
      commands = List.empty,
      minLedgerTime = None,
      actAs = Seq("1", "2"),
      readAs = Seq("a", "b"),
      disclosedContracts = Seq.empty,
      synchronizerId = "",
      packageIdSelectionPreference = Seq.empty,
      verboseHashing = true,
      prefetchContractKeys = Seq.empty,
      maxRecordTime = Option.empty,
      estimateTrafficCost = None,
    )

  val preparedTransaction = PreparedTransaction(
    transaction = None,
    metadata = Some(
      Metadata(
        submitterInfo = Some(
          SubmitterInfo(
            actAs = Seq("1", "2"),
            commandId = "commandId",
          )
        ),
        synchronizerId = "synchronizerId",
        mediatorGroup = 0,
        transactionUuid = UUID.randomUUID().toString,
        preparationTime = 0,
        inputContracts = Seq.empty,
        minLedgerEffectiveTime = None,
        maxLedgerEffectiveTime = None,
        globalKeyMapping = Seq.empty,
        maxRecordTime = Option.empty,
      )
    ),
  )

  val executeSubmissionRequest =
    ExecuteSubmissionRequest(
      preparedTransaction = Some(preparedTransaction),
      partySignatures = None,
      deduplicationPeriod = ExecuteSubmissionRequest.DeduplicationPeriod.DeduplicationDuration(
        ProtoConverter.DurationConverter.toProtoPrimitive(java.time.Duration.ofSeconds(1))
      ),
      submissionId = "submissionId",
      userId = "userId",
      hashingSchemeVersion = HASHING_SCHEME_VERSION_V2,
      minLedgerTime = None,
    )

  val executeSubmissionAndWaitForTransactionRequest =
    ExecuteSubmissionAndWaitForTransactionRequest(
      preparedTransaction = Some(preparedTransaction),
      partySignatures = None,
      deduplicationPeriod =
        ExecuteSubmissionAndWaitForTransactionRequest.DeduplicationPeriod.DeduplicationDuration(
          ProtoConverter.DurationConverter.toProtoPrimitive(java.time.Duration.ofSeconds(1))
        ),
      submissionId = "submissionId",
      userId = "userId",
      hashingSchemeVersion = HASHING_SCHEME_VERSION_V2,
      minLedgerTime = None,
      transactionFormat = Some(transactionFormat),
    )
}
