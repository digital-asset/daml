// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_dev

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.createdEvents
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, Party}
import com.daml.ledger.api.v2.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v2.commands.DisclosedContract
import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TemplateFilter,
  TransactionFormat,
}
import com.daml.ledger.api.v2.value.Identifier
import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.{DamlRecord, ExerciseByKeyCommand}
import com.daml.ledger.test.java.experimental.test.WithKey
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors

import java.util.List as JList

final class ContractKeysExplicitDisclosureIT extends LedgerTestSuite {
  import ContractKeysExplicitDisclosureIT.*

  test(
    "EDExerciseByKeyDisclosedContract",
    "A disclosed contract can be exercised by key with non-witness readers if authorized",
    partyAllocation = allocate(SingleParty, SingleParty),
  ) { implicit ec =>
    {
      case p @ Participants(
            Participant(ownerParticipant, Seq(owner)),
            Participant(divulgeeParticipant, Seq(divulgee)),
          ) =>
        for {
          // Create contract with `owner` as only stakeholder
          _ <- ownerParticipant.submitAndWait(
            ownerParticipant.submitAndWaitRequest(owner, new WithKey(owner).create.commands)
          )
          txReq <- ownerParticipant.getTransactionsRequest(
            formatByPartyAndTemplate(owner, WithKey.TEMPLATE_ID)
          )
          txs <- ownerParticipant.transactions(txReq)
          withKeyCreationTx = assertSingleton("Transaction expected non-empty", txs)
          withKeyCreate = createdEvents(withKeyCreationTx).headOption.value
          withKeyDisclosedContract = createEventToDisclosedContract(withKeyCreate)

          // Ensure participants are synchronized
          _ <- p.synchronize

          exerciseByKeyError <- divulgeeParticipant
            .submitAndWait(
              exerciseWithKey_byKey_request(divulgeeParticipant, owner, divulgee, None)
            )
            .mustFail("divulgee does not see the contract")
          // Assert that a random party can exercise the contract by key (if authorized)
          // when passing the disclosed contract to the submission
          _ <- divulgeeParticipant.submitAndWait(
            exerciseWithKey_byKey_request(
              divulgeeParticipant,
              owner,
              divulgee,
              Some(withKeyDisclosedContract),
            )
          )
        } yield assertGrpcError(
          exerciseByKeyError,
          CommandExecutionErrors.Interpreter.LookupErrors.ContractKeyNotFound,
          None,
          checkDefiniteAnswerMetadata = true,
        )
    }
  }
}

object ContractKeysExplicitDisclosureIT {

  private def formatByPartyAndTemplate(
      owner: Party,
      templateId: javaapi.data.Identifier,
  ): TransactionFormat = {
    val templateIdScalaPB = Identifier.fromJavaProto(templateId.toProto)

    TransactionFormat(
      eventFormat = Some(
        EventFormat(
          filtersByParty = Map(
            owner.getValue -> new Filters(
              Seq(
                CumulativeFilter(
                  IdentifierFilter.TemplateFilter(
                    TemplateFilter(Some(templateIdScalaPB), includeCreatedEventBlob = true)
                  )
                )
              )
            )
          ),
          filtersForAnyParty = None,
          verbose = false,
        )
      ),
      transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
    )
  }

  private def createEventToDisclosedContract(ev: CreatedEvent): DisclosedContract =
    DisclosedContract(
      templateId = ev.templateId,
      contractId = ev.contractId,
      createdEventBlob = ev.createdEventBlob,
      synchronizerId = "",
    )

  private def exerciseWithKey_byKey_request(
      ledger: ParticipantTestContext,
      owner: Party,
      party: Party,
      withKeyDisclosedContract: Option[DisclosedContract],
  ): SubmitAndWaitRequest =
    ledger
      .submitAndWaitRequest(
        party,
        JList.of(
          new ExerciseByKeyCommand(
            WithKey.TEMPLATE_ID_WITH_PACKAGE_ID,
            owner,
            "WithKey_NoOp",
            new DamlRecord(
              new DamlRecord.Field(party)
            ),
          )
        ),
      )
      .update(_.commands.disclosedContracts := withKeyDisclosedContract.iterator.toSeq)
}
