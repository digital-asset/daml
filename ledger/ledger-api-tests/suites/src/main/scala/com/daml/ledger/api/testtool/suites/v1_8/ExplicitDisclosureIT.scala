// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.createdEvents
import com.daml.ledger.api.v1.commands.DisclosedContract
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.client.binding
import com.daml.ledger.test.model.Test._

final class ExplicitDisclosureIT extends LedgerTestSuite {

  test(
    "EDPlaceholder",
    "Placeholder test (only to check whether it compiles)",
    allocate(Parties(2)),
  )(implicit ec => { case Participants(Participant(ledger, owner, delegate)) =>
    val contractKey = "SomeKeyValue"

    def fetchDelegatedRequest(
        delegationCid: binding.Primitive.ContractId[Delegation],
        delegatedCid: binding.Primitive.ContractId[Delegated],
        disclosedContract: Option[DisclosedContract] = None,
    ) = ledger
      .submitAndWaitRequest(
        delegate,
        delegationCid.exerciseFetchDelegated(delegate, delegatedCid).command,
      )
      .update(_.commands.disclosedContracts := disclosedContract.toList)

    for {
      // Create a Delegation contract
      // Contract is visible both to owner (as signatory) and delegate (as observer)
      delegationCid <- ledger.create(owner, Delegation(owner, delegate))

      // Create Delegated contract, and get the contract payload
      // This contract is only visible to the owner
      createDelegatedRequest = ledger.submitAndWaitRequest(
        owner,
        Delegated(owner, contractKey).create.command,
      )
      createDelegatedResponse <- ledger.submitAndWaitForTransaction(createDelegatedRequest)
      createDelegatedEvent = createdEvents(createDelegatedResponse.getTransaction).head
      delegatedCid = binding.Primitive.ContractId[Delegated](createDelegatedEvent.contractId)

      // Copy the actual Delegated contract (from the transaction stream of the owner) to a disclosed contract.
      // Pretend we then send the disclosed contract to the delegate out of band.
      disclosedContract = createEventToDisclosedContract(createDelegatedEvent)

      // Exercise a choice on the Delegation that fetches the Delegated contract
      // Fails because the submitter doesn't see the contract being fetched
      exerciseFetchRequest = fetchDelegatedRequest(delegationCid, delegatedCid, None)
      exerciseFetchError <- ledger.submitAndWait(exerciseFetchRequest).failed
      _ = assertGrpcError(
        exerciseFetchError,
        LedgerApiErrors.ConsistencyErrors.ContractNotFound,
        None,
        checkDefiniteAnswerMetadata = true,
      )

      // Exercise the same choice, this time using correct explicit disclosure
      exerciseFetchRequestWithDisclosure = fetchDelegatedRequest(
        delegationCid,
        delegatedCid,
        Some(disclosedContract),
      )
      _ <- ledger.submitAndWait(exerciseFetchRequestWithDisclosure)

      // Exercise the same choice, this time using bad explicit disclosure
      malformedDisclosedContract = disclosedContract.copy(
        arguments = Some(Delegated(owner, contractKey + "modified").arguments)
      )
      exerciseFetchRequestWithBadDisclosure = fetchDelegatedRequest(
        delegationCid,
        delegatedCid,
        Some(malformedDisclosedContract),
      )
      exerciseFetchRequestWithBadDisclosureError <- ledger
        .submitAndWait(exerciseFetchRequestWithBadDisclosure)
        .failed
      _ = assertGrpcError(
        exerciseFetchRequestWithBadDisclosureError,
        LedgerApiErrors.ConsistencyErrors.DisclosedContractInvalid,
        None,
        checkDefiniteAnswerMetadata = true,
      )
    } yield ()
  })

  private def createEventToDisclosedContract(ev: CreatedEvent) = DisclosedContract(
    templateId = ev.templateId,
    contractId = ev.contractId,
    arguments = ev.createArguments,
    metadata = ev.metadata,
  )
}
