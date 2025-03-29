// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.reassignment.{AssignedEvent, UnassignedEvent}
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse.ContractEntry
import com.daml.ledger.api.v2.state_service.IncompleteUnassigned
import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers.WrappedContractEntry
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.examples.java as M
import com.digitalasset.canton.protocol.{LfContractId, SerializableContract}
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, SynchronizerAlias}
import com.digitalasset.daml.lf.data.Ref.PackageId
import org.scalatest.Assertion
import org.scalatest.Inspectors.forEvery
import org.scalatest.LoneElement.convertToCollectionLoneElementWrapper

import scala.concurrent.Await
import scala.concurrent.duration.{DurationInt, FiniteDuration}

trait AcsInspection {
  import org.scalatest.EitherValues.*
  import org.scalatest.OptionValues.*
  import org.scalatest.matchers.should.Matchers.*

  def searchAcsSync(
      participantRefs: Seq[LocalParticipantReference],
      synchronizerAlias: SynchronizerAlias,
      module: String,
      template: String,
      timeout: FiniteDuration = 10.seconds,
      stakeholder: Option[PartyId] = None,
  ): LfContractId = {
    // Assert that every participant has exactly one contract of the given template
    val contractsByParticipant = participantRefs.map { participantRef =>
      BaseTest.eventually(timeout) {
        val activeContracts = participantRef.testing
          .acs_search(
            synchronizerAlias,
            filterTemplate = s"$module:$template",
            filterStakeholder = stakeholder,
          )
        withClue(s"extracting the active $template contracts at ${participantRef.name}") {
          participantRef -> activeContracts.loneElement
        }
      }
    }

    // Retrieve the contract id of one of the participants
    val (_, resultContract) =
      contractsByParticipant.headOption.getOrElse(fail("There must be at least one participant."))
    val resultContractId = resultContract.contractId

    // Assert that all participants have the same contract id.
    contractsByParticipant.map { case (participantRef, contract) =>
      withClue(participantRef.name) {
        contract.contractId shouldEqual resultContractId
      }
    }

    resultContractId
  }

  def assertInLedgerAcsSync(
      participantRefs: Seq[LocalParticipantReference],
      partyId: PartyId,
      contractId: LfContractId,
  ): Unit =
    for (participantRef <- participantRefs) {
      BaseTest.eventually() {
        val acs = participantRef.ledger_api.state.acs.of_party(partyId, verbose = false)
        acs.map(_.event.contractId) should contain(contractId.coid)
      }
    }

  private val eventuallyForeverUntilSuccess = 5.seconds

  def checkReassignmentStoreIsEmpty(
      source: SynchronizerId,
      participantRefs: Seq[LocalParticipantReference],
      partyId: PartyId,
  ): Unit = for (participantRef <- participantRefs) {
    withClue(s"For participant ${participantRef.name}") {
      BaseTest.eventuallyForever(timeUntilSuccess = eventuallyForeverUntilSuccess) {
        participantRef.ledger_api.state.acs
          .of_party(partyId)
          .flatMap(_.entry.incompleteUnassigned)
          .flatMap(_.unassignedEvent)
          .filter(_.source == source.toProtoPrimitive) should have size 0
      }
    }
  }

  def checkIncompleteUnassignedContracts(
      participantRefs: Seq[LocalParticipantReference],
      party: PartyId,
  ): Unit = for (participantRef <- participantRefs) {
    withClue(s"For participant ${participantRef.name}") {
      BaseTest.eventuallyForever(timeUntilSuccess = eventuallyForeverUntilSuccess) {
        participantRef.ledger_api.state.acs
          .of_party(party)
          .flatMap(_.entry.incompleteUnassigned)
          .flatMap(_.unassignedEvent) should not be empty
      }
    }
  }

  private def isInLedgerAcs(
      participantRef: LocalParticipantReference,
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      cid: LfContractId,
  ): Boolean =
    participantRef.ledger_api.state.acs.of_party(partyId).map(_.entry).exists {
      case ContractEntry.ActiveContract(activeContract) =>
        activeContract.synchronizerId == synchronizerId.toProtoPrimitive && activeContract.createdEvent.value.contractId == cid.coid

      case _ => false
    }

  def assertInLedgerAcsSync(
      participantRefs: Seq[LocalParticipantReference],
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      cid: LfContractId,
      timeout: FiniteDuration = 5.seconds,
  ): Unit =
    for (participantRef <- participantRefs) {
      BaseTest.eventually(timeout) {
        assert(
          isInLedgerAcs(participantRef, partyId, synchronizerId, cid),
          s"Participant ${participantRef.name} unexpectedly does not have the active contract $cid",
        )
      }
    }

  /*
  Check state of contract against canton stores.
   */
  protected def assertContractState(
      participant: LocalParticipantReference,
      contractId: LfContractId,
      activeOn: Seq[SynchronizerAlias],
      inactiveOn: Seq[SynchronizerAlias],
      timeout: FiniteDuration = 5.seconds,
  )(implicit traceContext: TraceContext): Assertion = {

    def getAcs(synchronizerAlias: SynchronizerAlias) =
      Await
        .result(participant.testing.state_inspection.findAcs(synchronizerAlias).value, timeout)
        .onShutdown(fail("Shutting down"))
        .value

    forEvery(activeOn) { synchronizer =>
      getAcs(synchronizer).get(contractId) should not be empty
    }
    forEvery(inactiveOn) { synchronizer =>
      getAcs(synchronizer).get(contractId) shouldBe empty
    }
  }

  def assertInAcsSync(
      participantRefs: Seq[LocalParticipantReference],
      synchronizerAlias: SynchronizerAlias,
      contractId: LfContractId,
      timeout: FiniteDuration = 5.seconds,
  ): Unit =
    for (participantRef <- participantRefs) {
      BaseTest.eventually(timeout) {
        val contracts =
          participantRef.testing.acs_search(synchronizerAlias, exactId = contractId.coid)
        val size = contracts.size

        assert(
          size == 1,
          s"Participant ${participantRef.name} unexpectedly does not have the active contract $contractId",
        )
      }
    }

  def assertNotInLedgerAcsSync(
      participantRefs: Seq[LocalParticipantReference],
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      cid: LfContractId,
      timeout: FiniteDuration = 5.seconds,
  ): Unit =
    for (participantRef <- participantRefs) {
      BaseTest.eventually(timeout) {
        assert(
          !isInLedgerAcs(participantRef, partyId, synchronizerId, cid),
          s"Participant ${participantRef.name} unexpectedly has active contract $cid",
        )
      }
    }

  def assertNotInAcsSync(
      participantRefs: Seq[LocalParticipantReference],
      synchronizerAlias: SynchronizerAlias,
      module: String,
      template: String,
      timeout: FiniteDuration = 5.seconds,
      stakeholder: Option[PartyId] = None,
  ): Unit =
    for (participantRef <- participantRefs) {
      BaseTest.eventually(timeout) {
        val contracts = participantRef.testing
          .acs_search(
            synchronizerAlias,
            filterTemplate = s"$module:$template",
            filterStakeholder = stakeholder,
          )

        assert(
          contracts.isEmpty,
          s"Participant ${participantRef.name} unexpectedly has active contracts $contracts",
        )
      }
    }

  def assertNotInLedgerAcsSync(
      participantRefs: Seq[LocalParticipantReference],
      partyId: PartyId,
      contractId: LfContractId,
  ): Unit =
    for (participantRef <- participantRefs) {
      BaseTest.eventually() {
        val acs = participantRef.ledger_api.state.acs.of_party(partyId, verbose = false)
        acs.map(_.event.contractId) should not contain contractId.coid
      }
    }

  /** @param stakeholder
    *   If provided, will be used as a filter.
    */
  def assertSizeOfAcs(
      participantRefs: Seq[LocalParticipantReference],
      synchronizerAlias: SynchronizerAlias,
      filterTemplate: String,
      expectedSize: Int,
      stakeholder: Option[PartyId] = None,
  ): Unit =
    for (participantRef <- participantRefs) {

      val predicate = (c: SerializableContract) =>
        stakeholder.forall(s => c.metadata.stakeholders.contains(s.toLf))

      val contracts = participantRef.testing
        .acs_search(
          synchronizerAlias,
          filterTemplate = filterTemplate,
          limit = PositiveInt.tryCreate(Math.max(2 * expectedSize, expectedSize + 10)),
        )
        .filter(predicate)

      withClue(
        s"$participantRef, $synchronizerAlias, stakeholder filter=$stakeholder\n$contracts"
      ) {
        contracts.size shouldEqual expectedSize
      }
    }

  def findPackageIdOf(
      moduleName: String,
      requestingParticipant: ParticipantReference,
  ): PackageId = {
    // Participant for the lookup. It does not matter which one, as all participants have the same package information.
    val candidatePackages =
      requestingParticipant.packages.find_by_module(moduleName).map(_.packageId)
    val resultAsString = withClue(s"extracting the package with module $moduleName") {
      candidatePackages.loneElement
    }
    PackageId.assertFromString(resultAsString)
  }

  def findIOU(
      participant: LocalParticipantReference,
      obligor: PartyId,
      owner: PartyId,
  ): M.iou.Iou.Contract =
    participant.ledger_api.javaapi.state.acs
      .await(M.iou.Iou.COMPANION)(
        obligor,
        contract =>
          contract.data.owner == owner.toProtoPrimitive && contract.data.payer == obligor.toProtoPrimitive,
      )

  def findIOU(
      participant: LocalParticipantReference,
      submitter: PartyId,
      predicate: M.iou.Iou.Contract => Boolean,
  ): M.iou.Iou.Contract =
    participant.ledger_api.javaapi.state.acs.await(M.iou.Iou.COMPANION)(submitter, predicate)

  private def comparableCreatedEvent(createdEvent: Option[CreatedEvent]): Option[CreatedEvent] =
    createdEvent.map(_.copy(offset = 0L, nodeId = 0))

  protected def comparableUnassignedEvent(unassignedEvent: UnassignedEvent): UnassignedEvent =
    unassignedEvent.copy(offset = 0L, nodeId = 0)

  // remove the offset and nodeIds from the IncompleteUnassigned since they are participant-specific
  protected def comparableIncompleteUnassigned(
      incompleteUnassigned: IncompleteUnassigned
  ): IncompleteUnassigned =
    incompleteUnassigned.update(
      _.optionalCreatedEvent := comparableCreatedEvent(incompleteUnassigned.createdEvent),
      _.optionalUnassignedEvent := incompleteUnassigned.unassignedEvent.map(
        comparableUnassignedEvent
      ),
    )

  // remove the offset and nodeIds from the AssignedEvent since they are participant-specific
  protected def comparableAssignedEvent(assigned: AssignedEvent): AssignedEvent =
    assigned.update(
      _.optionalCreatedEvent := assigned.createdEvent.map(_.copy(offset = 0, nodeId = 0))
    )

  // remove the offset and nodeIds from the events since they are participant-specific
  protected def comparable(contracts: Seq[WrappedContractEntry]): Seq[WrappedContractEntry] =
    contracts
      .map(_.entry)
      .map {
        case ContractEntry.Empty => ContractEntry.Empty
        case ContractEntry.ActiveContract(active) =>
          ContractEntry.ActiveContract(
            active.update(
              _.optionalCreatedEvent := comparableCreatedEvent(active.createdEvent)
            )
          )
        case ContractEntry.IncompleteUnassigned(unassigned) =>
          ContractEntry.IncompleteUnassigned(
            comparableIncompleteUnassigned(unassigned)
          )
        case ContractEntry.IncompleteAssigned(assigned) =>
          ContractEntry.IncompleteAssigned(
            assigned.update(
              _.optionalAssignedEvent :=
                assigned.assignedEvent.map(
                  _.update(
                    _.optionalCreatedEvent :=
                      comparableCreatedEvent(assigned.assignedEvent.flatMap(_.createdEvent))
                  )
                )
            )
          )
      }
      .map(WrappedContractEntry.apply)

}

object AcsInspection extends AcsInspection
