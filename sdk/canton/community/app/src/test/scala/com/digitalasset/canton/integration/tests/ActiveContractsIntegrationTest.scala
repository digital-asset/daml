// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.ledger.api.v2 as proto
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.event.CreatedEvent.toJavaProto
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse.ContractEntry
import com.daml.ledger.api.v2.value.{Identifier, Record}
import com.daml.ledger.javaapi.data.CreatedEvent.fromProto as createdEventFromProto
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.{
  AssignedWrapper,
  UnassignedWrapper,
}
import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers.WrappedContractEntry
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandErrors.GenericCommandError
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.crypto.TestSalt
import com.digitalasset.canton.data.ViewPosition
import com.digitalasset.canton.examples.java.iou.{Amount, GetCash, Iou}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.ActiveContractsIntegrationTest.*
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.GrpcAdminCommandSupport.ParticipantReferenceOps
import com.digitalasset.canton.integration.util.GrpcServices.StateService
import com.digitalasset.canton.integration.util.{
  AcsInspection,
  HasCommandRunnersHelpers,
  HasReassignmentCommandsHelpers,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.ledger.api.validation.{StricterValueValidator, ValueValidator}
import com.digitalasset.canton.participant.admin.data.RepairContract
import com.digitalasset.canton.participant.util.JavaCodegenUtil.ContractIdSyntax
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.{BaseTest, ReassignmentCounter, config, protocol}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.Versioned
import com.digitalasset.daml.lf.transaction.test.TestNodeBuilder
import org.scalatest.Assertion

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters.*

class ActiveContractsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection
    with HasReassignmentCommandsHelpers
    with HasCommandRunnersHelpers {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"), Set("sequencer3"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )

  private var party1a: PartyId = _
  private var party1b: PartyId = _
  private var party2: PartyId = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1_S1M1
      .withSetup { implicit env =>
        import env.*

        val synchronizers =
          Seq(
            daId -> synchronizerOwners1,
            acmeId -> synchronizerOwners2,
            repairSynchronizerId -> synchronizerOwners3,
          )

        // Disable automatic assignment so that we really control it
        synchronizers.foreach { case (synchronizerId, owners) =>
          owners.foreach(
            _.topology.synchronizer_parameters.propose_update(
              synchronizerId = synchronizerId,
              _.update(assignmentExclusivityTimeout = config.NonNegativeFiniteDuration.Zero),
            )
          )
        }

        for {
          p <- List(participant1, participant2)
          (d, a) <- Seq(
            sequencer1 -> daName,
            sequencer2 -> acmeName,
            sequencer3 -> repairSynchronizerName,
          )
        } yield p.synchronizers.connect_local(d, alias = a)

        Seq(daName, acmeName, repairSynchronizerName).foreach { alias =>
          party1a = participant1.parties.enable("party1", synchronizer = alias)
          party1b = participant1.parties.enable("party1b", synchronizer = alias)
          party2 = participant2.parties.enable("party2", synchronizer = alias)
        }

        participants.all.dars.upload(BaseTest.CantonExamplesPath)
      }

  protected def getActiveContracts(
      party: PartyId,
      validAt: Long,
      participant: LocalParticipantReference,
      filterTemplates: Option[Seq[TemplateId]] = None,
  ): Seq[WrappedContractEntry] =
    participant.ledger_api.state.acs.of_party(
      party = party,
      activeAtOffsetO = Some(validAt),
      filterTemplates = filterTemplates.getOrElse(Nil),
    )

  private def create(
      synchronizerId: SynchronizerId,
      signatory: PartyId,
      observer: PartyId,
  )(implicit
      env: TestConsoleEnvironment
  ): ContractData = {
    import env.*
    val (contract, createUpdate, _) =
      IouSyntax.createIouComplete(participant1, Some(synchronizerId))(signatory, observer)

    val createdEvent = createUpdate.events.head.getCreated

    ContractData(contract, createdEvent)

  }

  private def createViaRepair(
      synchronizerId: SynchronizerId,
      signatory: PartyId,
      observer: PartyId,
  )(implicit
      env: TestConsoleEnvironment
  ): ContractData = {
    import env.*

    val cantonContractIdVersion = AuthenticatedContractIdVersionV11

    val pureCrypto = participant1.underlying.map(_.cryptoPureApi).value
    val unicumGenerator = new UnicumGenerator(pureCrypto)

    val createIou = new Iou(
      signatory.toProtoPrimitive,
      observer.toProtoPrimitive,
      // Use the numeric precision that Daml engine normalizes Ledger API values to
      new Amount(new java.math.BigDecimal(new java.math.BigInteger("1000000000000"), 10), "USD"),
      List.empty.asJava,
    )
    val lfArguments = StricterValueValidator
      .validateRecord(Record.fromJavaProto(createIou.toValue.toProtoRecord))
      .getOrElse(throw new IllegalStateException)
    val lfTemplate = ValueValidator
      .validateIdentifier(Identifier.fromJavaProto(Iou.TEMPLATE_ID_WITH_PACKAGE_ID.toProto))
      .getOrElse(throw new IllegalStateException)

    val signatories = Set(signatory.toLf)
    val observers = Set(observer.toLf)

    val contractMetadata =
      ContractMetadata.tryCreate(signatories, signatories ++ observers, None)

    val packageName = Ref.PackageName.assertFromString("CantonExamples")

    val contractInst = LfThinContractInst(
      packageName = packageName,
      template = lfTemplate,
      arg = Versioned(protocol.DummyTransactionVersion, lfArguments),
    )

    val ledgerCreateTime = env.environment.clock.now
    val (contractSalt, unicum) = unicumGenerator.generateSaltAndUnicum(
      synchronizerId = synchronizerId,
      mediator = MediatorGroupRecipient(MediatorGroupIndex.one),
      transactionUuid = new UUID(1L, 1L),
      viewPosition = ViewPosition(List.empty),
      viewParticipantDataSalt = TestSalt.generateSalt(1),
      createIndex = 0,
      ledgerCreateTime = LedgerCreateTime(ledgerCreateTime),
      metadata = contractMetadata,
      suffixedContractInstance = ExampleTransactionFactory.asSerializableRaw(contractInst),
      cantonContractIdVersion,
    )

    lazy val contractId = cantonContractIdVersion.fromDiscriminator(
      ExampleTransactionFactory.lfHash(1337),
      unicum,
    )

    val createNode = TestNodeBuilder.create(
      id = contractId,
      templateId = lfTemplate,
      argument = lfArguments,
      signatories = signatories,
      observers = observers,
      packageName = packageName,
    )

    val repairContract = RepairContract(
      synchronizerId,
      contract = SerializableContract(
        contractId = contractId,
        contractInstance = createNode.versionedCoinst,
        metadata = ContractMetadata.tryCreate(
          signatories = Set(signatory.toLf),
          stakeholders = Set(signatory.toLf, observer.toLf),
          maybeKeyWithMaintainersVersioned = None,
        ),
        ledgerTime = ledgerCreateTime,
        contractSalt = contractSalt.unwrap,
      ).getOrElse(throw new IllegalStateException),
      ReassignmentCounter(0),
    )

    val startOffset =
      participant1.ledger_api.state.end()
    participant1.synchronizers.disconnect_all()

    participant1.repair.add(
      synchronizerId,
      testedProtocolVersion,
      Seq(repairContract),
    )
    participant1.synchronizers.reconnect_all()
    val createdEvent = eventually() {
      val endOffset = participant1.ledger_api.state.end()
      val updates = participant1.ledger_api.updates.transactions(
        partyIds = Set(signatory),
        completeAfter = Int.MaxValue,
        beginOffsetExclusive = startOffset,
        endOffsetInclusive = Some(endOffset),
      )
      updates.map(_.createEvents)
    }.flatten.loneElement
    val contract = Iou.Contract.fromCreatedEvent(createdEventFromProto(toJavaProto(createdEvent)))
    ContractData(contract, createdEvent)
  }

  private def submitAssignments(
      out: UnassignedWrapper,
      submitter: PartyId,
      participantOverride: Option[LocalParticipantReference],
  )(implicit
      env: TestConsoleEnvironment
  ): (AssignedWrapper, Completion) =
    assign(
      unassignId = out.unassignId,
      source = SynchronizerId.tryFromString(out.source),
      target = SynchronizerId.tryFromString(out.target),
      submittingParty = submitter.toLf,
      participantOverride = participantOverride,
    )

  private def normalizeEvent(event: CreatedEvent, party: PartyId): CreatedEvent =
    event.copy(witnessParties = List(party.toLf))

  private def normalizeEvent(
      event: proto.reassignment.UnassignedEvent,
      party: PartyId,
  ): proto.reassignment.UnassignedEvent =
    event.copy(witnessParties = List(party.toLf))

  private def buildExpectedActiveContractsFromCreate(
      party: PartyId
  )(
      contracts: Seq[(CreatedEvent, SynchronizerId, ReassignmentCounter)]
  ): Seq[WrappedContractEntry] =
    contracts
      .map { case (createdEvent, synchronizerId, reassignmentCounter) =>
        proto.state_service.ActiveContract(
          createdEvent = Some(normalizeEvent(createdEvent, party)),
          synchronizerId = synchronizerId.toProtoPrimitive,
          reassignmentCounter = reassignmentCounter.v,
        )
      }
      .map(proto.state_service.GetActiveContractsResponse.ContractEntry.ActiveContract.apply)
      .map(WrappedContractEntry.apply)

  private def buildExpectedActiveContractsFromAssigned(
      party: PartyId
  )(
      activationFromAssigned: Seq[AssignedWrapper]
  ): Seq[WrappedContractEntry] =
    activationFromAssigned.flatMap(
      _.events
        .map { assignedEvent =>
          WrappedContractEntry(
            proto.state_service.GetActiveContractsResponse.ContractEntry.ActiveContract(
              proto.state_service.ActiveContract(
                createdEvent = Some(normalizeEvent(assignedEvent.getCreatedEvent, party)),
                synchronizerId = assignedEvent.target,
                reassignmentCounter = assignedEvent.reassignmentCounter,
              )
            )
          )
        }
    )

  private def buildExpectedIncompletesUnassigned(party: PartyId)(
      incompleteUnassigned: Seq[
        (ContractData, UnassignedWrapper)
      ]
  ): Seq[WrappedContractEntry] = incompleteUnassigned
    .filter { case (_, event) =>
      event.events.view.flatMap(_.witnessParties).toSet.contains(party.toProtoPrimitive)
    }
    .map { case (contractData, unassigned) =>
      val expectedCreatedEvent = normalizeEvent(contractData.createdEvent, party)
      val expectedUnassigned = normalizeEvent(unassigned.events.loneElement, party)

      val expectedIncompleteUnassignment = proto.state_service.IncompleteUnassigned(
        createdEvent = Some(expectedCreatedEvent),
        unassignedEvent = Some(expectedUnassigned),
      )

      val incompleteUnassigned = proto.state_service.GetActiveContractsResponse.ContractEntry
        .IncompleteUnassigned(expectedIncompleteUnassignment)

      WrappedContractEntry(incompleteUnassigned)
    }

  private def buildExpectedResponse(party: PartyId)(
      activationFromCreate: Seq[(CreatedEvent, SynchronizerId, ReassignmentCounter)],
      activationFromAssigned: Seq[AssignedWrapper],
      incompletesUnassigned: Seq[
        (ContractData, UnassignedWrapper)
      ],
  ): Seq[WrappedContractEntry] = {
    val activeContractsFromCreate =
      buildExpectedActiveContractsFromCreate(party)(activationFromCreate)
    val activeContractsFromAssign =
      buildExpectedActiveContractsFromAssigned(party)(activationFromAssigned)
    val incompleteUnassigned =
      buildExpectedIncompletesUnassigned(party)(incompletesUnassigned)

    (activeContractsFromCreate ++ activeContractsFromAssign ++ incompleteUnassigned)
  }

  // v1 and v2 don't set the same values for createArgument
  // We make the test a bit less precise for the sake of convenience
  private def resetCreateArgument(
      response: WrappedContractEntry
  ): WrappedContractEntry =
    if (response.entry.isActiveContract) {
      val updatedCreateEvent = response.event.copy(createArguments = None)
      val updatedActiveContract =
        response.entry.activeContract.value.withCreatedEvent(updatedCreateEvent)

      WrappedContractEntry(ContractEntry.ActiveContract(updatedActiveContract))

    } else response

  "StateService.GetActiveContracts" should {
    "return incomplete reassignments and contracts" in { implicit env =>
      import env.*

      val signatory = party1a
      val observer = party2

      val unassignmentData =
        TrieMap[Int, (ContractData, UnassignedWrapper)]()

      // Utility method to check ACS
      def checkACS(participant: LocalParticipantReference, ledgerEnd: Long)(
          activationFromCreate: Seq[(CreatedEvent, SynchronizerId, ReassignmentCounter)],
          activationFromAssigned: Seq[AssignedWrapper],
          incompletesUnassigned: Seq[Int], // References contracts in the TrieMap
      ): Assertion = {
        val expectedResponse = buildExpectedResponse(
          signatory
        )(
          activationFromCreate = activationFromCreate,
          activationFromAssigned = activationFromAssigned,
          incompletesUnassigned = incompletesUnassigned.map(unassignmentData),
        )

        val contracts =
          getActiveContracts(signatory, validAt = ledgerEnd, participant)

        comparable(contracts) should contain theSameElementsAs comparable(expectedResponse)
      }

      // Emptiness
      checkACS(participant1, participant1.ledger_api.state.end())(
        Nil,
        Nil,
        Nil,
      )

      // Create contracts
      val contract1 = create(daId, signatory, observer)
      val contract2 = create(repairSynchronizerId, signatory, observer)
      val contract3 = create(acmeId, signatory, observer)

      // All contracts should be active
      checkACS(participant1, participant1.ledger_api.state.end())(
        activationFromCreate = Seq(
          (contract1.createdEvent, daId, ReassignmentCounter(0)),
          (contract2.createdEvent, repairSynchronizerId, ReassignmentCounter(0)),
          (contract3.createdEvent, acmeId, ReassignmentCounter(0)),
        ),
        activationFromAssigned = Nil,
        incompletesUnassigned = Nil,
      )

      /*
          unassignment contracts:
            contract1: da -> acme
            contract2: repair -> da
       */
      val participant2LedgerEndBeforeUnassignment1 = participant2.ledger_api.state.end()
      val (unassignment1, _) = unassign(
        cid = contract1.cid,
        source = daId,
        target = acmeId,
        submittingParty = signatory.toLf,
        participantOverride = Some(participant1),
      )

      unassignmentData.put(1, (contract1, unassignment1))

      checkACS(participant1, unassignment1.reassignment.offset)(
        activationFromCreate = Seq(
          (contract2.createdEvent, repairSynchronizerId, ReassignmentCounter(0)),
          (contract3.createdEvent, acmeId, ReassignmentCounter(0)),
        ),
        activationFromAssigned = Nil,
        incompletesUnassigned = Seq(1),
      )
      assertObservationOfUnassignedEvent(
        participant2,
        observer,
        unassignment1,
        participant2LedgerEndBeforeUnassignment1,
      )

      val participant2LedgerEndBeforeUnassignment2 = participant2.ledger_api.state.end()
      val (unassignment2, _) = unassign(
        cid = contract2.cid,
        source = repairSynchronizerId,
        target = daId,
        submittingParty = signatory.toLf,
        participantOverride = Some(participant1),
      )

      unassignmentData.put(2, (contract2, unassignment2))

      checkACS(participant1, unassignment2.reassignment.offset)(
        activationFromCreate = Seq((contract3.createdEvent, acmeId, ReassignmentCounter(0))),
        activationFromAssigned = Nil,
        incompletesUnassigned = Seq(1, 2),
      )
      assertObservationOfUnassignedEvent(
        participant2,
        observer,
        unassignment2,
        participant2LedgerEndBeforeUnassignment2,
      )

      // Submit the assignments
      val (assignment1, _) = submitAssignments(unassignment1, observer, Some(participant2))

      checkACS(participant2, assignment1.reassignment.offset)(
        activationFromCreate = Seq((contract3.createdEvent, acmeId, ReassignmentCounter(0))),
        activationFromAssigned = Seq(assignment1),
        incompletesUnassigned = Seq(2),
      )

      val (assignment2, _) = submitAssignments(unassignment2, observer, Some(participant2))

      // wait until participant2 is synchronized
      eventually() {
        env.participant2.ledger_api.state.end() should be >= assignment2.reassignment.offset
      }

      // Reassignments should not be incomplete anymore
      checkACS(participant2, assignment2.reassignment.offset)(
        activationFromCreate = Seq(
          (contract3.createdEvent, acmeId, ReassignmentCounter(0))
        ),
        activationFromAssigned = Seq(assignment1, assignment2),
        incompletesUnassigned = Nil,
      )

      // Cleaning
      IouSyntax.archive(participant1)(contract1.contract, signatory)
      IouSyntax.archive(participant1)(contract2.contract, signatory)
      IouSyntax.archive(participant1)(contract3.contract, signatory)

      // Final check
      checkACS(participant1, participant1.ledger_api.state.end())(
        Nil,
        Nil,
        Nil,
      )
    }

    "work with RepairService populated contracts too" in { implicit env =>
      import env.*
      val signatory = party1a
      val observer = party1b

      // Utility method to check ACS
      def checkACS(
          activationFromCreate: Seq[(CreatedEvent, SynchronizerId, ReassignmentCounter)]
      ): Assertion =
        forEvery(Seq(signatory)) { party =>
          val expectedResponse = buildExpectedResponse(
            party
          )(
            activationFromCreate = activationFromCreate,
            activationFromAssigned = Nil,
            incompletesUnassigned = Nil,
          ).map(resetCreateArgument)

          val participantEnd = participant1.ledger_api.state.end()
          val contracts =
            getActiveContracts(party, participantEnd, participant1).map(resetCreateArgument)

          comparable(contracts) should contain theSameElementsAs comparable(expectedResponse)
        }

      // Emptiness
      checkACS(Nil)

      // Create contracts with Repair Service
      val contract1 = createViaRepair(daId, signatory, observer)
      val contract2 = createViaRepair(repairSynchronizerId, signatory, observer)
      val contract3 = createViaRepair(acmeId, signatory, observer)

      // All contracts should be active
      checkACS(
        Seq(
          (contract1.createdEvent, daId, ReassignmentCounter(0)),
          (contract2.createdEvent, repairSynchronizerId, ReassignmentCounter(0)),
          (contract3.createdEvent, acmeId, ReassignmentCounter(0)),
        )
      )

      // Cleaning
      IouSyntax.archive(participant1)(contract1.contract, signatory)
      IouSyntax.archive(participant1)(contract2.contract, signatory)
      IouSyntax.archive(participant1)(contract3.contract, signatory)

      // Final check
      checkACS(Nil)
    }

    "have reassignment counter incremented" in { implicit env =>
      import env.*
      val signatory = party1a
      val observer = party2

      def getReassignmentCountersFromACS(): Seq[Long] = {
        val participantEnd = participant1.ledger_api.state.end()
        getActiveContracts(signatory, participantEnd, participant1).map(_.reassignmentCounter)
      }

      getReassignmentCountersFromACS() shouldBe empty

      val contract = create(daId, signatory, observer)

      val (unassignment1, _) = unassign(
        cid = contract.cid,
        source = daId,
        target = acmeId,
        submittingParty = signatory.toLf,
        participantOverride = Some(participant1),
      )

      unassignment1.events.loneElement.reassignmentCounter shouldBe 1
      getReassignmentCountersFromACS() shouldBe List(1)

      val (assignment1, _) = submitAssignments(unassignment1, signatory, Some(participant1))
      assignment1.events.loneElement.reassignmentCounter shouldBe 1
      getReassignmentCountersFromACS() shouldBe List(1)

      val (unassignment2, _) = unassign(
        cid = contract.cid,
        source = acmeId,
        target = repairSynchronizerId,
        submittingParty = signatory.toLf,
        participantOverride = Some(participant1),
      )
      unassignment2.events.loneElement.reassignmentCounter shouldBe 2
      getReassignmentCountersFromACS() shouldBe List(2)

      val (assignment2, _) = submitAssignments(unassignment2, signatory, Some(participant1))
      assignment2.events.loneElement.reassignmentCounter shouldBe 2
      getReassignmentCountersFromACS() shouldBe List(2)

      IouSyntax.archive(participant1)(contract.contract, signatory)
    }

    "take valid at filter into account" in { implicit env =>
      import env.*
      val party = party1a

      /*
        A test case consists of the offset used to query the snapshot endpoint
        and the expected list of contract to be returned.
       */
      case class TestCase(
          queryOffset: Long,
          expectedContracts: Seq[Iou.Contract],
      )

      // Before contracts were created
      // Note: this case forces this suite to come first in the class
      val initialTestCase = {
        val ledgerEnd = participant1.ledger_api.state.end()
        TestCase(
          queryOffset = ledgerEnd,
          expectedContracts = Nil,
        )
      }

      // Create and archive a contract
      IouSyntax.archive(participant1)(
        IouSyntax.createIou(participant1, Some(daId))(party, party),
        party,
      )

      val noContractTestCase = {
        val ledgerEnd = participant1.ledger_api.state.end()
        TestCase(
          queryOffset = ledgerEnd,
          expectedContracts = Nil,
        )
      }

      /*
          We create three contracts and fetch the ledger end after each create
          Once we query the ACS snapshot for the offsets, we expect to get 1, 2 and then 3 contracts
       */
      val testCases = (0 until 3)
        .foldLeft((Seq[Iou.Contract](), Seq[TestCase]())) { case ((contracts, testCases), _) =>
          val contract = IouSyntax.createIou(participant1, Some(daId))(party, party)
          val offset = participant1.ledger_api.state.end()

          val updatedContracts = contracts :+ contract
          val newTestCase = TestCase(
            queryOffset = offset,
            expectedContracts = updatedContracts,
          )

          (updatedContracts, testCases :+ newTestCase)
        }
        ._2

      // Using the participant's current end offset for the query should return the three contracts
      val participantEnd = participant1.ledger_api.state.end()
      val finalTestCase = testCases.last.copy(queryOffset = participantEnd)

      val allTestCases = testCases ++ Set(initialTestCase, finalTestCase, noContractTestCase)

      // At each call, with increasing offset, we should
      allTestCases.foreach { case TestCase(queryOffset, expectedContracts) =>
        val expectedCids = expectedContracts.map(_.id.toLf.coid)

        val activeContractsResponse = getActiveContracts(party, queryOffset, participant1)

        val activeContractsCids = activeContractsResponse.map(_.contractId)

        activeContractsCids.sorted shouldBe expectedCids.sorted
      }

      // Cleaning
      allTestCases
        .flatMap(_.expectedContracts)
        .distinct
        .foreach(IouSyntax.archive(participant1)(_, party))
    }

    "take template filtering into account" in { implicit env =>
      import env.*
      val party = party1a

      val contract = create(daId, party, party)

      val contractTemplate = TemplateId.fromJavaProtoIdentifier(Iou.TEMPLATE_ID.toProto)
      // LAPI server throws if the template is not found, so we just get an unused but valid template ID here
      val otherTemplate = TemplateId(
        packageId = GetCash.PACKAGE_ID,
        moduleName = GetCash.TEMPLATE_ID.getModuleName,
        entityName = GetCash.TEMPLATE_ID.getEntityName,
      )

      val (out, _) = unassign(
        cid = contract.cid,
        source = daId,
        target = acmeId,
        submittingParty = party.toLf,
        participantOverride = Some(participant1),
      )

      def checkACS(
          activeOn: Option[AssignedWrapper],
          incompleteReassignment: Boolean,
          templates: Seq[TemplateId],
      ): Assertion = {

        // We check for the two parties
        val expectedResponse = buildExpectedResponse(
          party
        )(
          activationFromCreate = Nil,
          activationFromAssigned = activeOn.toList,
          incompletesUnassigned = if (incompleteReassignment) Seq((contract, out)) else Nil,
        )

        val participantEnd = participant1.ledger_api.state.end()
        val contracts =
          getActiveContracts(party, participantEnd, participant1, Some(templates))

        comparable(contracts) should contain theSameElementsAs comparable(expectedResponse)
      }

      checkACS(activeOn = None, incompleteReassignment = true, Seq(contractTemplate))
      checkACS(activeOn = None, incompleteReassignment = true, Seq(contractTemplate, otherTemplate))
      checkACS(activeOn = None, incompleteReassignment = true, Seq()) // no filtering
      checkACS(activeOn = None, incompleteReassignment = false, Seq(otherTemplate))

      val (in, _) = submitAssignments(out, party, Some(participant1))

      checkACS(activeOn = Some(in), incompleteReassignment = false, Seq(contractTemplate))
      checkACS(
        activeOn = Some(in),
        incompleteReassignment = false,
        Seq(contractTemplate, otherTemplate),
      )
      checkACS(activeOn = Some(in), incompleteReassignment = false, Seq())
      checkACS(activeOn = None, incompleteReassignment = false, Seq(otherTemplate))

      // Cleaning
      IouSyntax.archive(participant1)(contract.contract, party)
    }

    "take party filtering into account" in { implicit env =>
      import env.*
      val party = party1a
      val otherParty = party1b

      val contract = create(daId, party, party)
      val contractTemplate = TemplateId.fromJavaProtoIdentifier(Iou.TEMPLATE_ID.toProto)

      val (out, _) = unassign(
        cid = contract.cid,
        source = daId,
        target = acmeId,
        submittingParty = party.toLf,
        participantOverride = Some(participant1),
      )

      def checkACS(
          activeOn: Option[AssignedWrapper],
          incompleteReassignment: Boolean,
          acsFor: PartyId,
      ): Assertion = {

        // We check for the two parties
        val expectedResponse = buildExpectedResponse(
          acsFor
        )(
          activationFromCreate = Nil,
          activationFromAssigned = activeOn.toList,
          incompletesUnassigned = if (incompleteReassignment) Seq((contract, out)) else Nil,
        )

        val participantEnd = participant1.ledger_api.state.end()
        val contracts =
          getActiveContracts(
            acsFor,
            participantEnd,
            participant1,
            Some(Seq(contractTemplate)),
          )

        comparable(contracts) should contain theSameElementsAs comparable(expectedResponse)
      }

      checkACS(activeOn = None, incompleteReassignment = true, party)
      checkACS(activeOn = None, incompleteReassignment = true, otherParty)
      checkACS(activeOn = None, incompleteReassignment = false, otherParty)

      val (in, _) = submitAssignments(out, party, Some(participant1))

      checkACS(activeOn = Some(in), incompleteReassignment = false, party)
      checkACS(activeOn = None, incompleteReassignment = false, otherParty)

      // Cleaning
      IouSyntax.archive(participant1)(contract.contract, party)
    }

    "return an error if validAt is bigger than ledgerEnd" in { implicit env =>
      import env.*
      val bigOffset = Long.MaxValue

      val result = participant1
        .runStreamingLapiAdminCommand(
          StateService.getActiveContracts(
            proto.state_service.GetActiveContractsRequest(
              activeAtOffset = bigOffset,
              filter = Some(getTransactionFilter(List(party1a.toLf))),
              verbose = false,
              eventFormat = None,
            )
          )
        )

      inside(result) { case error: GenericCommandError =>
        error.cause should include("is after ledger end")
      }
    }
  }

  private def assertObservationOfUnassignedEvent(
      participant: LocalParticipantReference,
      observer: PartyId,
      out: UnassignedWrapper,
      startFromExclusive: Long,
  ) = {
    val unassignedUniqueId = participant.ledger_api.updates
      .reassignments(
        partyIds = Set(observer),
        filterTemplates = Seq.empty,
        beginOffsetExclusive = startFromExclusive,
        completeAfter = PositiveInt.one,
        resultFilter = _.isUnassignment,
      )
      .collect { case unassigned: UnassignedWrapper =>
        (unassigned.source, unassigned.unassignId)
      }
      .loneElement

    unassignedUniqueId shouldBe (out.source, out.unassignId)
  }
}

private object ActiveContractsIntegrationTest {
  final case class ContractData(contract: Iou.Contract, createdEvent: CreatedEvent) {
    val cid: LfContractId = contract.id.toLf
  }
}
