// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.SynchronizerCryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.TransactionRoutingError
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus
import com.digitalasset.canton.ledger.participant.state.{RoutingSynchronizerState, SynchronizerRank}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressingLogger}
import com.digitalasset.canton.participant.protocol.submission.SynchronizerSelectionFixture.*
import com.digitalasset.canton.participant.protocol.submission.SynchronizerSelectionFixture.Transactions.{
  ExerciseByInterface,
  ThreeExercises,
}
import com.digitalasset.canton.participant.protocol.submission.UsableSynchronizers.{
  UnknownPackage,
  UnsupportedMinimumProtocolVersion,
}
import com.digitalasset.canton.protocol.{
  LfContractId,
  LfSerializationVersion,
  LfVersionedTransaction,
  Stakeholders,
}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshotLoader
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{LfSerializationVersionToProtocolVersions, ProtocolVersion}
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  LfPartyId,
  ProtocolVersionChecksAnyWordSpec,
}
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.Implicits.*
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future}

import TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
import TransactionRoutingError.RoutingInternalError.InputContractsOnDifferentSynchronizers
import TransactionRoutingError.TopologyErrors.NoSynchronizerForSubmission
import TransactionRoutingError.{UnableToGetStaticParameters, UnableToQueryTopologySnapshot}

class SynchronizerSelectorTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAnyWordSpec {
  implicit class RichEitherT[A](val e: EitherT[Future, TransactionRoutingError, A]) {
    def leftValue: TransactionRoutingError = e.value.futureValue.left.value
  }

  implicit val _loggerFactor: SuppressingLogger = loggerFactory

  /*
    Topology:
      - participants: submitter and observer, each hosting submitter party and observer party, respectively
      - packages are vetted
      - synchronizer: da by default
        - input contract is on da
        - participants are connected to da
        - admissibleSynchronizers is Set(da)

    Transaction:
      - exercise by interface
      - one contract as input
   */
  "SynchronizerSelector (simple exercise by interface)" should {
    import SynchronizerSelectorTest.*
    import SynchronizerSelectorTest.ForSimpleTopology.*
    import SimpleTopology.*

    implicit val _loggerFactor: SuppressingLogger = loggerFactory

    val defaultSynchronizerRank = SynchronizerRank(Map.empty, 0, da)

    def reassignmentsDaToAcme(contracts: Set[LfContractId]) = SynchronizerRank(
      reassignments = contracts.map(_ -> (signatory, da)).toMap, // current synchronizer is da
      priority = 0,
      synchronizerId = acme, // reassign to acme
    )

    "return correct value in happy path" in {
      val selector = selectorForExerciseByInterface()

      selector.forSingleSynchronizer.futureValueUS.value shouldBe defaultSynchronizerRank
      selector.forMultiSynchronizer.futureValueUS.value shouldBe defaultSynchronizerRank
    }

    "return an error when not connected to the synchronizer" in {
      val selector = selectorForExerciseByInterface(connectedSynchronizers = Set())
      val expected = TransactionRoutingError.UnableToQueryTopologySnapshot.Failed(da)

      // Single synchronizer
      selector.forSingleSynchronizer.futureValueUS.leftOrFail(
        "FutureValueUS failed for single synchronizer"
      ) shouldBe expected

      // Multi synchronizer
      selector.forMultiSynchronizer.futureValueUS.leftOrFail(
        "FutureValueUS failed for multi synchronizer"
      ) shouldBe NoSynchronizerForSubmission.Error(
        Map(da -> expected.toString)
      )
    }

    "return proper response when submitters or informees are hosted on the wrong synchronizer" in {
      val selector = selectorForExerciseByInterface(
        admissibleSynchronizers = NonEmpty.mk(Set, acme), // different from da
        connectedSynchronizers = Set(acme, da),
      )

      // Single synchronizer: failure
      selector.forSingleSynchronizer.futureValueUS.leftOrFail(
        ""
      ) shouldBe InvalidPrescribedSynchronizerId
        .NotAllInformeeAreOnSynchronizer(
          da,
          synchronizersOfAllInformees = NonEmpty.mk(Set, acme),
        )

      // Multi synchronizer: reassignment proposal (da -> acme)
      val synchronizerRank = reassignmentsDaToAcme(selector.inputContractIds)
      selector.forMultiSynchronizer.futureValueUS.value shouldBe synchronizerRank

      // Multi synchronizer, missing connection to acme: error
      val selectorMissingConnection = selectorForExerciseByInterface(
        admissibleSynchronizers = NonEmpty.mk(Set, acme), // different than da
        connectedSynchronizers = Set(da),
      )

      selectorMissingConnection.forMultiSynchronizer.futureValueUS.leftOrFail(
        ""
      ) shouldBe NoSynchronizerForSubmission.Error(
        Map(acme -> TransactionRoutingError.UnableToQueryTopologySnapshot.Failed(acme).toString)
      )
    }

    "take priority into account (multi synchronizer setting)" in {
      def pickSynchronizer(bestSynchronizer: PhysicalSynchronizerId): PhysicalSynchronizerId =
        selectorForExerciseByInterface(
          // da is not in the list to force reassignment
          admissibleSynchronizers = NonEmpty.mk(Set, acme, repair),
          connectedSynchronizers = Set(acme, da, repair),
          priorityOfSynchronizer = d => if (d == bestSynchronizer) 10 else 0,
        ).forMultiSynchronizer.futureValueUS.value.synchronizerId

      pickSynchronizer(acme) shouldBe acme
      pickSynchronizer(repair) shouldBe repair
    }

    /*
    Running with pv=dev does not make any sense since we want oldPV < newPV=dev
    Running with 35 <= pv < dev would make sense but makes the test setup more complicated and does change the scenario.
     */
    "take minimum protocol version into account" onlyRunWith ProtocolVersion.v34 in {
      val oldPV = ProtocolVersion.v34

      val serializationVersion = LfSerializationVersion.VDev
      val newPV =
        LfSerializationVersionToProtocolVersions.lfSerializationVersionToMinimumProtocolVersions
          .get(serializationVersion)
          .value

      val selectorOldPV = selectorForExerciseByInterface(
        serializationVersion = serializationVersion, // requires protocol version dev
        connectedSynchronizers = Set(da.copy(protocolVersion = oldPV)),
        admissibleSynchronizers = NonEmpty.mk(Set, da.copy(protocolVersion = oldPV)),
      )

      // synchronizer protocol version is too low
      val expectedError = UnsupportedMinimumProtocolVersion(
        synchronizerId = da,
        requiredPV = newPV,
        lfVersion = serializationVersion,
      )

      selectorOldPV.forSingleSynchronizer
        .leftOrFailShutdown("aborted due to shutdown.")
        .futureValue shouldBe InvalidPrescribedSynchronizerId.Generic(
        da,
        expectedError.toString,
      )

      selectorOldPV.forMultiSynchronizer
        .leftOrFailShutdown("aborted due to shutdown.")
        .futureValue shouldBe NoSynchronizerForSubmission.Error(
        Map(da -> expectedError.toString)
      )

      // Happy path
      val daDev = da.copy(protocolVersion = newPV)
      val selectorNewPV = selectorForExerciseByInterface(
        serializationVersion = LfSerializationVersion.VDev, // requires protocol version dev
        connectedSynchronizers = Set(daDev),
        admissibleSynchronizers = NonEmpty.mk(Set, daDev),
        synchronizerOfContracts =
          contracts => contracts.map(_ -> (daDev, ContractStateStatus.Active)).toMap,
      )

      val daDevRank = SynchronizerRank(Map.empty, 0, daDev)

      selectorNewPV.forSingleSynchronizer.futureValueUS.value shouldBe daDevRank
      selectorNewPV.forMultiSynchronizer.futureValueUS.value shouldBe daDevRank
    }

    "refuse to route to a synchronizer with missing package vetting" in {
      val missingPackage = ExerciseByInterface.interfacePackageId
      val ledgerTime = CantonTimestamp.now()

      def runWithModifiedPackages(packages: Seq[VettedPackage]) = {
        val selector =
          selectorForExerciseByInterface(vettedPackages = packages, ledgerTime = ledgerTime)

        val expectedError = UnknownPackage(
          da,
          List(
            unknownPackageFor(submitterParticipantId, missingPackage),
            unknownPackageFor(observerParticipantId, missingPackage),
          ),
        )

        selector.forSingleSynchronizer.futureValueUS
          .leftOrFail("") shouldBe InvalidPrescribedSynchronizerId.Generic(
          da,
          expectedError.toString,
        )

        selector.forMultiSynchronizer.futureValueUS.leftOrFail(
          ""
        ) shouldBe NoSynchronizerForSubmission
          .Error(
            Map(da -> expectedError.toString)
          )
      }

      runWithModifiedPackages(
        ExerciseByInterface.correctPackages.filterNot(_.packageId == missingPackage)
      )

      val packageNotYetValid = ExerciseByInterface.correctPackages.map(vp =>
        if (vp.packageId == missingPackage)
          vp.copy(validFromInclusive = Some(ledgerTime.plusMillis(1L)))
        else vp
      )
      runWithModifiedPackages(packageNotYetValid)

      val packageNotValidAnymore = ExerciseByInterface.correctPackages.map(vp =>
        if (vp.packageId == missingPackage)
          vp.copy(validUntilExclusive = Some(ledgerTime.minusMillis(1L)))
        else vp
      )
      runWithModifiedPackages(packageNotValidAnymore)
    }

    "route to synchronizer where all submitter have submission rights" in {
      val treeExercises = ThreeExercises()
      val synchronizerOfContracts =
        Map(
          treeExercises.inputContract1Id -> (repair, ContractStateStatus.Active),
          treeExercises.inputContract2Id -> (repair, ContractStateStatus.Active),
          treeExercises.inputContract3Id -> (da, ContractStateStatus.Active),
        )
      val inputContractStakeholders = Map(
        treeExercises.inputContract1Id -> Stakeholders
          .withSignatoriesAndObservers(Set(party3), Set(observer)),
        treeExercises.inputContract2Id -> Stakeholders
          .withSignatoriesAndObservers(Set(party3), Set(observer)),
        treeExercises.inputContract3Id -> Stakeholders.withSignatoriesAndObservers(
          Set(signatory),
          Set(party3),
        ),
      )
      val topology: Map[LfPartyId, List[ParticipantId]] = Map(
        signatory -> List(submitterParticipantId),
        observer -> List(observerParticipantId),
        party3 -> List(participantId3),
      )

      // this test requires a reassignment that is only possible from da to repair.
      // All submitters are connected to the repair synchronizer as follow:
      //        Map(
      //          submitterParticipantId -> Set(da, repair),
      //          observerParticipantId -> Set(acme, repair),
      //          participantId3 -> Set(da, acme, repair),
      //        )
      val selector = selectorForThreeExercises(
        treeExercises,
        admissibleSynchronizers = NonEmpty.mk(Set, da, acme, repair),
        connectedSynchronizers = Set(da, acme, repair),
        synchronizerOfContracts = _ => synchronizerOfContracts,
        inputContractStakeholders = inputContractStakeholders,
        topology = topology,
      )

      selector.forSingleSynchronizer.futureValueUS.leftOrFail(
        ""
      ) shouldBe InputContractsOnDifferentSynchronizers(Set(da, repair))
      selector.forMultiSynchronizer.futureValueUS.value shouldBe SynchronizerRank(
        reassignments = Map(treeExercises.inputContract3Id -> (signatory, da)),
        priority = 0,
        synchronizerId = repair,
      )
    }

    "a synchronizer is prescribed" when {
      "return correct response when prescribed submitter synchronizer id is the current one" in {
        val selector = selectorForExerciseByInterface(
          prescribedPSId = Some(da)
        )

        selector.forSingleSynchronizer.futureValueUS.value shouldBe defaultSynchronizerRank
        selector.forMultiSynchronizer.futureValueUS.value shouldBe defaultSynchronizerRank
      }

      "return an error when prescribed synchronizer is incorrect" in {
        val selector = selectorForExerciseByInterface(
          prescribedPSId = Some(acme),
          connectedSynchronizers = Set(acme, da),
        )

        // Single synchronizer: prescribed synchronizer should be synchronizer of input contract
        selector.forSingleSynchronizer.futureValueUS.leftOrFail(
          ""
        ) shouldBe InvalidPrescribedSynchronizerId
          .InputContractsNotOnSynchronizer(
            physicalSynchronizerId = acme,
            inputContractPhysicalSynchronizerId = da,
          )

        // Multi synchronizer
        selector.forMultiSynchronizer.futureValueUS.leftOrFail(
          ""
        ) shouldBe InvalidPrescribedSynchronizerId
          .NotAllInformeeAreOnSynchronizer(
            acme,
            synchronizersOfAllInformees = NonEmpty.mk(Set, da),
          )
      }

      "propose reassignments when needed" in {
        val selector = selectorForExerciseByInterface(
          prescribedPSId = Some(acme),
          connectedSynchronizers = Set(acme, da),
          admissibleSynchronizers = NonEmpty.mk(Set, acme, da),
        )

        // Single synchronizer: prescribed synchronizer should be synchronizer of input contract
        selector.forSingleSynchronizer.futureValueUS.leftOrFail(
          ""
        ) shouldBe InvalidPrescribedSynchronizerId
          .InputContractsNotOnSynchronizer(
            physicalSynchronizerId = acme,
            inputContractPhysicalSynchronizerId = da,
          )

        // Multi synchronizer: reassignment proposal (da -> acme)
        val synchronizerRank = reassignmentsDaToAcme(selector.inputContractIds)
        selector.forMultiSynchronizer.futureValueUS.value shouldBe synchronizerRank
      }
    }
  }

  /*
    Topology:
      - participants: submitter and observer, each hosting submitter party and observer party, respectively
        - packages are vetted
        - synchronizer: da by default
          - input contract is on da
          - participants are connected to da
          - admissibleSynchronizers is Set(da)

    Transaction:
      - three exercises
      - three contracts as input
   */
  "SynchronizerSelector (simple transaction with three input contracts)" should {
    import SynchronizerSelectorTest.*
    import SynchronizerSelectorTest.ForSimpleTopology.*
    import SimpleTopology.*

    "minimize the number of reassignments" in {
      val threeExercises = ThreeExercises(fixtureSerializationVersion)

      val synchronizers = NonEmpty.mk(Set, acme, da, repair)

      def selectSynchronizer(
          synchronizerOfContracts: Map[LfContractId, PhysicalSynchronizerId]
      ): SynchronizerRank =
        selectorForThreeExercises(
          threeExercises = threeExercises,
          connectedSynchronizers = synchronizers,
          admissibleSynchronizers = synchronizers,
          synchronizerOfContracts = _ =>
            synchronizerOfContracts.map { case (coid, synchronizerId) =>
              coid -> (synchronizerId, ContractStateStatus.Active)
            },
        ).forMultiSynchronizer.futureValueUS.value

      /*
        Two contracts on acme, one on repair
        Expected: reassign to acme
       */
      {
        val synchronizersOfContracts = Map(
          threeExercises.inputContract1Id -> acme,
          threeExercises.inputContract2Id -> acme,
          threeExercises.inputContract3Id -> repair,
        )

        val expectedSynchronizerRank = SynchronizerRank(
          reassignments = Map(threeExercises.inputContract3Id -> (signatory, repair)),
          priority = 0,
          synchronizerId = acme, // reassign to acme
        )

        selectSynchronizer(synchronizersOfContracts) shouldBe expectedSynchronizerRank
      }

      /*
        Two contracts on repair, one on acme
        Expected: reassign to repair
       */
      {
        val synchronizersOfContracts = Map(
          threeExercises.inputContract1Id -> acme,
          threeExercises.inputContract2Id -> repair,
          threeExercises.inputContract3Id -> repair,
        )

        val expectedSynchronizerRank = SynchronizerRank(
          reassignments = Map(threeExercises.inputContract1Id -> (signatory, acme)),
          priority = 0,
          synchronizerId = repair, // reassign to repair
        )

        selectSynchronizer(synchronizersOfContracts) shouldBe expectedSynchronizerRank
      }

    }

    "Correctly report archived and not found contracts" in {
      val treeExercises = ThreeExercises()
      val synchronizers = NonEmpty.mk(Set, acme, da, repair)

      def selectSynchronizer(
          synchronizerOfContracts: Map[LfContractId, (PhysicalSynchronizerId, ContractStateStatus)]
      ): Either[TransactionRoutingError, SynchronizerRank] =
        selectorForThreeExercises(
          threeExercises = treeExercises,
          connectedSynchronizers = synchronizers,
          admissibleSynchronizers = synchronizers,
          synchronizerOfContracts = _ => synchronizerOfContracts,
        ).forMultiSynchronizer.futureValueUS

      val synchronizersOfContracts = Map(
        treeExercises.inputContract1Id -> (acme, ContractStateStatus.Active),
        treeExercises.inputContract2Id -> (acme, ContractStateStatus.Archived),
      )

      val test1 = selectSynchronizer(synchronizersOfContracts)
      test1.isLeft shouldBe true
      test1.left.value shouldBe TransactionRoutingError.TopologyErrors.UnknownContractSynchronizers
        .Error(
          notFound = Seq(treeExercises.inputContract3Id.coid),
          archived = Map(acme.logical -> Seq(treeExercises.inputContract2Id.coid)),
        )

      val allContractArchived = Map(
        treeExercises.inputContract1Id -> (acme, ContractStateStatus.Archived),
        treeExercises.inputContract2Id -> (acme, ContractStateStatus.Archived),
        treeExercises.inputContract3Id -> (da, ContractStateStatus.Archived),
      )

      val test2 = selectSynchronizer(allContractArchived)
      test2.left.value shouldBe TransactionRoutingError.TopologyErrors.UnknownContractSynchronizers
        .Error(
          notFound = Nil,
          archived = Map(
            acme.logical -> Seq(
              treeExercises.inputContract1Id.coid,
              treeExercises.inputContract2Id.coid,
            ),
            da.logical -> Seq(treeExercises.inputContract3Id.coid),
          ),
        )

      val test3 = selectSynchronizer(Map())
      test3.left.value shouldBe TransactionRoutingError.TopologyErrors.UnknownContractSynchronizers
        .Error(
          notFound = treeExercises.inputContractIds.map(_.coid),
          archived = Map(),
        )
    }
  }
}

private[routing] object SynchronizerSelectorTest {
  import BaseTest.*

  private def createSynchronizerId(alias: String): PhysicalSynchronizerId = SynchronizerId(
    UniqueIdentifier.tryCreate(alias, DefaultTestIdentities.namespace)
  ).toPhysical

  private lazy val da = createSynchronizerId("da")
  private lazy val acme = createSynchronizerId("acme")
  private lazy val repair = createSynchronizerId("repair")

  object ForSimpleTopology {
    import SimpleTopology.*

    private val defaultSynchronizerOfContracts
        : Seq[LfContractId] => Map[LfContractId, (PhysicalSynchronizerId, ContractStateStatus)] =
      contracts => contracts.map(_ -> (da, ContractStateStatus.Active)).toMap

    private val defaultPriorityOfSynchronizer: PhysicalSynchronizerId => Int = _ => 0

    private val defaultSynchronizer: PhysicalSynchronizerId = da

    private val defaultAdmissibleSynchronizers: NonEmpty[Set[PhysicalSynchronizerId]] =
      NonEmpty.mk(Set, da)

    private val defaultPrescribedSynchronizerId: Option[PhysicalSynchronizerId] = None

    def selectorForExerciseByInterface(
        priorityOfSynchronizer: PhysicalSynchronizerId => Int = defaultPriorityOfSynchronizer,
        synchronizerOfContracts: Seq[LfContractId] => Map[
          LfContractId,
          (PhysicalSynchronizerId, ContractStateStatus),
        ] = defaultSynchronizerOfContracts,
        connectedSynchronizers: Set[PhysicalSynchronizerId] = Set(defaultSynchronizer),
        admissibleSynchronizers: NonEmpty[Set[PhysicalSynchronizerId]] =
          defaultAdmissibleSynchronizers,
        prescribedPSId: Option[PhysicalSynchronizerId] = defaultPrescribedSynchronizerId,
        serializationVersion: LfSerializationVersion = fixtureSerializationVersion,
        vettedPackages: Seq[VettedPackage] = ExerciseByInterface.correctPackages,
        ledgerTime: CantonTimestamp = CantonTimestamp.now(),
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
        loggerFactory: NamedLoggerFactory,
    ): Selector = {

      val exerciseByInterface = ExerciseByInterface(serializationVersion)

      val inputContractStakeholders = Map(
        exerciseByInterface.inputContractId -> Stakeholders.withSignatoriesAndObservers(
          Set(signatory),
          Set(observer),
        )
      )

      new Selector(loggerFactory)(
        priorityOfSynchronizer,
        synchronizerOfContracts,
        connectedSynchronizers,
        admissibleSynchronizers,
        prescribedPSId,
        vettedPackages,
        exerciseByInterface.tx,
        ledgerTime,
        inputContractStakeholders,
      )
    }

    def selectorForThreeExercises(
        threeExercises: ThreeExercises,
        priorityOfSynchronizer: PhysicalSynchronizerId => Int = defaultPriorityOfSynchronizer,
        synchronizerOfContracts: Seq[LfContractId] => Map[
          LfContractId,
          (PhysicalSynchronizerId, ContractStateStatus),
        ] = defaultSynchronizerOfContracts,
        connectedSynchronizers: Set[PhysicalSynchronizerId] = Set(defaultSynchronizer),
        admissibleSynchronizers: NonEmpty[Set[PhysicalSynchronizerId]] =
          defaultAdmissibleSynchronizers,
        vettedPackages: Seq[VettedPackage] = ExerciseByInterface.correctPackages,
        ledgerTime: CantonTimestamp = CantonTimestamp.now(),
        inputContractStakeholders: Map[LfContractId, Stakeholders] = Map.empty,
        topology: Map[LfPartyId, List[ParticipantId]] = correctTopology,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
        loggerFactory: NamedLoggerFactory,
    ): Selector = {

      val contractStakeholders =
        if (inputContractStakeholders.isEmpty)
          threeExercises.inputContractIds.map { inputContractId =>
            inputContractId -> Stakeholders.withSignatoriesAndObservers(
              Set(signatory),
              Set(observer),
            )
          }.toMap
        else inputContractStakeholders

      new Selector(loggerFactory)(
        priorityOfSynchronizer,
        synchronizerOfContracts,
        connectedSynchronizers,
        admissibleSynchronizers,
        None,
        vettedPackages,
        threeExercises.tx,
        ledgerTime,
        contractStakeholders,
        topology,
      )
    }

    class Selector(loggerFactory: NamedLoggerFactory)(
        priorityOfSynchronizer: PhysicalSynchronizerId => Int,
        synchronizerOfContracts: Seq[LfContractId] => Map[
          LfContractId,
          (PhysicalSynchronizerId, ContractStateStatus),
        ],
        connectedSynchronizers: Set[PhysicalSynchronizerId],
        admissibleSynchronizers: NonEmpty[Set[PhysicalSynchronizerId]],
        prescribedSubmitterPSId: Option[PhysicalSynchronizerId],
        vettedPackages: Seq[VettedPackage],
        tx: LfVersionedTransaction,
        ledgerTime: CantonTimestamp,
        inputContractStakeholders: Map[LfContractId, Stakeholders],
        topology: Map[LfPartyId, List[ParticipantId]] = correctTopology,
    )(implicit ec: ExecutionContext, traceContext: TraceContext) {

      import SimpleTopology.*

      val inputContractIds: Set[LfContractId] = inputContractStakeholders.keySet

      object TestSynchronizerState$ extends RoutingSynchronizerState {
        override def getTopologySnapshotFor(
            psid: PhysicalSynchronizerId
        ): Either[UnableToQueryTopologySnapshot.Failed, TopologySnapshotLoader] =
          Either
            .cond(
              connectedSynchronizers.contains(psid),
              SimpleTopology.defaultTestingIdentityFactory(
                topology,
                vettedPackages,
              ),
              UnableToQueryTopologySnapshot.Failed(psid),
            )

        override def getSynchronizersOfContracts(coids: Seq[LfContractId])(implicit
            ec: ExecutionContext,
            traceContext: TraceContext,
        ): FutureUnlessShutdown[Map[LfContractId, (PhysicalSynchronizerId, ContractStateStatus)]] =
          FutureUnlessShutdown.pure(synchronizerOfContracts(coids))

        override def getSyncCryptoPureApi(
            synchronizerId: PhysicalSynchronizerId
        ): Either[UnableToGetStaticParameters.Failed, Option[SynchronizerCryptoPureApi]] =
          Right(None)

        override val topologySnapshots = Map.empty

        override def existsReadySynchronizer(): Boolean =
          // Not used in this test
          false
      }

      private val synchronizerRankComputation = new SynchronizerRankComputation(
        participantId = submitterParticipantId,
        priorityOfSynchronizer = priorityOfSynchronizer,
        loggerFactory = loggerFactory,
      )

      private val transactionDataET = TransactionData
        .create(
          actAs = Set(signatory),
          readAs = Set(),
          externallySignedSubmissionO = None,
          ledgerTime = ledgerTime,
          transaction = tx,
          synchronizerState = TestSynchronizerState$,
          contractsStakeholders = inputContractStakeholders,
          prescribedSynchronizerIdO = prescribedSubmitterPSId,
          disclosedContracts = Nil,
        )

      private val synchronizerSelector
          : EitherT[FutureUnlessShutdown, TransactionRoutingError, SynchronizerSelector] =
        transactionDataET
          .map { transactionData =>
            new SynchronizerSelector(
              transactionData = transactionData,
              admissibleSynchronizers = admissibleSynchronizers,
              priorityOfSynchronizer = priorityOfSynchronizer,
              synchronizerRankComputation = synchronizerRankComputation,
              synchronizerState = TestSynchronizerState$,
              loggerFactory = loggerFactory,
            )
          }

      def forSingleSynchronizer
          : EitherT[FutureUnlessShutdown, TransactionRoutingError, SynchronizerRank] =
        synchronizerSelector.flatMap(_.forSingleSynchronizer)

      def forMultiSynchronizer
          : EitherT[FutureUnlessShutdown, TransactionRoutingError, SynchronizerRank] =
        synchronizerSelector.flatMap(_.forMultiSynchronizer)
    }
  }

}
