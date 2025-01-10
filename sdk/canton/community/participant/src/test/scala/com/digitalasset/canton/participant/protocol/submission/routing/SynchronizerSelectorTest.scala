// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressingLogger}
import com.digitalasset.canton.participant.protocol.submission.DomainSelectionFixture.*
import com.digitalasset.canton.participant.protocol.submission.DomainSelectionFixture.Transactions.{
  ExerciseByInterface,
  ThreeExercises,
}
import com.digitalasset.canton.participant.protocol.submission.UsableSynchronizers.{
  UnknownPackage,
  UnsupportedMinimumProtocolVersion,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError
import com.digitalasset.canton.participant.sync.TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
import com.digitalasset.canton.participant.sync.TransactionRoutingError.RoutingInternalError.InputContractsOnDifferentSynchronizers
import com.digitalasset.canton.participant.sync.TransactionRoutingError.TopologyErrors.NoSynchronizerForSubmission
import com.digitalasset.canton.participant.sync.TransactionRoutingError.UnableToQueryTopologySnapshot
import com.digitalasset.canton.protocol.{
  LfContractId,
  LfLanguageVersion,
  LfVersionedTransaction,
  Stakeholders,
}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{DamlLfVersionToProtocolVersions, ProtocolVersion}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPartyId}
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.Implicits.*
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future}

class SynchronizerSelectorTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  implicit class RichEitherT[A](val e: EitherT[Future, TransactionRoutingError, A]) {
    def leftValue: TransactionRoutingError = e.value.futureValue.left.value
  }

  implicit val _loggerFactor: SuppressingLogger = loggerFactory

  /*
    Topology:
      - participants: submitter and observer, each hosting submitter party and observer party, respectively
      - packages are vetted
      - domain: da by default
        - input contract is on da
        - participants are connected to da
        - admissibleDomains is Set(da)

    Transaction:
      - exercise by interface
      - one contract as input
   */
  "DomainSelector (simple exercise by interface)" should {
    import SynchronizerSelectorTest.*
    import SynchronizerSelectorTest.ForSimpleTopology.*
    import SimpleTopology.*

    implicit val _loggerFactor: SuppressingLogger = loggerFactory

    val defaultDomainRank = SynchronizerRank(Map.empty, 0, da)

    def reassignmentsDaToAcme(contracts: Set[LfContractId]) = SynchronizerRank(
      reassignments = contracts.map(_ -> (signatory, da)).toMap, // current synchronizer is da
      priority = 0,
      synchronizerId = acme, // reassign to acme
    )

    "return correct value in happy path" in {
      val selector = selectorForExerciseByInterface()

      selector.forSingleDomain.futureValueUS.value shouldBe defaultDomainRank
      selector.forMultiDomain.futureValueUS.value shouldBe defaultDomainRank
    }

    "return an error when not connected to the domain" in {
      val selector = selectorForExerciseByInterface(connectedSynchronizers = Set())
      val expected = TransactionRoutingError.UnableToQueryTopologySnapshot.Failed(da)

      // Single domain
      selector.forSingleDomain.futureValueUS.leftOrFail(
        "FutureValueUS failed for single domain"
      ) shouldBe expected

      // Multi domain
      selector.forMultiDomain.futureValueUS.leftOrFail(
        "FutureValueUS failed for multi domain"
      ) shouldBe NoSynchronizerForSubmission.Error(
        Map(da -> expected.toString)
      )
    }

    "return proper response when submitters or informees are hosted on the wrong domain" in {
      val selector = selectorForExerciseByInterface(
        admissibleDomains = NonEmpty.mk(Set, acme), // different than da
        connectedSynchronizers = Set(acme, da),
      )

      // Single domain: failure
      selector.forSingleDomain.futureValueUS.leftOrFail("") shouldBe InvalidPrescribedSynchronizerId
        .NotAllInformeeAreOnSynchronizer(
          da,
          synchronizersOfAllInformee = NonEmpty.mk(Set, acme),
        )

      // Multi domain: reassignment proposal (da -> acme)
      val domainRank = reassignmentsDaToAcme(selector.inputContractIds)
      selector.forMultiDomain.futureValueUS.value shouldBe domainRank

      // Multi domain, missing connection to acme: error
      val selectorMissingConnection = selectorForExerciseByInterface(
        admissibleDomains = NonEmpty.mk(Set, acme), // different than da
        connectedSynchronizers = Set(da),
      )

      selectorMissingConnection.forMultiDomain.futureValueUS.leftOrFail(
        ""
      ) shouldBe NoSynchronizerForSubmission.Error(
        Map(acme -> TransactionRoutingError.UnableToQueryTopologySnapshot.Failed(acme).toString)
      )
    }

    "take priority into account (multi synchronizer setting)" in {
      def pickDomain(bestSynchronizer: SynchronizerId): SynchronizerId =
        selectorForExerciseByInterface(
          // da is not in the list to force reassignment
          admissibleDomains = NonEmpty.mk(Set, acme, repair),
          connectedSynchronizers = Set(acme, da, repair),
          priorityOfSynchronizer = d => if (d == bestSynchronizer) 10 else 0,
        ).forMultiDomain.futureValueUS.value.synchronizerId

      pickDomain(acme) shouldBe acme
      pickDomain(repair) shouldBe repair
    }

    // TODO(#15561) Re-enable this test when we have a stable protocol version
    "take minimum protocol version into account" ignore {
      val oldPV = ProtocolVersion.v33

      val transactionVersion = LfLanguageVersion.v2_dev
      val newPV = DamlLfVersionToProtocolVersions.damlLfVersionToMinimumProtocolVersions
        .get(transactionVersion)
        .value

      val selectorOldPV = selectorForExerciseByInterface(
        transactionVersion = transactionVersion, // requires protocol version dev
        domainProtocolVersion = _ => oldPV,
      )

      // synchronizer protocol version is too low
      val expectedError = UnsupportedMinimumProtocolVersion(
        synchronizerId = da,
        currentPV = oldPV,
        requiredPV = newPV,
        lfVersion = transactionVersion,
      )

      selectorOldPV.forSingleDomain.leftOrFailShutdown(
        "aborted due to shutdown."
      ) shouldBe InvalidPrescribedSynchronizerId.Generic(
        da,
        expectedError.toString,
      )

      selectorOldPV.forMultiDomain.leftOrFailShutdown(
        "aborted due to shutdown."
      ) shouldBe NoSynchronizerForSubmission.Error(
        Map(da -> expectedError.toString)
      )

      // Happy path
      val selectorNewPV = selectorForExerciseByInterface(
        transactionVersion = LfLanguageVersion.v2_dev, // requires protocol version dev
        domainProtocolVersion = _ => newPV,
      )

      selectorNewPV.forSingleDomain.futureValueUS shouldBe defaultDomainRank
      selectorNewPV.forMultiDomain.futureValueUS shouldBe defaultDomainRank
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

        selector.forSingleDomain.futureValueUS
          .leftOrFail("") shouldBe InvalidPrescribedSynchronizerId.Generic(
          da,
          expectedError.toString,
        )

        selector.forMultiDomain.futureValueUS.leftOrFail("") shouldBe NoSynchronizerForSubmission
          .Error(
            Map(da -> expectedError.toString)
          )
      }

      runWithModifiedPackages(
        ExerciseByInterface.correctPackages.filterNot(_.packageId == missingPackage)
      )

      val packageNotYetValid = ExerciseByInterface.correctPackages.map(vp =>
        if (vp.packageId == missingPackage) vp.copy(validFrom = Some(ledgerTime.plusMillis(1L)))
        else vp
      )
      runWithModifiedPackages(packageNotYetValid)

      val packageNotValidAnymore = ExerciseByInterface.correctPackages.map(vp =>
        if (vp.packageId == missingPackage) vp.copy(validUntil = Some(ledgerTime.minusMillis(1L)))
        else vp
      )
      runWithModifiedPackages(packageNotValidAnymore)
    }

    "route to synchronizer where all submitter have submission rights" in {
      val treeExercises = ThreeExercises()
      val domainOfContracts: Map[LfContractId, SynchronizerId] =
        Map(
          treeExercises.inputContract1Id -> repair,
          treeExercises.inputContract2Id -> repair,
          treeExercises.inputContract3Id -> da,
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
        admissibleDomains = NonEmpty.mk(Set, da, acme, repair),
        connectedSynchronizers = Set(da, acme, repair),
        domainOfContracts = _ => domainOfContracts,
        inputContractStakeholders = inputContractStakeholders,
        topology = topology,
      )

      selector.forSingleDomain.futureValueUS.leftOrFail(
        ""
      ) shouldBe InputContractsOnDifferentSynchronizers(Set(da, repair))
      selector.forMultiDomain.futureValueUS.value shouldBe SynchronizerRank(
        reassignments = Map(treeExercises.inputContract3Id -> (signatory, da)),
        priority = 0,
        synchronizerId = repair,
      )
    }

    "a synchronizer is prescribed" when {
      "return correct response when prescribed submitter synchronizer id is the current one" in {
        val selector = selectorForExerciseByInterface(
          prescribedSynchronizerId = Some(da)
        )

        selector.forSingleDomain.futureValueUS.value shouldBe defaultDomainRank
        selector.forMultiDomain.futureValueUS.value shouldBe defaultDomainRank
      }

      "return an error when prescribed synchronizer is incorrect" in {
        val selector = selectorForExerciseByInterface(
          prescribedSynchronizerId = Some(acme),
          connectedSynchronizers = Set(acme, da),
        )

        // Single domain: prescribed synchronizer should be synchronizer of input contract
        selector.forSingleDomain.futureValueUS.leftOrFail(
          ""
        ) shouldBe InvalidPrescribedSynchronizerId
          .InputContractsNotOnSynchronizer(
            synchronizerId = acme,
            inputContractSynchronizerId = da,
          )

        // Multi domain
        selector.forMultiDomain.futureValueUS.leftOrFail(
          ""
        ) shouldBe InvalidPrescribedSynchronizerId
          .NotAllInformeeAreOnSynchronizer(
            acme,
            synchronizersOfAllInformee = NonEmpty.mk(Set, da),
          )
      }

      "propose reassignments when needed" in {
        val selector = selectorForExerciseByInterface(
          prescribedSynchronizerId = Some(acme),
          connectedSynchronizers = Set(acme, da),
          admissibleDomains = NonEmpty.mk(Set, acme, da),
        )

        // Single domain: prescribed synchronizer should be synchronizer of input contract
        selector.forSingleDomain.futureValueUS.leftOrFail(
          ""
        ) shouldBe InvalidPrescribedSynchronizerId
          .InputContractsNotOnSynchronizer(
            synchronizerId = acme,
            inputContractSynchronizerId = da,
          )

        // Multi domain: reassignment proposal (da -> acme)
        val domainRank = reassignmentsDaToAcme(selector.inputContractIds)
        selector.forMultiDomain.futureValueUS.value shouldBe domainRank
      }
    }
  }

  /*
    Topology:
      - participants: submitter and observer, each hosting submitter party and observer party, respectively
        - packages are vetted
        - domain: da by default
          - input contract is on da
          - participants are connected to da
          - admissibleDomains is Set(da)

    Transaction:
      - three exercises
      - three contracts as input
   */
  "DomainSelector (simple transaction with three input contracts)" should {
    import SynchronizerSelectorTest.*
    import SynchronizerSelectorTest.ForSimpleTopology.*
    import SimpleTopology.*

    "minimize the number of reassignments" in {
      val threeExercises = ThreeExercises(fixtureTransactionVersion)

      val synchronizers = NonEmpty.mk(Set, acme, da, repair)

      def selectDomain(domainOfContracts: Map[LfContractId, SynchronizerId]): SynchronizerRank =
        selectorForThreeExercises(
          threeExercises = threeExercises,
          connectedSynchronizers = synchronizers,
          admissibleDomains = synchronizers,
          domainOfContracts = _ => domainOfContracts,
        ).forMultiDomain.futureValueUS.value

      /*
        Two contracts on acme, one on repair
        Expected: reassign to acme
       */
      {
        val domainsOfContracts = Map(
          threeExercises.inputContract1Id -> acme,
          threeExercises.inputContract2Id -> acme,
          threeExercises.inputContract3Id -> repair,
        )

        val expectedDomainRank = SynchronizerRank(
          reassignments = Map(threeExercises.inputContract3Id -> (signatory, repair)),
          priority = 0,
          synchronizerId = acme, // reassign to acme
        )

        selectDomain(domainsOfContracts) shouldBe expectedDomainRank
      }

      /*
        Two contracts on repair, one on acme
        Expected: reassign to repair
       */
      {
        val domainsOfContracts = Map(
          threeExercises.inputContract1Id -> acme,
          threeExercises.inputContract2Id -> repair,
          threeExercises.inputContract3Id -> repair,
        )

        val expectedDomainRank = SynchronizerRank(
          reassignments = Map(threeExercises.inputContract1Id -> (signatory, acme)),
          priority = 0,
          synchronizerId = repair, // reassign to repair
        )

        selectDomain(domainsOfContracts) shouldBe expectedDomainRank
      }

    }
  }
}

private[routing] object SynchronizerSelectorTest {
  private def createSynchronizerId(alias: String): SynchronizerId = SynchronizerId(
    UniqueIdentifier.tryCreate(alias, DefaultTestIdentities.namespace)
  )

  private lazy val da = createSynchronizerId("da")
  private lazy val acme = createSynchronizerId("acme")
  private lazy val repair = createSynchronizerId("repair")

  object ForSimpleTopology {
    import SimpleTopology.*

    private val defaultDomainOfContracts: Seq[LfContractId] => Map[LfContractId, SynchronizerId] =
      contracts => contracts.map(_ -> da).toMap

    private val defaultPriorityOfSynchronizer: SynchronizerId => Int = _ => 0

    private val defaultSynchronizer: SynchronizerId = da

    private val defaultAdmissibleDomains: NonEmpty[Set[SynchronizerId]] =
      NonEmpty.mk(Set, da)

    private val defaultPrescribedSynchronizerId: Option[SynchronizerId] = None

    private val defaultDomainProtocolVersion: SynchronizerId => ProtocolVersion = _ =>
      BaseTest.testedProtocolVersion

    def selectorForExerciseByInterface(
        priorityOfSynchronizer: SynchronizerId => Int = defaultPriorityOfSynchronizer,
        domainOfContracts: Seq[LfContractId] => Map[LfContractId, SynchronizerId] =
          defaultDomainOfContracts,
        connectedSynchronizers: Set[SynchronizerId] = Set(defaultSynchronizer),
        admissibleDomains: NonEmpty[Set[SynchronizerId]] = defaultAdmissibleDomains,
        prescribedSynchronizerId: Option[SynchronizerId] = defaultPrescribedSynchronizerId,
        domainProtocolVersion: SynchronizerId => ProtocolVersion = defaultDomainProtocolVersion,
        transactionVersion: LfLanguageVersion = fixtureTransactionVersion,
        vettedPackages: Seq[VettedPackage] = ExerciseByInterface.correctPackages,
        ledgerTime: CantonTimestamp = CantonTimestamp.now(),
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
        loggerFactory: NamedLoggerFactory,
    ): Selector = {

      val exerciseByInterface = ExerciseByInterface(transactionVersion)

      val inputContractStakeholders = Map(
        exerciseByInterface.inputContractId -> Stakeholders.withSignatoriesAndObservers(
          Set(signatory),
          Set(observer),
        )
      )

      new Selector(loggerFactory)(
        priorityOfSynchronizer,
        domainOfContracts,
        connectedSynchronizers,
        admissibleDomains,
        prescribedSynchronizerId,
        domainProtocolVersion,
        vettedPackages,
        exerciseByInterface.tx,
        ledgerTime,
        inputContractStakeholders,
      )
    }

    def selectorForThreeExercises(
        threeExercises: ThreeExercises,
        priorityOfSynchronizer: SynchronizerId => Int = defaultPriorityOfSynchronizer,
        domainOfContracts: Seq[LfContractId] => Map[LfContractId, SynchronizerId] =
          defaultDomainOfContracts,
        connectedSynchronizers: Set[SynchronizerId] = Set(defaultSynchronizer),
        admissibleDomains: NonEmpty[Set[SynchronizerId]] = defaultAdmissibleDomains,
        domainProtocolVersion: SynchronizerId => ProtocolVersion = defaultDomainProtocolVersion,
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
        domainOfContracts,
        connectedSynchronizers,
        admissibleDomains,
        None,
        domainProtocolVersion,
        vettedPackages,
        threeExercises.tx,
        ledgerTime,
        contractStakeholders,
        topology,
      )
    }

    class Selector(loggerFactory: NamedLoggerFactory)(
        priorityOfSynchronizer: SynchronizerId => Int,
        domainOfContracts: Seq[LfContractId] => Map[LfContractId, SynchronizerId],
        connectedSynchronizers: Set[SynchronizerId],
        admissibleDomains: NonEmpty[Set[SynchronizerId]],
        prescribedSubmitterSynchronizerId: Option[SynchronizerId],
        domainProtocolVersion: SynchronizerId => ProtocolVersion,
        vettedPackages: Seq[VettedPackage],
        tx: LfVersionedTransaction,
        ledgerTime: CantonTimestamp,
        inputContractStakeholders: Map[LfContractId, Stakeholders],
        topology: Map[LfPartyId, List[ParticipantId]] = correctTopology,
    )(implicit ec: ExecutionContext, traceContext: TraceContext) {

      import SimpleTopology.*

      val inputContractIds: Set[LfContractId] = inputContractStakeholders.keySet

      object TestSynchronizerStateProvider$ extends SynchronizerStateProvider {
        override def getTopologySnapshotAndPVFor(synchronizerId: SynchronizerId)(implicit
            traceContext: TraceContext
        ): Either[UnableToQueryTopologySnapshot.Failed, (TopologySnapshot, ProtocolVersion)] =
          Either
            .cond(
              connectedSynchronizers.contains(synchronizerId),
              SimpleTopology.defaultTestingIdentityFactory(
                topology,
                vettedPackages,
              ),
              UnableToQueryTopologySnapshot.Failed(synchronizerId),
            )
            .map((_, domainProtocolVersion(synchronizerId)))

        override def getSynchronizersOfContracts(coids: Seq[LfContractId])(implicit
            ec: ExecutionContext,
            traceContext: TraceContext,
        ): FutureUnlessShutdown[Map[LfContractId, SynchronizerId]] =
          FutureUnlessShutdown.pure(domainOfContracts(coids))
      }

      private val synchronizerRankComputation = new SynchronizerRankComputation(
        participantId = submitterParticipantId,
        priorityOfSynchronizer = priorityOfSynchronizer,
        snapshotProvider = TestSynchronizerStateProvider$,
        loggerFactory = loggerFactory,
      )

      private val transactionDataET = TransactionData
        .create(
          actAs = Set(signatory),
          readAs = Set(),
          externallySignedSubmissionO = None,
          ledgerTime = ledgerTime,
          transaction = tx,
          synchronizerStateProvider = TestSynchronizerStateProvider$,
          contractsStakeholders = inputContractStakeholders,
          prescribedSynchronizerIdO = prescribedSubmitterSynchronizerId,
          disclosedContracts = Nil,
        )

      private val domainSelector
          : EitherT[FutureUnlessShutdown, TransactionRoutingError, SynchronizerSelector] =
        transactionDataET
          .map { transactionData =>
            new SynchronizerSelector(
              transactionData = transactionData,
              admissibleSynchronizers = admissibleDomains,
              priorityOfSynchronizer = priorityOfSynchronizer,
              synchronizerRankComputation = synchronizerRankComputation,
              synchronizerStateProvider = TestSynchronizerStateProvider$,
              loggerFactory = loggerFactory,
            )
          }

      def forSingleDomain
          : EitherT[FutureUnlessShutdown, TransactionRoutingError, SynchronizerRank] =
        domainSelector.flatMap(_.forSingleDomain)

      def forMultiDomain: EitherT[FutureUnlessShutdown, TransactionRoutingError, SynchronizerRank] =
        domainSelector.flatMap(_.forMultiDomain)
    }
  }

}
