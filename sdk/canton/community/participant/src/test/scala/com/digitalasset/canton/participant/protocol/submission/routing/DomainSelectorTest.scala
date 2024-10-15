// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressingLogger}
import com.digitalasset.canton.participant.protocol.submission.DomainSelectionFixture.*
import com.digitalasset.canton.participant.protocol.submission.DomainSelectionFixture.Transactions.{
  ExerciseByInterface,
  ThreeExercises,
}
import com.digitalasset.canton.participant.protocol.submission.UsableDomain.{
  UnknownPackage,
  UnsupportedMinimumProtocolVersion,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError
import com.digitalasset.canton.participant.sync.TransactionRoutingError.ConfigurationErrors.InvalidPrescribedDomainId
import com.digitalasset.canton.participant.sync.TransactionRoutingError.RoutingInternalError.InputContractsOnDifferentDomains
import com.digitalasset.canton.participant.sync.TransactionRoutingError.TopologyErrors.NoDomainForSubmission
import com.digitalasset.canton.participant.sync.TransactionRoutingError.UnableToQueryTopologySnapshot
import com.digitalasset.canton.protocol.{LfContractId, LfLanguageVersion, LfVersionedTransaction}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{DamlLfVersionToProtocolVersions, ProtocolVersion}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPartyId}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.Implicits.*
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future}

class DomainSelectorTest extends AnyWordSpec with BaseTest with HasExecutionContext {
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
    import DomainSelectorTest.*
    import DomainSelectorTest.ForSimpleTopology.*
    import SimpleTopology.*

    implicit val _loggerFactor: SuppressingLogger = loggerFactory

    val defaultDomainRank = DomainRank(Map.empty, 0, da)

    def reassignmentsDaToAcme(contracts: Set[LfContractId]) = DomainRank(
      reassignments = contracts.map(_ -> (signatory, da)).toMap, // current domain is da
      priority = 0,
      domainId = acme, // reassign to acme
    )

    "return correct value in happy path" in {
      val selector = selectorForExerciseByInterface()

      selector.forSingleDomain.futureValue shouldBe defaultDomainRank
      selector.forMultiDomain.futureValue shouldBe defaultDomainRank
    }

    "return an error when not connected to the domain" in {
      val selector = selectorForExerciseByInterface(connectedDomains = Set())
      val expected = TransactionRoutingError.UnableToQueryTopologySnapshot.Failed(da)

      // Single domain
      selector.forSingleDomain.leftValue shouldBe expected

      // Multi domain
      selector.forMultiDomain.leftValue shouldBe NoDomainForSubmission.Error(
        Map(da -> expected.toString)
      )
    }

    "return proper response when submitters or informees are hosted on the wrong domain" in {
      val selector = selectorForExerciseByInterface(
        admissibleDomains = NonEmpty.mk(Set, acme), // different than da
        connectedDomains = Set(acme, da),
      )

      // Single domain: failure
      selector.forSingleDomain.leftValue shouldBe InvalidPrescribedDomainId
        .NotAllInformeeAreOnDomain(
          da,
          domainsOfAllInformee = NonEmpty.mk(Set, acme),
        )

      // Multi domain: reassignment proposal (da -> acme)
      val domainRank = reassignmentsDaToAcme(selector.inputContractIds)
      selector.forMultiDomain.futureValue shouldBe domainRank

      // Multi domain, missing connection to acme: error
      val selectorMissingConnection = selectorForExerciseByInterface(
        admissibleDomains = NonEmpty.mk(Set, acme), // different than da
        connectedDomains = Set(da),
      )

      selectorMissingConnection.forMultiDomain.leftValue shouldBe NoDomainForSubmission.Error(
        Map(acme -> TransactionRoutingError.UnableToQueryTopologySnapshot.Failed(acme).toString)
      )
    }

    "take priority into account (multi domain setting)" in {
      def pickDomain(bestDomain: DomainId): DomainId = selectorForExerciseByInterface(
        // da is not in the list to force reassignment
        admissibleDomains = NonEmpty.mk(Set, acme, repair),
        connectedDomains = Set(acme, da, repair),
        priorityOfDomain = d => if (d == bestDomain) 10 else 0,
      ).forMultiDomain.futureValue.domainId

      pickDomain(acme) shouldBe acme
      pickDomain(repair) shouldBe repair
    }

    // TODO(#15561) Re-enable this test when we have a stable protocol version
    "take minimum protocol version into account" ignore {
      val oldPV = ProtocolVersion.v32

      val transactionVersion = LfLanguageVersion.v2_dev
      val newPV = DamlLfVersionToProtocolVersions.damlLfVersionToMinimumProtocolVersions
        .get(transactionVersion)
        .value

      val selectorOldPV = selectorForExerciseByInterface(
        transactionVersion = transactionVersion, // requires protocol version dev
        domainProtocolVersion = _ => oldPV,
      )

      // Domain protocol version is too low
      val expectedError = UnsupportedMinimumProtocolVersion(
        domainId = da,
        currentPV = oldPV,
        requiredPV = newPV,
        lfVersion = transactionVersion,
      )

      selectorOldPV.forSingleDomain.leftValue shouldBe InvalidPrescribedDomainId.Generic(
        da,
        expectedError.toString,
      )

      selectorOldPV.forMultiDomain.leftValue shouldBe NoDomainForSubmission.Error(
        Map(da -> expectedError.toString)
      )

      // Happy path
      val selectorNewPV = selectorForExerciseByInterface(
        transactionVersion = LfLanguageVersion.v2_dev, // requires protocol version dev
        domainProtocolVersion = _ => newPV,
      )

      selectorNewPV.forSingleDomain.futureValue shouldBe defaultDomainRank
      selectorNewPV.forMultiDomain.futureValue shouldBe defaultDomainRank
    }

    "refuse to route to a domain with missing package vetting" in {
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

        selector.forSingleDomain.leftValue shouldBe InvalidPrescribedDomainId.Generic(
          da,
          expectedError.toString,
        )

        selector.forMultiDomain.leftValue shouldBe NoDomainForSubmission.Error(
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

    "route to domain where all submitter have submission rights" in {
      val treeExercises = ThreeExercises()
      val domainOfContracts: Map[LfContractId, DomainId] =
        Map(
          treeExercises.inputContract1Id -> repair,
          treeExercises.inputContract2Id -> repair,
          treeExercises.inputContract3Id -> da,
        )
      val inputContractStakeholders: Map[LfContractId, Set[Ref.Party]] = Map(
        treeExercises.inputContract1Id -> Set(party3, observer),
        treeExercises.inputContract2Id -> Set(party3, observer),
        treeExercises.inputContract3Id -> Set(signatory, party3),
      )
      val topology: Map[LfPartyId, List[ParticipantId]] = Map(
        signatory -> List(submitterParticipantId),
        observer -> List(observerParticipantId),
        party3 -> List(participantId3),
      )

      // this test requires a reassignment that is only possible from da to repair.
      // All submitters are connected to the repair domain as follow:
      //        Map(
      //          submitterParticipantId -> Set(da, repair),
      //          observerParticipantId -> Set(acme, repair),
      //          participantId3 -> Set(da, acme, repair),
      //        )
      val selector = selectorForThreeExercises(
        treeExercises,
        admissibleDomains = NonEmpty.mk(Set, da, acme, repair),
        connectedDomains = Set(da, acme, repair),
        domainOfContracts = _ => domainOfContracts,
        inputContractStakeholders = inputContractStakeholders,
        topology = topology,
      )

      selector.forSingleDomain.leftValue shouldBe InputContractsOnDifferentDomains(Set(da, repair))
      selector.forMultiDomain.futureValue shouldBe DomainRank(
        reassignments = Map(treeExercises.inputContract3Id -> (signatory, da)),
        priority = 0,
        domainId = repair,
      )
    }

    "a domain is prescribed" when {
      "return correct response when prescribed submitter domain ID is the current one" in {
        val selector = selectorForExerciseByInterface(
          prescribedDomainId = Some(da)
        )

        selector.forSingleDomain.futureValue shouldBe defaultDomainRank
        selector.forMultiDomain.futureValue shouldBe defaultDomainRank
      }

      "return an error when prescribed domain is incorrect" in {
        val selector = selectorForExerciseByInterface(
          prescribedDomainId = Some(acme),
          connectedDomains = Set(acme, da),
        )

        // Single domain: prescribed domain should be domain of input contract
        selector.forSingleDomain.leftValue shouldBe InvalidPrescribedDomainId
          .InputContractsNotOnDomain(
            domainId = acme,
            inputContractDomain = da,
          )

        // Multi domain
        selector.forMultiDomain.leftValue shouldBe InvalidPrescribedDomainId
          .NotAllInformeeAreOnDomain(
            acme,
            domainsOfAllInformee = NonEmpty.mk(Set, da),
          )
      }

      "propose reassignments when needed" in {
        val selector = selectorForExerciseByInterface(
          prescribedDomainId = Some(acme),
          connectedDomains = Set(acme, da),
          admissibleDomains = NonEmpty.mk(Set, acme, da),
        )

        // Single domain: prescribed domain should be domain of input contract
        selector.forSingleDomain.leftValue shouldBe InvalidPrescribedDomainId
          .InputContractsNotOnDomain(
            domainId = acme,
            inputContractDomain = da,
          )

        // Multi domain: reassignment proposal (da -> acme)
        val domainRank = reassignmentsDaToAcme(selector.inputContractIds)
        selector.forMultiDomain.futureValue shouldBe domainRank
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
    import DomainSelectorTest.*
    import DomainSelectorTest.ForSimpleTopology.*
    import SimpleTopology.*

    "minimize the number of reassignments" in {
      val threeExercises = ThreeExercises(fixtureTransactionVersion)

      val domains = NonEmpty.mk(Set, acme, da, repair)

      def selectDomain(domainOfContracts: Map[LfContractId, DomainId]): DomainRank =
        selectorForThreeExercises(
          threeExercises = threeExercises,
          connectedDomains = domains,
          admissibleDomains = domains,
          domainOfContracts = _ => domainOfContracts,
        ).forMultiDomain.futureValue

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

        val expectedDomainRank = DomainRank(
          reassignments = Map(threeExercises.inputContract3Id -> (signatory, repair)),
          priority = 0,
          domainId = acme, // reassign to acme
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

        val expectedDomainRank = DomainRank(
          reassignments = Map(threeExercises.inputContract1Id -> (signatory, acme)),
          priority = 0,
          domainId = repair, // reassign to repair
        )

        selectDomain(domainsOfContracts) shouldBe expectedDomainRank
      }

    }
  }
}

private[routing] object DomainSelectorTest {
  private def createDomainId(alias: String): DomainId = DomainId(
    UniqueIdentifier.tryCreate(alias, DefaultTestIdentities.namespace)
  )

  private lazy val da = createDomainId("da")
  private lazy val acme = createDomainId("acme")
  private lazy val repair = createDomainId("repair")

  object ForSimpleTopology {
    import SimpleTopology.*

    private val defaultDomainOfContracts: Seq[LfContractId] => Map[LfContractId, DomainId] =
      contracts => contracts.map(_ -> da).toMap

    private val defaultPriorityOfDomain: DomainId => Int = _ => 0

    private val defaultDomain: DomainId = da

    private val defaultAdmissibleDomains: NonEmpty[Set[DomainId]] =
      NonEmpty.mk(Set, da)

    private val defaultPrescribedDomainId: Option[DomainId] = None

    private val defaultDomainProtocolVersion: DomainId => ProtocolVersion = _ =>
      BaseTest.testedProtocolVersion

    def selectorForExerciseByInterface(
        priorityOfDomain: DomainId => Int = defaultPriorityOfDomain,
        domainOfContracts: Seq[LfContractId] => Map[LfContractId, DomainId] =
          defaultDomainOfContracts,
        connectedDomains: Set[DomainId] = Set(defaultDomain),
        admissibleDomains: NonEmpty[Set[DomainId]] = defaultAdmissibleDomains,
        prescribedDomainId: Option[DomainId] = defaultPrescribedDomainId,
        domainProtocolVersion: DomainId => ProtocolVersion = defaultDomainProtocolVersion,
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
        exerciseByInterface.inputContractId -> Set(signatory, observer)
      )

      new Selector(loggerFactory)(
        priorityOfDomain,
        domainOfContracts,
        connectedDomains,
        admissibleDomains,
        prescribedDomainId,
        domainProtocolVersion,
        vettedPackages,
        exerciseByInterface.tx,
        ledgerTime,
        inputContractStakeholders,
      )
    }

    def selectorForThreeExercises(
        threeExercises: ThreeExercises,
        priorityOfDomain: DomainId => Int = defaultPriorityOfDomain,
        domainOfContracts: Seq[LfContractId] => Map[LfContractId, DomainId] =
          defaultDomainOfContracts,
        connectedDomains: Set[DomainId] = Set(defaultDomain),
        admissibleDomains: NonEmpty[Set[DomainId]] = defaultAdmissibleDomains,
        domainProtocolVersion: DomainId => ProtocolVersion = defaultDomainProtocolVersion,
        vettedPackages: Seq[VettedPackage] = ExerciseByInterface.correctPackages,
        ledgerTime: CantonTimestamp = CantonTimestamp.now(),
        inputContractStakeholders: Map[LfContractId, Set[Ref.Party]] = Map.empty,
        topology: Map[LfPartyId, List[ParticipantId]] = correctTopology,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
        loggerFactory: NamedLoggerFactory,
    ): Selector = {

      val contractStakeholders =
        if (inputContractStakeholders.isEmpty)
          threeExercises.inputContractIds.map { inputContractId =>
            inputContractId -> Set(signatory, observer)
          }.toMap
        else inputContractStakeholders

      new Selector(loggerFactory)(
        priorityOfDomain,
        domainOfContracts,
        connectedDomains,
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
        priorityOfDomain: DomainId => Int,
        domainOfContracts: Seq[LfContractId] => Map[LfContractId, DomainId],
        connectedDomains: Set[DomainId],
        admissibleDomains: NonEmpty[Set[DomainId]],
        prescribedSubmitterDomainId: Option[DomainId],
        domainProtocolVersion: DomainId => ProtocolVersion,
        vettedPackages: Seq[VettedPackage],
        tx: LfVersionedTransaction,
        ledgerTime: CantonTimestamp,
        inputContractStakeholders: Map[LfContractId, Set[Ref.Party]],
        topology: Map[LfPartyId, List[ParticipantId]] = correctTopology,
    )(implicit ec: ExecutionContext, traceContext: TraceContext) {

      import SimpleTopology.*

      val inputContractIds: Set[LfContractId] = inputContractStakeholders.keySet

      object TestDomainStateProvider extends DomainStateProvider {
        override def getTopologySnapshotAndPVFor(domainId: DomainId)(implicit
            traceContext: TraceContext
        ): Either[UnableToQueryTopologySnapshot.Failed, (TopologySnapshot, ProtocolVersion)] =
          Either
            .cond(
              connectedDomains.contains(domainId),
              SimpleTopology.defaultTestingIdentityFactory(
                topology,
                vettedPackages,
              ),
              UnableToQueryTopologySnapshot.Failed(domainId),
            )
            .map((_, domainProtocolVersion(domainId)))

        override def getDomainsOfContracts(coids: Seq[LfContractId])(implicit
            ec: ExecutionContext,
            traceContext: TraceContext,
        ): Future[Map[LfContractId, DomainId]] =
          Future.successful(domainOfContracts(coids))
      }

      private val domainRankComputation = new DomainRankComputation(
        participantId = submitterParticipantId,
        priorityOfDomain = priorityOfDomain,
        snapshotProvider = TestDomainStateProvider,
        loggerFactory = loggerFactory,
      )

      private val transactionDataET = TransactionData
        .create(
          actAs = Set(signatory),
          readAs = Set(),
          ledgerTime = ledgerTime,
          transaction = tx,
          domainStateProvider = TestDomainStateProvider,
          contractsStakeholders = inputContractStakeholders,
          prescribedDomainIdO = prescribedSubmitterDomainId,
          disclosedContracts = Nil,
        )

      private val domainSelector: EitherT[Future, TransactionRoutingError, DomainSelector] =
        transactionDataET.map { transactionData =>
          new DomainSelector(
            transactionData = transactionData,
            admissibleDomains = admissibleDomains,
            priorityOfDomain = priorityOfDomain,
            domainRankComputation = domainRankComputation,
            domainStateProvider = TestDomainStateProvider,
            loggerFactory = loggerFactory,
          )
        }

      def forSingleDomain: EitherT[Future, TransactionRoutingError, DomainRank] =
        domainSelector.flatMap(_.forSingleDomain)

      def forMultiDomain: EitherT[Future, TransactionRoutingError, DomainRank] =
        domainSelector.flatMap(_.forMultiDomain)
    }
  }

}
