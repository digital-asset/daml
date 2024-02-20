// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import com.daml.lf.data.Ref
import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.transaction.test.TransactionBuilder.Implicits.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressingLogger}
import com.digitalasset.canton.participant.protocol.submission.DomainSelectionFixture.Transactions.{
  ExerciseByInterface,
  ThreeExercises,
}
import com.digitalasset.canton.participant.protocol.submission.DomainSelectionFixture.*
import com.digitalasset.canton.participant.protocol.submission.UsableDomain.{
  UnknownPackage,
  UnsupportedMinimumProtocolVersion,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError
import com.digitalasset.canton.participant.sync.TransactionRoutingError.ConfigurationErrors.InvalidPrescribedDomainId
import com.digitalasset.canton.participant.sync.TransactionRoutingError.TopologyErrors.NoDomainForSubmission
import com.digitalasset.canton.participant.sync.TransactionRoutingError.UnableToQueryTopologySnapshot
import com.digitalasset.canton.protocol.{LfContractId, LfTransactionVersion, LfVersionedTransaction}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  DomainAlias,
  HasExecutionContext,
  LfPackageId,
  LfWorkflowId,
}
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

    def transfersDaToAcme(contracts: Set[LfContractId]) = DomainRank(
      transfers = contracts.map(_ -> (signatory, da)).toMap, // current domain is da
      priority = 0,
      domainId = acme, // transfer to acme
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

      // Multi domain: transfer proposal (da -> acme)
      val domainRank = transfersDaToAcme(selector.inputContractIds)
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
        // da is not in the list to force transfer
        admissibleDomains = NonEmpty.mk(Set, acme, repair),
        connectedDomains = Set(acme, da, repair),
        priorityOfDomain = d => if (d == bestDomain) 10 else 0,
      ).forMultiDomain.futureValue.domainId

      pickDomain(acme) shouldBe acme
      pickDomain(repair) shouldBe repair
    }

    "take minimum protocol version into account" in {
      val oldPV = ProtocolVersion.smallestStable
      val newPV = ProtocolVersion.dev
      val transactionVersion = LfTransactionVersion.VDev

      val selectorOldPV = selectorForExerciseByInterface(
        transactionVersion = TransactionVersion.VDev, // requires protocol version dev
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
        transactionVersion = TransactionVersion.VDev, // requires protocol version dev
        domainProtocolVersion = _ => newPV,
      )

      selectorNewPV.forSingleDomain.futureValue shouldBe defaultDomainRank
      selectorNewPV.forMultiDomain.futureValue shouldBe defaultDomainRank
    }

    "refuse to route to a domain with missing package vetting" in {
      val missingPackage = ExerciseByInterface.interfacePackageId

      val selector = selectorForExerciseByInterface(
        vettedPackages = ExerciseByInterface.correctPackages.filterNot(_ == missingPackage)
      )

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

    "a domain is prescribed" when {
      "return correct response when prescribed domain is the current one" in {
        val selector = selectorForExerciseByInterface(
          prescribedDomainAlias = Some("da")
        )

        selector.forSingleDomain.futureValue shouldBe defaultDomainRank
        selector.forMultiDomain.futureValue shouldBe defaultDomainRank
      }

      "return correct response when prescribed domain as a domain ID is the current one" in {
        val selector = selectorForExerciseByInterface(
          prescribedDomainId = Some(da)
        )

        selector.forSingleDomain.futureValue shouldBe defaultDomainRank
        selector.forMultiDomain.futureValue shouldBe defaultDomainRank
      }

      "return correct response when prescribed submitter domain ID is the current one" in {
        val selector = selectorForExerciseByInterface(
          prescribedSubmitterDomainId = Some(da)
        )

        selector.forSingleDomain.futureValue shouldBe defaultDomainRank
        selector.forMultiDomain.futureValue shouldBe defaultDomainRank
      }

      "return correct response when prescribed submitter domain ID is the current one (precedence over prescribed alias)" in {
        val selector = selectorForExerciseByInterface(
          prescribedDomainAlias = Some("acme"),
          prescribedSubmitterDomainId = Some(da),
        )

        selector.forSingleDomain.futureValue shouldBe defaultDomainRank
        selector.forMultiDomain.futureValue shouldBe defaultDomainRank
      }

      "return correct response when prescribed submitter domain ID is the current one (precedence over prescribed domain ID)" in {
        val selector = selectorForExerciseByInterface(
          prescribedDomainId = Some(acme),
          prescribedSubmitterDomainId = Some(da),
        )

        selector.forSingleDomain.futureValue shouldBe defaultDomainRank
        selector.forMultiDomain.futureValue shouldBe defaultDomainRank
      }

      "return an error when prescribed domain is incorrect" in {
        val selector = selectorForExerciseByInterface(
          prescribedDomainAlias = Some("acme"),
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

      "propose transfers when needed" in {
        val selector = selectorForExerciseByInterface(
          prescribedDomainAlias = Some("acme"),
          connectedDomains = Set(acme, da),
          admissibleDomains = NonEmpty.mk(Set, acme, da),
        )

        // Single domain: prescribed domain should be domain of input contract
        selector.forSingleDomain.leftValue shouldBe InvalidPrescribedDomainId
          .InputContractsNotOnDomain(
            domainId = acme,
            inputContractDomain = da,
          )

        // Multi domain: transfer proposal (da -> acme)
        val domainRank = transfersDaToAcme(selector.inputContractIds)
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

    "minimize the number of transfers" in {
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
        Expected: transfer to acme
       */
      {
        val domainsOfContracts = Map(
          threeExercises.inputContract1Id -> acme,
          threeExercises.inputContract2Id -> acme,
          threeExercises.inputContract3Id -> repair,
        )

        val expectedDomainRank = DomainRank(
          transfers = Map(threeExercises.inputContract3Id -> (signatory, repair)),
          priority = 0,
          domainId = acme, // transfer to acme
        )

        selectDomain(domainsOfContracts) shouldBe expectedDomainRank
      }

      /*
        Two contracts on repair, one on acme
        Expected: transfer to repair
       */
      {
        val domainsOfContracts = Map(
          threeExercises.inputContract1Id -> acme,
          threeExercises.inputContract2Id -> repair,
          threeExercises.inputContract3Id -> repair,
        )

        val expectedDomainRank = DomainRank(
          transfers = Map(threeExercises.inputContract1Id -> (signatory, acme)),
          priority = 0,
          domainId = repair, // transfer to repair
        )

        selectDomain(domainsOfContracts) shouldBe expectedDomainRank
      }

    }
  }
}

private[routing] object DomainSelectorTest {
  private def createDomainId(alias: String): DomainId = DomainId(
    UniqueIdentifier(Identifier.tryCreate(alias), DefaultTestIdentities.namespace)
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

    private val defaultPrescribedDomainAlias: Option[String] = None
    private val defaultPrescribedDomainId: Option[DomainId] = None
    private val defaultPrescribedSubmitterDomainId: Option[DomainId] = None

    private val defaultDomainProtocolVersion: DomainId => ProtocolVersion = _ =>
      BaseTest.testedProtocolVersion

    def selectorForExerciseByInterface(
        priorityOfDomain: DomainId => Int = defaultPriorityOfDomain,
        domainOfContracts: Seq[LfContractId] => Map[LfContractId, DomainId] =
          defaultDomainOfContracts,
        connectedDomains: Set[DomainId] = Set(defaultDomain),
        admissibleDomains: NonEmpty[Set[DomainId]] = defaultAdmissibleDomains,
        prescribedDomainAlias: Option[String] = defaultPrescribedDomainAlias,
        prescribedDomainId: Option[DomainId] = defaultPrescribedDomainId,
        prescribedSubmitterDomainId: Option[DomainId] = defaultPrescribedSubmitterDomainId,
        domainProtocolVersion: DomainId => ProtocolVersion = defaultDomainProtocolVersion,
        transactionVersion: TransactionVersion = fixtureTransactionVersion,
        vettedPackages: Seq[LfPackageId] = ExerciseByInterface.correctPackages,
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
        prescribedDomainAlias,
        prescribedDomainId,
        prescribedSubmitterDomainId,
        domainProtocolVersion,
        vettedPackages,
        exerciseByInterface.tx,
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
        prescribedDomainAlias: Option[String] = defaultPrescribedDomainAlias,
        domainProtocolVersion: DomainId => ProtocolVersion = defaultDomainProtocolVersion,
        vettedPackages: Seq[LfPackageId] = ExerciseByInterface.correctPackages,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
        loggerFactory: NamedLoggerFactory,
    ): Selector = {

      val inputContractStakeholders = threeExercises.inputContractIds.map { inputContractId =>
        inputContractId -> Set(signatory, observer)
      }.toMap

      new Selector(loggerFactory)(
        priorityOfDomain,
        domainOfContracts,
        connectedDomains,
        admissibleDomains,
        prescribedDomainAlias,
        None,
        None,
        domainProtocolVersion,
        vettedPackages,
        threeExercises.tx,
        inputContractStakeholders,
      )
    }

    class Selector(loggerFactory: NamedLoggerFactory)(
        priorityOfDomain: DomainId => Int,
        domainOfContracts: Seq[LfContractId] => Map[LfContractId, DomainId],
        connectedDomains: Set[DomainId],
        admissibleDomains: NonEmpty[Set[DomainId]],
        prescribedDomainAlias: Option[String],
        prescribedDomainId: Option[DomainId],
        prescribedSubmitterDomainId: Option[DomainId],
        domainProtocolVersion: DomainId => ProtocolVersion,
        vettedPackages: Seq[LfPackageId],
        tx: LfVersionedTransaction,
        inputContractStakeholders: Map[LfContractId, Set[Ref.Party]],
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
                correctTopology,
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

      private val domainAliasResolver: DomainAlias => Option[DomainId] = alias =>
        Some(
          DomainId(
            UniqueIdentifier(Identifier.tryCreate(alias.unwrap), DefaultTestIdentities.namespace)
          )
        )

      private val domainRankComputation = new DomainRankComputation(
        participantId = submitterParticipantId,
        priorityOfDomain = priorityOfDomain,
        snapshotProvider = TestDomainStateProvider,
        loggerFactory = loggerFactory,
      )

      private val transactionDataET = TransactionData
        .create(
          submitters = Set(signatory),
          transaction = tx,
          workflowIdO = prescribedDomainAlias
            .map(LfWorkflowId.assertFromString)
            .orElse(
              prescribedDomainId
                .map(domainId => s"workflow data domain-id:${domainId.toProtoPrimitive}")
                .map(LfWorkflowId.assertFromString)
            ),
          domainStateProvider = TestDomainStateProvider,
          domainIdResolver = domainAliasResolver,
          contractRoutingParties = inputContractStakeholders,
          submitterDomainId = prescribedSubmitterDomainId,
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
