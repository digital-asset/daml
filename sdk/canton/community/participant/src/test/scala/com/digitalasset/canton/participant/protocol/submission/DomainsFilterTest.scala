// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.transaction.test.TransactionBuilder.Implicits.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.submission.DomainSelectionFixture.Transactions.ExerciseByInterface
import com.digitalasset.canton.participant.protocol.submission.DomainSelectionFixture.*
import com.digitalasset.canton.participant.protocol.submission.DomainsFilterTest.*
import com.digitalasset.canton.protocol.LfVersionedTransaction
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPackageId, LfPartyId}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future}

class DomainsFilterTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  "DomainsFilter (simple create)" should {
    import SimpleTopology.*

    val filter = DomainsFilterForTx(
      Transactions.Create.tx(fixtureTransactionVersion),
      testedProtocolVersion,
    )
    val correctPackages = Transactions.Create.correctPackages

    "keep domains that satisfy all the constraints" in {
      val (unusableDomains, usableDomains) =
        filter.split(loggerFactory, correctTopology, correctPackages).futureValue

      unusableDomains shouldBe empty
      usableDomains shouldBe List(DefaultTestIdentities.domainId)
    }

    "reject domains when informees don't have an active participant" in {
      val partyNotConnected = observer
      val topology = correctTopology.filterNot { case (partyId, _) => partyId == partyNotConnected }

      val (unusableDomains, usableDomains) =
        filter.split(loggerFactory, topology, correctPackages).futureValue

      unusableDomains shouldBe List(
        UsableDomain.MissingActiveParticipant(
          DefaultTestIdentities.domainId,
          Set(partyNotConnected),
        )
      )
      usableDomains shouldBe empty
    }

    "reject domains when packages are missing" in {
      val missingPackage = defaultPackageId
      val packages = correctPackages.filterNot(_ == missingPackage)

      val (unusableDomains, usableDomains) =
        filter.split(loggerFactory, correctTopology, packages).futureValue
      usableDomains shouldBe empty

      unusableDomains shouldBe List(
        UsableDomain.UnknownPackage(
          DefaultTestIdentities.domainId,
          List(
            unknownPackageFor(submitterParticipantId, missingPackage),
            unknownPackageFor(observerParticipantId, missingPackage),
          ),
        )
      )
    }

    "reject domains when the minimum protocol version is not satisfied" in {
      import SimpleTopology.*

      // LanguageVersion.VDev needs pv=dev so we use pv=6
      val currentDomainPV = ProtocolVersion.v6
      val filter =
        DomainsFilterForTx(Transactions.Create.tx(TransactionVersion.VDev), currentDomainPV)

      val (unusableDomains, usableDomains) =
        filter
          .split(loggerFactory, correctTopology, Transactions.Create.correctPackages)
          .futureValue
      unusableDomains shouldBe List(
        UsableDomain.UnsupportedMinimumProtocolVersion(
          domainId = DefaultTestIdentities.domainId,
          currentPV = currentDomainPV,
          requiredPV = ProtocolVersion.dev,
          lfVersion = TransactionVersion.VDev,
        )
      )
      usableDomains shouldBe empty
    }
  }

  "DomainsFilter (simple exercise by interface)" should {
    import SimpleTopology.*
    val exerciseByInterface = Transactions.ExerciseByInterface(fixtureTransactionVersion)

    val filter = DomainsFilterForTx(exerciseByInterface.tx, testedProtocolVersion)
    val correctPackages = ExerciseByInterface.correctPackages

    "keep domains that satisfy all the constraints" in {
      val (unusableDomains, usableDomains) =
        filter.split(loggerFactory, correctTopology, correctPackages).futureValue

      unusableDomains shouldBe empty
      usableDomains shouldBe List(DefaultTestIdentities.domainId)
    }

    "reject domains when packages are missing" in {
      val testInstances = Seq(
        List(defaultPackageId),
        List(Transactions.ExerciseByInterface.interfacePackageId),
        List(defaultPackageId, Transactions.ExerciseByInterface.interfacePackageId),
      )

      forAll(testInstances) { missingPackages =>
        val packages = correctPackages.filterNot(missingPackages.contains)

        def unknownPackageFor(
            participantId: ParticipantId
        ): List[TransactionTreeFactory.PackageUnknownTo] = missingPackages.map { missingPackage =>
          TransactionTreeFactory.PackageUnknownTo(
            missingPackage,
            participantId,
          )
        }

        val (unusableDomains, usableDomains) =
          filter.split(loggerFactory, correctTopology, packages).futureValue

        usableDomains shouldBe empty
        unusableDomains shouldBe List(
          UsableDomain.UnknownPackage(
            DefaultTestIdentities.domainId,
            unknownPackageFor(submitterParticipantId) ++ unknownPackageFor(observerParticipantId),
          )
        )
      }
    }
  }

}

private[submission] object DomainsFilterTest {
  final case class DomainsFilterForTx(
      tx: LfVersionedTransaction,
      domainProtocolVersion: ProtocolVersion,
  ) {
    def split(
        loggerFactory: NamedLoggerFactory,
        topology: Map[LfPartyId, List[ParticipantId]],
        packages: Seq[LfPackageId] = Seq(),
    )(implicit
        ec: ExecutionContext
    ): Future[(List[UsableDomain.DomainNotUsedReason], List[DomainId])] = {
      val domains = List(
        (
          DefaultTestIdentities.domainId,
          domainProtocolVersion,
          SimpleTopology.defaultTestingIdentityFactory(topology, packages),
        )
      )

      DomainsFilter(
        submittedTransaction = tx,
        domains = domains,
        loggerFactory = loggerFactory,
      ).split
    }
  }
}
