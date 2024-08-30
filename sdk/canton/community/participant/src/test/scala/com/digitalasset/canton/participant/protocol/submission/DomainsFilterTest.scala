// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.submission.DomainSelectionFixture.Transactions.ExerciseByInterface
import com.digitalasset.canton.participant.protocol.submission.DomainSelectionFixture.*
import com.digitalasset.canton.participant.protocol.submission.DomainsFilterTest.*
import com.digitalasset.canton.protocol.{LfTransactionVersion, LfVersionedTransaction}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{DamlLfVersionToProtocolVersions, ProtocolVersion}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPartyId}
import com.digitalasset.daml.lf.transaction.TransactionVersion
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.Implicits.*
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future}

class DomainsFilterTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  "DomainsFilter (simple create)" should {
    import SimpleTopology.*

    val ledgerTime = CantonTimestamp.now()

    val filter = DomainsFilterForTx(
      Transactions.Create.tx(fixtureTransactionVersion),
      ledgerTime,
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

    "reject domains when packages are not valid at the requested ledger time" in {
      def runWithModifiedVettedPackage(
          validFrom: Option[CantonTimestamp] = None,
          validUntil: Option[CantonTimestamp] = None,
      ) = {
        val packageNotValid = defaultPackageId
        val packagesWithModifedValidityPeriod = correctPackages.map(vp =>
          if (vp.packageId == packageNotValid)
            vp.copy(validFrom = validFrom, validUntil = validUntil)
          else vp
        )
        val (unusableDomains, usableDomains) =
          filter
            .split(loggerFactory, correctTopology, packagesWithModifedValidityPeriod)
            .futureValue
        usableDomains shouldBe empty

        unusableDomains shouldBe List(
          UsableDomain.UnknownPackage(
            DefaultTestIdentities.domainId,
            List(
              unknownPackageFor(submitterParticipantId, packageNotValid),
              unknownPackageFor(observerParticipantId, packageNotValid),
            ),
          )
        )
      }

      runWithModifiedVettedPackage(validFrom = Some(ledgerTime.plusMillis(1L)))
      runWithModifiedVettedPackage(validUntil = Some(ledgerTime.minusMillis(1L)))
    }

    "reject domains when packages are not missing" in {
      val missingPackage = defaultPackageId
      val packages = correctPackages.filterNot(_.packageId == missingPackage)

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

    // TODO(#15561) Re-enable this test when we have a stable protocol version
    "reject domains when the minimum protocol version is not satisfied " ignore {
      import SimpleTopology.*

      // LanguageVersion.VDev needs pv=dev so we use pv=6
      val currentDomainPV = ProtocolVersion.v32
      val filter =
        DomainsFilterForTx(
          Transactions.Create.tx(TransactionVersion.VDev),
          ledgerTime,
          currentDomainPV,
        )

      val (unusableDomains, usableDomains) =
        filter
          .split(loggerFactory, correctTopology, Transactions.Create.correctPackages)
          .futureValue
      val requiredPV = DamlLfVersionToProtocolVersions.damlLfVersionToMinimumProtocolVersions
        .get(LfTransactionVersion.VDev)
        .value
      unusableDomains shouldBe List(
        UsableDomain.UnsupportedMinimumProtocolVersion(
          domainId = DefaultTestIdentities.domainId,
          currentPV = currentDomainPV,
          requiredPV = requiredPV,
          lfVersion = TransactionVersion.VDev,
        )
      )
      usableDomains shouldBe empty
    }
  }

  "DomainsFilter (simple exercise by interface)" should {
    import SimpleTopology.*
    val exerciseByInterface = Transactions.ExerciseByInterface(fixtureTransactionVersion)

    val ledgerTime = CantonTimestamp.now()
    val filter = DomainsFilterForTx(exerciseByInterface.tx, ledgerTime, testedProtocolVersion)
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
        val packages = correctPackages.filterNot(vp => missingPackages.contains(vp.packageId))

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
      ledgerTime: CantonTimestamp,
      domainProtocolVersion: ProtocolVersion,
  ) {
    def split(
        loggerFactory: NamedLoggerFactory,
        topology: Map[LfPartyId, List[ParticipantId]],
        packages: Seq[VettedPackage] = Seq(),
    )(implicit
        ec: ExecutionContext,
        tc: TraceContext,
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
        ledgerTime = ledgerTime,
        domains = domains,
        loggerFactory = loggerFactory,
      ).split
    }
  }
}
