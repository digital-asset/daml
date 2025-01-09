// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.submission.DomainSelectionFixture.*
import com.digitalasset.canton.participant.protocol.submission.DomainSelectionFixture.Transactions.ExerciseByInterface
import com.digitalasset.canton.participant.protocol.submission.DomainsFilterTest.*
import com.digitalasset.canton.protocol.{LfLanguageVersion, LfVersionedTransaction}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{DamlLfVersionToProtocolVersions, ProtocolVersion}
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext, LfPartyId}
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.Implicits.*
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

class DomainsFilterTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown {
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
        filter.split(correctTopology, correctPackages).futureValueUS

      unusableDomains shouldBe empty
      usableDomains shouldBe List(DefaultTestIdentities.synchronizerId)
    }

    "reject domains when informees don't have an active participant" in {
      val partyNotConnected = observer
      val topology = correctTopology.filterNot { case (partyId, _) => partyId == partyNotConnected }

      val (unusableDomains, usableDomains) = filter.split(topology, correctPackages).futureValueUS

      unusableDomains shouldBe List(
        UsableSynchronizers.MissingActiveParticipant(
          DefaultTestIdentities.synchronizerId,
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
            .split(correctTopology, packagesWithModifedValidityPeriod)
            .futureValueUS
        usableDomains shouldBe empty

        unusableDomains shouldBe List(
          UsableSynchronizers.UnknownPackage(
            DefaultTestIdentities.synchronizerId,
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
        filter.split(correctTopology, packages).futureValueUS
      usableDomains shouldBe empty

      unusableDomains shouldBe List(
        UsableSynchronizers.UnknownPackage(
          DefaultTestIdentities.synchronizerId,
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
      val currentDomainPV = ProtocolVersion.v33
      val filter =
        DomainsFilterForTx(
          Transactions.Create.tx(LfLanguageVersion.v2_dev),
          ledgerTime,
          currentDomainPV,
        )

      val (unusableDomains, usableDomains) =
        filter
          .split(correctTopology, Transactions.Create.correctPackages)
          .futureValueUS
      val requiredPV = DamlLfVersionToProtocolVersions.damlLfVersionToMinimumProtocolVersions
        .get(LfLanguageVersion.v2_dev)
        .value
      unusableDomains shouldBe List(
        UsableSynchronizers.UnsupportedMinimumProtocolVersion(
          synchronizerId = DefaultTestIdentities.synchronizerId,
          currentPV = currentDomainPV,
          requiredPV = requiredPV,
          lfVersion = LfLanguageVersion.v2_dev,
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
        filter.split(correctTopology, correctPackages).futureValueUS

      unusableDomains shouldBe empty
      usableDomains shouldBe List(DefaultTestIdentities.synchronizerId)
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
          filter.split(correctTopology, packages).futureValueUS

        usableDomains shouldBe empty
        unusableDomains shouldBe List(
          UsableSynchronizers.UnknownPackage(
            DefaultTestIdentities.synchronizerId,
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
        topology: Map[LfPartyId, List[ParticipantId]],
        packages: Seq[VettedPackage] = Seq(),
    )(implicit
        ec: ExecutionContext,
        tc: TraceContext,
    ): FutureUnlessShutdown[
      (List[UsableSynchronizers.SynchronizerNotUsedReason], List[SynchronizerId])
    ] = {
      val domains = List(
        (
          DefaultTestIdentities.synchronizerId,
          domainProtocolVersion,
          SimpleTopology.defaultTestingIdentityFactory(topology, packages),
        )
      )

      UsableSynchronizers.check(
        domains = domains,
        transaction = tx,
        ledgerTime = ledgerTime,
      )
    }
  }
}
