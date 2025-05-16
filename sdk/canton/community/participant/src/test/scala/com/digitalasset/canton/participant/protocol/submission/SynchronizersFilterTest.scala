// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.submission.SynchronizerSelectionFixture.*
import com.digitalasset.canton.participant.protocol.submission.SynchronizerSelectionFixture.Transactions.ExerciseByInterface
import com.digitalasset.canton.participant.protocol.submission.SynchronizersFilterTest.*
import com.digitalasset.canton.protocol.{LfLanguageVersion, LfVersionedTransaction}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{DamlLfVersionToProtocolVersions, ProtocolVersion}
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext, LfPartyId}
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.Implicits.*
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

class SynchronizersFilterTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown {
  "SynchronizersFilter (simple create)" should {
    import SimpleTopology.*

    val ledgerTime = CantonTimestamp.now()

    val filter = SynchronizersFilterForTx(
      Transactions.Create.tx(fixtureTransactionVersion),
      ledgerTime,
      testedProtocolVersion,
    )
    val correctPackages = Transactions.Create.correctPackages

    "keep synchronizers that satisfy all the constraints" in {
      val (unusableSynchronizers, usableSynchronizers) =
        filter.split(correctTopology, correctPackages).futureValueUS

      unusableSynchronizers shouldBe empty
      usableSynchronizers shouldBe List(DefaultTestIdentities.physicalSynchronizerId)
    }

    "reject synchronizers when informees don't have an active participant" in {
      val partyNotConnected = observer
      val topology = correctTopology.filterNot { case (partyId, _) => partyId == partyNotConnected }

      val (unusableSynchronizers, usableSynchronizers) =
        filter.split(topology, correctPackages).futureValueUS

      unusableSynchronizers shouldBe List(
        UsableSynchronizers.MissingActiveParticipant(
          DefaultTestIdentities.physicalSynchronizerId,
          Set(partyNotConnected),
        )
      )
      usableSynchronizers shouldBe empty
    }

    "reject synchronizers when packages are not valid at the requested ledger time" in {
      def runWithModifiedVettedPackage(
          validFrom: Option[CantonTimestamp] = None,
          validUntil: Option[CantonTimestamp] = None,
      ) = {
        val packageNotValid = defaultPackageId
        val packagesWithModifedValidityPeriod = correctPackages.map(vp =>
          if (vp.packageId == packageNotValid)
            vp.copy(validFromInclusive = validFrom, validUntilExclusive = validUntil)
          else vp
        )
        val (unusableSynchronizers, usableSynchronizers) =
          filter
            .split(correctTopology, packagesWithModifedValidityPeriod)
            .futureValueUS
        usableSynchronizers shouldBe empty

        unusableSynchronizers shouldBe List(
          UsableSynchronizers.UnknownPackage(
            DefaultTestIdentities.physicalSynchronizerId,
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

    "reject synchronizers when packages are not missing" in {
      val missingPackage = defaultPackageId
      val packages = correctPackages.filterNot(_.packageId == missingPackage)

      val (unusableSynchronizers, usableSynchronizers) =
        filter.split(correctTopology, packages).futureValueUS
      usableSynchronizers shouldBe empty

      unusableSynchronizers shouldBe List(
        UsableSynchronizers.UnknownPackage(
          DefaultTestIdentities.physicalSynchronizerId,
          List(
            unknownPackageFor(submitterParticipantId, missingPackage),
            unknownPackageFor(observerParticipantId, missingPackage),
          ),
        )
      )
    }

    // TODO(#15561) Re-enable this test when we have a stable protocol version
    "reject synchronizers when the minimum protocol version is not satisfied " ignore {
      import SimpleTopology.*

      // LanguageVersion.VDev needs pv=dev so we use pv=6
      val currentSynchronizerPV = ProtocolVersion.v34
      val filter =
        SynchronizersFilterForTx(
          Transactions.Create.tx(LfLanguageVersion.v2_dev),
          ledgerTime,
          currentSynchronizerPV,
        )

      val (unusableSynchronizers, usableSynchronizers) =
        filter
          .split(correctTopology, Transactions.Create.correctPackages)
          .futureValueUS
      val requiredPV = DamlLfVersionToProtocolVersions.damlLfVersionToMinimumProtocolVersions
        .get(LfLanguageVersion.v2_dev)
        .value
      unusableSynchronizers shouldBe List(
        UsableSynchronizers.UnsupportedMinimumProtocolVersion(
          synchronizerId = DefaultTestIdentities.physicalSynchronizerId,
          requiredPV = requiredPV,
          lfVersion = LfLanguageVersion.v2_dev,
        )
      )
      usableSynchronizers shouldBe empty
    }
  }

  "SynchronizersFilter (simple exercise by interface)" should {
    import SimpleTopology.*
    val exerciseByInterface = Transactions.ExerciseByInterface(fixtureTransactionVersion)

    val ledgerTime = CantonTimestamp.now()
    val filter = SynchronizersFilterForTx(exerciseByInterface.tx, ledgerTime, testedProtocolVersion)
    val correctPackages = ExerciseByInterface.correctPackages

    "keep synchronizers that satisfy all the constraints" in {
      val (unusableSynchronizers, usableSynchronizers) =
        filter.split(correctTopology, correctPackages).futureValueUS

      unusableSynchronizers shouldBe empty
      usableSynchronizers shouldBe List(DefaultTestIdentities.physicalSynchronizerId)
    }

    "reject synchronizers when packages are missing" in {
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

        val (unusableSynchronizers, usableSynchronizers) =
          filter.split(correctTopology, packages).futureValueUS

        usableSynchronizers shouldBe empty
        unusableSynchronizers shouldBe List(
          UsableSynchronizers.UnknownPackage(
            DefaultTestIdentities.physicalSynchronizerId,
            unknownPackageFor(submitterParticipantId) ++ unknownPackageFor(observerParticipantId),
          )
        )
      }
    }
  }

}

private[submission] object SynchronizersFilterTest {
  final case class SynchronizersFilterForTx(
      tx: LfVersionedTransaction,
      ledgerTime: CantonTimestamp,
      synchronizerProtocolVersion: ProtocolVersion,
  ) {
    def split(
        topology: Map[LfPartyId, List[ParticipantId]],
        packages: Seq[VettedPackage] = Seq(),
    )(implicit
        ec: ExecutionContext,
        tc: TraceContext,
    ): FutureUnlessShutdown[
      (List[UsableSynchronizers.SynchronizerNotUsedReason], List[PhysicalSynchronizerId])
    ] = {
      val synchronizers = List(
        (
          DefaultTestIdentities.physicalSynchronizerId
            .copy(protocolVersion = synchronizerProtocolVersion),
          SimpleTopology.defaultTestingIdentityFactory(topology, packages),
        )
      )

      UsableSynchronizers.check(
        synchronizers = synchronizers,
        transaction = tx,
        ledgerTime = ledgerTime,
      )
    }
  }
}
