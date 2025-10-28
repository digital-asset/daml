// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.submission.SynchronizerSelectionFixture.*
import com.digitalasset.canton.participant.protocol.submission.SynchronizerSelectionFixture.Transactions.ExerciseByInterface
import com.digitalasset.canton.participant.protocol.submission.SynchronizersFilterTest.*
import com.digitalasset.canton.participant.protocol.submission.UsableSynchronizers.UnsupportedMinimumProtocolVersionForInteractiveSubmission
import com.digitalasset.canton.protocol.{LfSerializationVersion, LfVersionedTransaction}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  HashingSchemeVersion,
  LfSerializationVersionToProtocolVersions,
  ProtocolVersion,
}
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutionContext,
  LfPartyId,
  ProtocolVersionChecksAnyWordSpec,
}
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.Implicits.*
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

class SynchronizersFilterTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown
    with ProtocolVersionChecksAnyWordSpec {
  "SynchronizersFilter (simple create)" should {
    import SimpleTopology.*

    val ledgerTime = CantonTimestamp.now()

    val filter = SynchronizersFilterForTx(
      Transactions.Create.tx(fixtureSerializationVersion),
      ledgerTime,
      testedProtocolVersion,
      None,
    )
    val correctPackages = Transactions.Create.correctPackages

    "keep synchronizers that satisfy all the constraints" in {
      val (unusableSynchronizers, usableSynchronizers) =
        filter.split(correctTopology, correctPackages).futureValueUS

      unusableSynchronizers shouldBe empty
      usableSynchronizers shouldBe List(DefaultTestIdentities.physicalSynchronizerId)
    }

    "reject synchronizers when the hashing scheme version is not supported" in {
      val allHashingSchemes =
        HashingSchemeVersion.MinimumProtocolVersionToHashingVersion.values.flatten.toList
      HashingSchemeVersion.MinimumProtocolVersionToHashingVersion.foreach {
        case (pv, supportedHashingSchemes) =>
          allHashingSchemes.filterNot(supportedHashingSchemes.contains).foreach {
            unsupportedHashingScheme =>
              val lfSerializationVersion =
                LfSerializationVersionToProtocolVersions.lfSerializationVersionToMinimumProtocolVersions.collectFirst {
                  case (lfSerialization, minimumPv) if pv >= minimumPv => lfSerialization
                }.value
              val filter =
                SynchronizersFilterForTx(
                  Transactions.Create.tx(lfSerializationVersion),
                  ledgerTime,
                  pv,
                  Some(unsupportedHashingScheme),
                )

              val (unusableSynchronizers, usableSynchronizers) =
                filter.split(correctTopology, correctPackages).futureValueUS

              unusableSynchronizers shouldBe List(
                UnsupportedMinimumProtocolVersionForInteractiveSubmission(
                  synchronizerId = DefaultTestIdentities.physicalSynchronizerId,
                  requiredPV =
                    HashingSchemeVersion.minProtocolVersionForHSV(unsupportedHashingScheme),
                  isVersion = unsupportedHashingScheme,
                )
              )
              usableSynchronizers shouldBe empty
          }
      }
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

    /*
    Running with pv=dev does not make any sense since we want oldPV < newPV=dev
    Running with 35 <= pv < dev would make sense but makes the test setup more complicated and does change the scenario.
     */
    "reject synchronizers when the minimum protocol version is not satisfied " onlyRunWith ProtocolVersion.v34 in {
      import SimpleTopology.*

      // LanguageVersion.VDev needs pv=dev so we use pv=6
      val currentSynchronizerPV = ProtocolVersion.v34
      val filter =
        SynchronizersFilterForTx(
          Transactions.Create.tx(LfSerializationVersion.VDev),
          ledgerTime,
          currentSynchronizerPV,
          Option.empty[HashingSchemeVersion],
        )

      val (unusableSynchronizers, usableSynchronizers) =
        filter
          .split(correctTopology, Transactions.Create.correctPackages)
          .futureValueUS
      val requiredPV =
        LfSerializationVersionToProtocolVersions.lfSerializationVersionToMinimumProtocolVersions
          .get(LfSerializationVersion.VDev)
          .value
      unusableSynchronizers shouldBe List(
        UsableSynchronizers.UnsupportedMinimumProtocolVersion(
          synchronizerId = DefaultTestIdentities.physicalSynchronizerId,
          requiredPV = requiredPV,
          lfVersion = LfSerializationVersion.VDev,
        )
      )
      usableSynchronizers shouldBe empty
    }
  }

  "SynchronizersFilter (simple exercise by interface)" should {
    import SimpleTopology.*
    val exerciseByInterface = Transactions.ExerciseByInterface(fixtureSerializationVersion)

    val ledgerTime = CantonTimestamp.now()
    val filter = SynchronizersFilterForTx(
      exerciseByInterface.tx,
      ledgerTime,
      testedProtocolVersion,
      Some(HashingSchemeVersion.V2),
    )
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
      hashingSchemeVersion: Option[HashingSchemeVersion],
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
        hashingSchemeVersion = hashingSchemeVersion,
      )
    }
  }
}
