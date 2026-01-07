// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.api.v2.interactive.interactive_submission_service.GetPreferredPackagesResponse
import com.daml.ledger.api.v2.package_reference.PackageReference
import com.digitalasset.canton.LfPackageName
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.damltests.appinstall.v1.java.appinstall.AppInstall as AppInstallV1
import com.digitalasset.canton.damltests.appinstall.v2.java.appinstall.AppInstall as AppInstallV2
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.ledger.error.LedgerApiErrors.NoPreferredPackagesFound
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.SetupPackageVetting
import org.scalatest.Assertion

import java.time.Duration

import UpgradingBaseTest.Syntax.*

class PackagePreferenceQueryIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with UpgradingBaseTest.WhenPV {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )

  private val AppInstallPackageName = LfPackageName.assertFromString(AppInstallV2.PACKAGE_NAME)
  private val PackageReferences = Map(
    AppInstallV1.PACKAGE_ID -> PackageReference(
      packageId = AppInstallV1.PACKAGE_ID,
      packageName = AppInstallV1.PACKAGE_NAME,
      packageVersion = AppInstallV1.PACKAGE_VERSION.toString,
    ),
    AppInstallV2.PACKAGE_ID -> PackageReference(
      packageId = AppInstallV2.PACKAGE_ID,
      packageName = AppInstallV2.PACKAGE_NAME,
      packageVersion = AppInstallV2.PACKAGE_VERSION.toString,
    ),
  )
  private val vettingEndsForV1At = CantonTimestamp.now().add(Duration.ofDays(1L))

  @volatile var party1, party2: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.synchronizers.connect_local(sequencer2, alias = acmeName)

        participant2.synchronizers.connect_local(sequencer1, alias = daName)
        participant2.synchronizers.connect_local(sequencer2, alias = acmeName)

        party1 = participant1.parties.enable("party1", synchronizer = daName)
        participant1.parties.enable("party1", synchronizer = acmeName)
        party2 = participant2.parties.enable("party2", synchronizer = daName)
        participant2.parties.enable("party2", synchronizer = acmeName)

        SetupPackageVetting(
          darPaths = Set(UpgradingBaseTest.AppInstallV1, UpgradingBaseTest.AppInstallV2),
          targetTopology = Map(
            // V1 on synchronizer 1
            daId -> Map(
              participant1 -> Set(
                AppInstallV1.PACKAGE_ID.toPackageId.withVettingEndsAt(vettingEndsForV1At),
                AppInstallV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
              ),
              participant2 -> Set(AppInstallV1.PACKAGE_ID.toPackageId.withNoVettingBounds),
            ),
            // Disjoint on synchronizer 2 - no valid preference
            acmeId -> Map(
              participant1 -> Set(AppInstallV2.PACKAGE_ID.toPackageId.withNoVettingBounds),
              participant2 -> Set(AppInstallV1.PACKAGE_ID.toPackageId.withNoVettingBounds),
            ),
          ),
        )

        // TODO(#25385): Remove this upload if participant2 can compute the preference of a counterparty without having
        //               the preferred package stored locally
        //
        // Upload V2 on participant 2 as well,
        // otherwise it can't correctly output the preference of party1 if its highest vetted package is lower
        // than the highest vetted package on participant 1
        participant2.dars.upload(
          UpgradingBaseTest.AppInstallV2,
          vetAllPackages = false,
          synchronizeVetting = false,
        )
      }

  "get_preferred_package_version" should {
    "return the correct response for a package-name, set of parties and the given topology state without synchronizer-id restriction" in {
      implicit env =>
        import env.*

        // Assert both parties preferences
        onAllParticipants { participantRef =>
          val expectedPreference = GetPreferredPackagesResponse(
            packageReferences = Seq(PackageReferences(AppInstallV1.PACKAGE_ID)),
            synchronizerId = daId.logical.toProtoPrimitive,
          )

          val parties = Set(party1, party2)
          participantRef.ledger_api.interactive_submission
            .preferred_packages(
              Map(AppInstallPackageName -> parties)
            ) shouldBe expectedPreference withClue s"Preferred package version should be V1 on ${daId.logical.toProtoPrimitive} as seen by ${participantRef.id.toProtoPrimitive} for parties $parties"
        }

        // Assert preference of party from participant 1
        onAllParticipants { participantRef =>
          val expectedPreference = GetPreferredPackagesResponse(
            packageReferences = Seq(PackageReferences(AppInstallV2.PACKAGE_ID)),
            synchronizerId = daId.logical.toProtoPrimitive,
          )

          val parties = Set(party1)
          val response = participantRef.ledger_api.interactive_submission
            .preferred_packages(Map(AppInstallPackageName -> parties))
          response shouldBe expectedPreference withClue s"Preferred package version should be V2 on ${daId.logical.toProtoPrimitive} as seen by ${participantRef.id.toProtoPrimitive} for parties $parties"
        }

        // Assert preference of party from participant 2
        onAllParticipants { participantRef =>
          val expectedPreference = GetPreferredPackagesResponse(
            packageReferences = Seq(PackageReferences(AppInstallV1.PACKAGE_ID)),
            synchronizerId = daId.logical.toProtoPrimitive,
          )

          val parties = Set(party2)
          participantRef.ledger_api.interactive_submission
            .preferred_packages(
              Map(AppInstallPackageName -> parties)
            ) shouldBe expectedPreference withClue s"Preferred package version should be V1 on ${daId.logical.toProtoPrimitive} as seen by ${participantRef.id.toProtoPrimitive} $parties"
        }
    }

    "using synchronizer-id restriction yields the correct result" in { implicit env =>
      import env.*

      // Restriction by the synchronizer-id of the preference yields the correct result
      onAllParticipants { participantRef =>
        val expectedPreference = GetPreferredPackagesResponse(
          packageReferences = Seq(PackageReferences(AppInstallV1.PACKAGE_ID)),
          synchronizerId = daId.logical.toProtoPrimitive,
        )

        participantRef.ledger_api.interactive_submission
          .preferred_packages(
            Map(AppInstallPackageName -> Set(party1, party2)),
            synchronizerId = Some(daId),
          ) shouldBe expectedPreference withClue s"Preferred package version should be V1 on ${daId.logical.toProtoPrimitive} as seen by ${participantRef.id.toProtoPrimitive}"
      }

      // Restriction by a synchronizer-id for which no preference exists yields no preference
      onAllParticipants { participantRef =>
        assertThrowsAndLogsCommandFailures(
          participantRef.ledger_api.interactive_submission
            .preferred_packages(
              Map(AppInstallPackageName -> Set(party1, party2)),
              synchronizerId = Some(acmeId),
            ),
          _.shouldBeCantonErrorCode(NoPreferredPackagesFound),
        )
      }
    }

    "using the validity period restriction yields the correct result" in { implicit env =>
      import env.*

      // Restriction by a validity timestamp within the bounds for V1 yields a valid preference
      onAllParticipants { participantRef =>
        val expectedPreference = GetPreferredPackagesResponse(
          packageReferences = Seq(PackageReferences(AppInstallV1.PACKAGE_ID)),
          synchronizerId = daId.logical.toProtoPrimitive,
        )

        participantRef.ledger_api.interactive_submission
          .preferred_packages(
            Map(AppInstallPackageName -> Set(party1, party2)),
            vettingValidAt = Some(vettingEndsForV1At.minus(Duration.ofSeconds(1L))),
            synchronizerId = Some(daId),
          ) shouldBe expectedPreference withClue s"Preferred package version should be V1 on ${daId.logical.toProtoPrimitive} as seen by ${participantRef.id}"
      }

      // Restriction by a validity timestamp outside the bounds for V1 yields no preference
      onAllParticipants { participantRef =>
        val vettingValidAt = vettingEndsForV1At.plus(Duration.ofSeconds(1L))
        assertThrowsAndLogsCommandFailures(
          participantRef.ledger_api.interactive_submission
            .preferred_packages(
              Map(AppInstallPackageName -> Set(party1, party2)),
              vettingValidAt = Some(vettingValidAt),
              synchronizerId = Some(daId),
            ),
          _.shouldBeCantonErrorCode(NoPreferredPackagesFound),
        )
      }
    }
  }

  private def onAllParticipants(assertion: LocalParticipantReference => Assertion)(implicit
      env: TestConsoleEnvironment
  ): Unit =
    env.participants.local.foreach(assertion)

}
