// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.javaapi.data.CreatedEvent
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.damltests.appinstall.v2.java.appinstall.{
  AppInstall as AppInstallV2,
  AppInstallRequest as AppInstallRequestV2,
}
import com.digitalasset.canton.damltests.featuredapprightimpl.v1.java.featuredapprightimpl.FeaturedAppRightImpl as FeaturedAppRightImplV1
import com.digitalasset.canton.damltests.featuredapprightimpl.v2.java.featuredapprightimpl.FeaturedAppRightImpl as FeaturedAppRightImplV2
import com.digitalasset.canton.damltests.featuredapprightimpl.v2.java.featuredapprightv1.FeaturedAppRight
import com.digitalasset.canton.damltests.{appinstall, featuredapprightimpl}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.Interpreter.LookupErrors.UnresolvedPackageName
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.{PartyId, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.util.SetupPackageVetting
import com.digitalasset.canton.{LfPackageName, SynchronizerAlias}
import com.digitalasset.daml.lf.data.Ref
import monocle.macros.syntax.lens.*

import java.util.Optional
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala}
import scala.jdk.OptionConverters.RichOption
import scala.util.chaining.scalaUtilChainingOps

import UpgradingBaseTest.Syntax.*

final class ComplexTopologyAwarePackageSelectionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )

  @volatile private var provider, user, dso: PartyId = _
  @volatile private var providerParticipant, userParticipant,
      dsoParticipant: LocalParticipantReference = _
  @volatile private var IAppRight, AppRightV1, AppRightV2: Ref.PackageId = _
  @volatile private var InstallV1, InstallV2: Ref.PackageId = _
  @volatile private var GlobalSynchronizerId, PrivateSynchronizerId: PhysicalSynchronizerId = _

  private val GlobalSynchronizerName = SynchronizerAlias.tryCreate("global")
  private val PrivateSynchronizerName = SynchronizerAlias.tryCreate("private")

  private lazy val AllDars = Set(
    UpgradingBaseTest.FeaturedAppRightIface,
    UpgradingBaseTest.FeaturedAppRightImplV1,
    UpgradingBaseTest.FeaturedAppRightImplV2,
    UpgradingBaseTest.AppInstallV1,
    UpgradingBaseTest.AppInstallV2,
  )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.ledgerApi.topologyAwarePackageSelection.enabled).replace(true)
        )
      )
      .withSetup { implicit env =>
        import env.*

        GlobalSynchronizerId = sequencer1.physical_synchronizer_id
        PrivateSynchronizerId = sequencer2.physical_synchronizer_id

        // Disambiguate participants
        providerParticipant = participant1
        userParticipant = participant2
        dsoParticipant = participant3

        // Connect all participants to the global synchronizer
        participants.all.synchronizers
          .connect_local(sequencer1, alias = GlobalSynchronizerName)

        // Only the provider and user participants connect to the private synchronizer
        Seq(providerParticipant, userParticipant)
          .foreach(
            _.synchronizers.connect_local(sequencer2, alias = PrivateSynchronizerName)
          )

        // Assign parties
        provider = providerParticipant.parties.enable(
          "provider",
          synchronizer = GlobalSynchronizerName,
        )
        providerParticipant.parties.enable("provider", synchronizer = PrivateSynchronizerName)
        user = userParticipant.parties.enable("user", synchronizer = GlobalSynchronizerName)
        userParticipant.parties.enable("user", synchronizer = PrivateSynchronizerName)
        dso = dsoParticipant.parties.enable("dso", synchronizer = GlobalSynchronizerName)

        // Upload DARs to participants but do not vet as vetting state setup is done in the test
        Seq(providerParticipant, userParticipant, dsoParticipant).foreach { p =>
          IAppRight = p.dars
            .upload(UpgradingBaseTest.FeaturedAppRightIface, vetAllPackages = false)
            .pipe(Ref.PackageId.assertFromString)

          AppRightV1 = p.dars
            .upload(UpgradingBaseTest.FeaturedAppRightImplV1, vetAllPackages = false)
            .pipe(Ref.PackageId.assertFromString)
          AppRightV2 = p.dars
            .upload(UpgradingBaseTest.FeaturedAppRightImplV2, vetAllPackages = false)
            .pipe(Ref.PackageId.assertFromString)

          InstallV1 = p.dars
            .upload(UpgradingBaseTest.AppInstallV1, vetAllPackages = false)
            .pipe(Ref.PackageId.assertFromString)
          InstallV2 = p.dars
            .upload(UpgradingBaseTest.AppInstallV2, vetAllPackages = false)
            .pipe(Ref.PackageId.assertFromString)
        }
      }

  "Featured App Right flow" when {
    "on GS (single-synchronizer)" when {
      "all parties are on V2, but aware of V1 as well (V2 AppRight and V2 AppInstall args)" should {
        "succeed" in { _ =>
          test(
            vettingState = Map(
              GlobalSynchronizerId -> Map(
                dsoParticipant -> Set(IAppRight, AppRightV1, AppRightV2),
                providerParticipant -> Set(IAppRight, AppRightV1, AppRightV2, InstallV1, InstallV2),
                userParticipant -> Set(IAppRight, AppRightV1, AppRightV2, InstallV1, InstallV2),
              )
            ),
            expectAppRightVersionPkgId = AppRightV2,
            expectAppInstallVersionPkgId = InstallV2,
            expectAppActivityMarkerVersionPkgId = AppRightV2,
            expectedSynchronizerAppRight = GlobalSynchronizerId,
            expectedSynchronizerAppInstallRequest = GlobalSynchronizerId,
            expectedSynchronizerAppInstall = GlobalSynchronizerId,
          )
        }
      }

      "user is on V1 with dso and provider on V2 (V1 AppRight and V1 AppInstall args)" should {
        "succeed" in { _ =>
          test(
            vettingState = Map(
              GlobalSynchronizerId -> Map(
                dsoParticipant -> Set(IAppRight, AppRightV1, AppRightV2),
                providerParticipant -> Set(IAppRight, AppRightV1, AppRightV2, InstallV1, InstallV2),
                userParticipant -> Set(IAppRight, AppRightV1, InstallV1),
              )
            ),
            expectAppRightVersionPkgId = AppRightV2,
            expectAppInstallVersionPkgId = InstallV1,
            expectAppActivityMarkerVersionPkgId = AppRightV1,
            expectedSynchronizerAppRight = GlobalSynchronizerId,
            expectedSynchronizerAppInstallRequest = GlobalSynchronizerId,
            expectedSynchronizerAppInstall = GlobalSynchronizerId,
          )
        }
      }

      "user and provider are on V1 with dso on V2 (V1 AppRight and V1 AppInstall args)" should {
        "succeed" in { _ =>
          test(
            vettingState = Map(
              GlobalSynchronizerId -> Map(
                dsoParticipant -> Set(IAppRight, AppRightV1, AppRightV2),
                providerParticipant -> Set(IAppRight, AppRightV1, InstallV1),
                userParticipant -> Set(IAppRight, AppRightV1, InstallV1),
              )
            ),
            expectAppRightVersionPkgId = FeaturedAppRightImplV1.COMPANION.PACKAGE_ID,
            expectAppInstallVersionPkgId = InstallV1,
            expectAppActivityMarkerVersionPkgId = AppRightV1,
            expectedSynchronizerAppRight = GlobalSynchronizerId,
            expectedSynchronizerAppInstallRequest = GlobalSynchronizerId,
            expectedSynchronizerAppInstall = GlobalSynchronizerId,
          )
        }
      }

      "all parties are on V1 (V1 AppRight and V1 AppInstall args)" should {
        "succeed" in { _ =>
          test(
            vettingState = Map(
              GlobalSynchronizerId -> Map(
                dsoParticipant -> Set(IAppRight, AppRightV1),
                providerParticipant -> Set(IAppRight, AppRightV1, InstallV1),
                userParticipant -> Set(IAppRight, AppRightV1, InstallV1),
              )
            ),
            expectAppRightVersionPkgId = FeaturedAppRightImplV1.COMPANION.PACKAGE_ID,
            expectAppInstallVersionPkgId = InstallV1,
            expectAppActivityMarkerVersionPkgId = AppRightV1,
            expectedSynchronizerAppRight = GlobalSynchronizerId,
            expectedSynchronizerAppInstallRequest = GlobalSynchronizerId,
            expectedSynchronizerAppInstall = GlobalSynchronizerId,
          )
        }
      }

      // DISABLED TEST. Please ignore
      // This should be a negative test that is currently passing due to
      // vetting checks in phases 1 and 3 that do not check the vetting of input contracts creation package-ids
      "user does not know of V2 AppRight but knows about AppInstall V2" should {
        "fail" ignore { _ =>
          //      userParticipant.dars.remove(AppRightV2) // Uncomment to see the test crash with an EngineError due to missing package in phase 3
          test(
            vettingState = Map(
              GlobalSynchronizerId -> Map(
                dsoParticipant -> Set(IAppRight, AppRightV1, AppRightV2),
                providerParticipant -> Set(IAppRight, AppRightV1, AppRightV2, InstallV2),
                userParticipant -> Set(IAppRight, AppRightV1, InstallV2),
              )
            ),
            expectAppRightVersionPkgId = AppRightV2,
            expectAppInstallVersionPkgId = InstallV2,
            expectAppActivityMarkerVersionPkgId = AppRightV1,
            expectedSynchronizerAppRight = GlobalSynchronizerId,
            expectedSynchronizerAppInstallRequest = GlobalSynchronizerId,
            expectedSynchronizerAppInstall = GlobalSynchronizerId,
          )
        }
      }

      "dso and user are on disjoint versions" should {
        "fail" in { _ =>
          assertThrowsAndLogsCommandFailures(
            test(
              vettingState = Map(
                GlobalSynchronizerId -> Map(
                  dsoParticipant -> Set(IAppRight, AppRightV2),
                  providerParticipant -> Set(
                    IAppRight,
                    AppRightV1,
                    AppRightV2,
                    InstallV1,
                    InstallV2,
                  ),
                  userParticipant -> Set(IAppRight, AppRightV1, InstallV1),
                )
              ),
              expectAppRightVersionPkgId = AppRightV2,
              expectAppInstallVersionPkgId = InstallV1,
              expectAppActivityMarkerVersionPkgId = AppRightV1,
              expectedSynchronizerAppRight = GlobalSynchronizerId,
              expectedSynchronizerAppInstallRequest = GlobalSynchronizerId,
              expectedSynchronizerAppInstall = GlobalSynchronizerId,
            ),
            // TODO(#25385) Specialize error once package-name discard reason is propagated
            //              Note: Currently we only log the discard reason at DEBUG level
            _.shouldBeCantonErrorCode(UnresolvedPackageName),
          )
        }
      }
    }

    "app install created on private and accepted on global" when {
      "both private and GS have v1 and v2" should {
        "allow creating the request on private and accept on GS" in { _ =>
          test(
            vettingState = Map(
              GlobalSynchronizerId -> Map(
                dsoParticipant -> Set(IAppRight, AppRightV1),
                providerParticipant -> Set(IAppRight, AppRightV1, AppRightV2, InstallV1, InstallV2),
                userParticipant -> Set(IAppRight, AppRightV1, AppRightV2, InstallV1, InstallV2),
              ),
              PrivateSynchronizerId -> Map(
                providerParticipant -> Set(IAppRight, AppRightV1, AppRightV2, InstallV1, InstallV2),
                userParticipant -> Set(IAppRight, AppRightV1, AppRightV2, InstallV1, InstallV2),
              ),
            ),
            expectAppRightVersionPkgId = AppRightV1,
            expectAppInstallVersionPkgId = InstallV2,
            expectAppActivityMarkerVersionPkgId = AppRightV1,
            expectedSynchronizerAppRight = GlobalSynchronizerId,
            expectedSynchronizerAppInstallRequest = PrivateSynchronizerId,
            expectedSynchronizerAppInstall = GlobalSynchronizerId,
            appInstallRequestPrescribedSynchronizer = Some(PrivateSynchronizerId),
          )
        }
      }

      "private sync has only V1 and GS only V2" should {
        // TODO(#25385) Keep this test case around as a target state for the future
        //                   Note: even though GS has an upgraded version of the app,
        //                         the reassignment is rejected due to vetting checks
        "be upgraded and accepted on global" ignore { _ =>
          test(
            vettingState = Map(
              GlobalSynchronizerId -> Map(
                dsoParticipant -> Set(IAppRight, AppRightV2),
                providerParticipant -> Set(IAppRight, AppRightV2, InstallV2),
                userParticipant -> Set(IAppRight, AppRightV2, InstallV2),
              ),
              PrivateSynchronizerId -> Map(
                providerParticipant -> Set(IAppRight, AppRightV1, InstallV1),
                userParticipant -> Set(IAppRight, AppRightV1, InstallV1),
              ),
            ),
            expectAppRightVersionPkgId = AppRightV2,
            expectAppInstallVersionPkgId = InstallV1,
            expectAppActivityMarkerVersionPkgId = AppRightV2,
            expectedSynchronizerAppRight = GlobalSynchronizerId,
            expectedSynchronizerAppInstallRequest = PrivateSynchronizerId,
            expectedSynchronizerAppInstall = GlobalSynchronizerId,
            appInstallRequestPrescribedSynchronizer = Some(PrivateSynchronizerId),
          )
        }
      }
    }

    "Commands.package_id_selection_preferences is specified" should {
      "restrict the packages used in command selection" in { _ =>
        arrangeVettingStateUnbounded(vettingState =
          Map(
            GlobalSynchronizerId -> Map(
              dsoParticipant -> Set(IAppRight, AppRightV1, AppRightV2),
              providerParticipant -> Set(IAppRight, AppRightV1, AppRightV2),
              // We don't care about the user in this test as we just test one command
            )
          )
        )

        val appRightTree = dsoParticipant.ledger_api.javaapi.commands
          .submit(
            Seq(dso),
            new FeaturedAppRightImplV2(
              dso.toProtoPrimitive,
              provider.toProtoPrimitive,
              Optional.empty(),
            ).create().commands().asScala.toSeq,
            userPackageSelectionPreference = Seq(AppRightV1),
          )

        // User preference specifies only V1, so expect V1 to be created
        inside(appRightTree.getEventsById.asScala.head._2) { case created: CreatedEvent =>
          created.getTemplateId shouldBe FeaturedAppRightImplV1.TEMPLATE_ID_WITH_PACKAGE_ID
        }
      }

      "fail" in { _ =>
        arrangeVettingStateUnbounded(vettingState =
          Map(
            GlobalSynchronizerId -> Map(
              dsoParticipant -> Set(IAppRight, AppRightV1),
              providerParticipant -> Set(IAppRight, AppRightV1),
              // We don't care about the user as it's not involved in the flow
            )
          )
        )

        val userPackagePreferenceSet = Seq(AppRightV2)
        // dso prefers V2 even though it only knows about V1
        assertThrowsAndLogsCommandFailures(
          dsoParticipant.ledger_api.javaapi.commands
            .submit(
              Seq(dso),
              new FeaturedAppRightImplV2(
                dso.toProtoPrimitive,
                provider.toProtoPrimitive,
                Optional.empty(),
              ).create().commands().asScala.toSeq,
              userPackageSelectionPreference = userPackagePreferenceSet,
            ),
          _.shouldBeCantonErrorCode(CommandExecutionErrors.UserPackagePreferenceNotVetted),
        )
      }
    }
  }

  private def test(
      vettingState: Map[PhysicalSynchronizerId, Map[LocalParticipantReference, Set[Ref.PackageId]]],
      // The expected package-id of the FeaturedAppRightImpl created event in the IndexDB
      expectAppRightVersionPkgId: String,
      // The expected package-id of the FeaturedAppActivityMarker created event in the IndexDB
      expectAppActivityMarkerVersionPkgId: String,
      // The expected package-id of the AppInstall created event in the IndexDB
      expectAppInstallVersionPkgId: String,
      // The expected synchronizer-id of the AppInstallRequest create
      expectedSynchronizerAppInstallRequest: SynchronizerId,
      // The expected synchronizer-id of the original FeaturedAppRightImpl create
      //  Note: it might be later reassigned but that is checked via the expectedSynchronizerAppInstall
      expectedSynchronizerAppRight: SynchronizerId,
      // The expected synchronizer-id of the AppInstall create
      expectedSynchronizerAppInstall: SynchronizerId,
      // Useful to force the starting point of the flow
      appInstallRequestPrescribedSynchronizer: Option[SynchronizerId] = None,
  ) = {
    arrangeVettingStateUnbounded(vettingState)

    val appRightPreference = dsoParticipant.ledger_api.interactive_submission
      .preferred_packages(
        Map(
          LfPackageName.assertFromString(FeaturedAppRightImplV2.PACKAGE_NAME) -> Set(dso, provider)
        )
      )
      .packageReferences
      .find(_.packageName == FeaturedAppRightImplV2.PACKAGE_NAME)
      .value

    val useV2AppRightArg = appRightPreference.packageId == AppRightV2

    // dso creates FeaturedAppRight
    val appRightTree =
      dsoParticipant.ledger_api.javaapi.commands.submit(
        Seq(dso),
        new FeaturedAppRightImplV2(
          dso.toProtoPrimitive,
          provider.toProtoPrimitive,
          Option.when(useV2AppRightArg)(BigDecimal(1337L).bigDecimal).toJava,
        ).create().commands().asScala.toSeq,
      )

    // Assert synchronizer of the created app right
    appRightTree.getSynchronizerId shouldBe expectedSynchronizerAppRight.toProtoPrimitive

    val appRightObj = appRightTree
      .pipe(JavaDecodeUtil.decodeAllCreated(FeaturedAppRightImplV2.COMPANION))
      .head

    appRightObj.data shouldBe new FeaturedAppRightImplV2(
      dso.toProtoPrimitive,
      provider.toProtoPrimitive,
      Option
        .when(expectAppRightVersionPkgId == FeaturedAppRightImplV2.PACKAGE_ID)(
          BigDecimal(1337L).bigDecimal.setScale(10)
        )
        .toJava,
    )
    val appRightCid: FeaturedAppRight.ContractId =
      appRightObj.id
        .toInterface(featuredapprightimpl.v2.java.featuredapprightv1.FeaturedAppRight.INTERFACE)

    // Assert the created version of the FeaturedAppRight
    appRightTree.getEventsById.asScala.head._2.getTemplateId.getPackageId shouldBe expectAppRightVersionPkgId

    val appInstallRequestPreference =
      userParticipant.ledger_api.interactive_submission
        .preferred_packages(
          Map(LfPackageName.assertFromString(AppInstallV2.PACKAGE_NAME) -> Set(provider, user))
        )
        .packageReferences
        .find(_.packageName == AppInstallV2.PACKAGE_NAME)
        .value

    val useV2AppInstallCreateArg = appInstallRequestPreference.packageId == AppInstallV2.PACKAGE_ID

    // user creates the app install request
    val appInstallRequestTree = userParticipant.ledger_api.javaapi.commands
      .submit(
        actAs = Seq(user),
        commands = new AppInstallRequestV2(
          new AppInstallV2(
            dso.toProtoPrimitive,
            provider.toProtoPrimitive,
            user.toProtoPrimitive,
            BigDecimal(1337L).bigDecimal,
            Option.when(useV2AppInstallCreateArg)(BigDecimal(1338L).bigDecimal).toJava,
          )
        ).create().commands().asScala.toSeq,
        synchronizerId = appInstallRequestPrescribedSynchronizer,
      )

    // Assert synchronizer of the created app install request
    appInstallRequestTree.getSynchronizerId shouldBe expectedSynchronizerAppInstallRequest.toProtoPrimitive

    val appInstallRequestObj = appInstallRequestTree
      .pipe(JavaDecodeUtil.decodeAllCreated(AppInstallRequestV2.COMPANION))
      .head

    appInstallRequestObj.data.install shouldBe new AppInstallV2(
      dso.toProtoPrimitive,
      provider.toProtoPrimitive,
      user.toProtoPrimitive,
      BigDecimal(1337L).bigDecimal.setScale(10),
      Option
        .when(expectAppInstallVersionPkgId == AppInstallV2.PACKAGE_ID)(
          BigDecimal(1338L).bigDecimal.setScale(10)
        )
        .toJava,
    )
    val appInstallRequestCid = appInstallRequestObj.id

    // provider accepts the app install request
    val appInstallTree = providerParticipant.ledger_api.javaapi.commands
      .submit(
        Seq(provider),
        appInstallRequestCid
          .exerciseAppInstallRequest_Accept(
            new appinstall.v2.java.featuredapprightv1.FeaturedAppRight.ContractId(
              appRightCid.contractId
            )
          )
          .commands()
          .asScala
          .toSeq,
      )

    // Assert synchronizer of the created app install and implicitly, of the transaction creating it
    appInstallTree.getSynchronizerId shouldBe expectedSynchronizerAppInstall.toProtoPrimitive

    val createdPackageIds = appInstallTree.getEventsById.asScala.view.values.collect {
      case event: CreatedEvent => event.getTemplateId.getPackageId
    }.toList

    // It's fine to compare blindly since AppInstall and FeaturedAppActivityMarker do not share the same package-name
    createdPackageIds.sorted shouldBe List(
      expectAppInstallVersionPkgId,
      expectAppActivityMarkerVersionPkgId,
    ).sorted
  }

  private def arrangeVettingStateUnbounded(
      vettingState: Map[PhysicalSynchronizerId, Map[LocalParticipantReference, Set[Ref.PackageId]]]
  ): Unit =
    SetupPackageVetting(
      darPaths = AllDars,
      targetTopology = vettingState.map { case (syncId, participantPackages) =>
        syncId -> participantPackages.map { case (participant, packages) =>
          participant -> packages.map(_ withNoVettingBounds)
        }
      },
    )
}
