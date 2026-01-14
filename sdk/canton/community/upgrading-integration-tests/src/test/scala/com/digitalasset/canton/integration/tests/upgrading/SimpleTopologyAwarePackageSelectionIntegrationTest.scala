// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.damltests.appupgrade.v1.java.appupgrade.{
  AppInstall as AppInstall_V1,
  AppInstallRequest,
  AppInstallRequest as AppInstallRequest_V1,
}
import com.digitalasset.canton.damltests.token
import com.digitalasset.canton.error.TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.PartyId
import monocle.macros.syntax.lens.*

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.chaining.scalaUtilChainingOps

abstract class SimpleTopologyAwarePackageSelectionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  @volatile var v2Hash: String = _
  @volatile var appInstallRequest: AppInstallRequest.Contract = _
  @volatile var provider, user: PartyId = _
  @volatile var providerParticipant, userParticipant: LocalParticipantReference = _

  def featureEnabled: Boolean
  def expectedResult(cmd: => Unit): Unit

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.ledgerApi.topologyAwarePackageSelection.enabled).replace(featureEnabled)
        )
      )
      .withSetup { implicit env =>
        import env.*

        providerParticipant = participant1
        userParticipant = participant2

        providerParticipant.synchronizers.connect_local(sequencer1, alias = daName)
        userParticipant.synchronizers.connect_local(sequencer1, alias = daName)

        providerParticipant.dars.upload(UpgradingBaseTest.AppUpgradeV1)
        userParticipant.dars.upload(UpgradingBaseTest.AppUpgradeV1)

        v2Hash = providerParticipant.dars.upload(
          UpgradingBaseTest.AppUpgradeV2,
          vetAllPackages = false,
          synchronizeVetting = false,
        )
        provider = providerParticipant.parties.enable(
          "provider",
          synchronizeParticipants = Seq(userParticipant),
        )
        user = userParticipant.parties.enable(
          "user",
          synchronizeParticipants = Seq(providerParticipant),
        )
      }

  "Command submission" when {
    "the submitter didn't vet a package that it has locally in its store (V2)" should {
      "does not select it for the package preference" in { _ =>
        appInstallRequest = userParticipant.ledger_api.javaapi.commands
          .submit(
            Seq(user),
            new AppInstallRequest_V1(
              new AppInstall_V1(
                provider.toProtoPrimitive,
                user.toProtoPrimitive,
                BigDecimal(1337L).bigDecimal,
              )
            ).create().commands().asScala.toList,
          )
          .pipe(JavaDecodeUtil.decodeAllCreated(AppInstallRequest_V1.COMPANION))
          .pipe(inside(_) { case Seq(created) => created })

        expectedResult {
          providerParticipant.ledger_api.javaapi.commands.submit(
            Seq(provider),
            appInstallRequest.id.exerciseAppInstall_Accept().commands().asScala.toSeq,
          )
        }
      }
    }

    "the counterparty did not vet V2 package" should {
      "does not select in for the package preference" in { _ =>
        providerParticipant.dars.vetting.enable(v2Hash)

        expectedResult {
          providerParticipant.ledger_api.javaapi.commands.submit(
            Seq(provider),
            appInstallRequest.id.exerciseAppInstall_Accept().commands().asScala.toSeq,
          )
        }
      }
    }

    "two parties have disjoint vettings for a package-name not involved in the submission" should {
      "succeed" in { _ =>
        // Upload Token V1 and V2 to both participants
        providerParticipant.dars.upload(UpgradingBaseTest.TokenV1, vetAllPackages = false)
        userParticipant.dars.upload(UpgradingBaseTest.TokenV1, vetAllPackages = false)
        providerParticipant.dars.upload(UpgradingBaseTest.TokenV2, vetAllPackages = false)
        userParticipant.dars.upload(UpgradingBaseTest.TokenV2, vetAllPackages = false)

        // Vet the token variants disjointly on both participants
        providerParticipant.dars.vetting.enable(token.v1.java.token.Token.PACKAGE_ID)
        userParticipant.dars.vetting.enable(token.v2.java.token.Token.PACKAGE_ID)

        expectedResult {
          providerParticipant.ledger_api.javaapi.commands.submit(
            Seq(provider),
            appInstallRequest.id.exerciseAppInstall_Accept().commands().asScala.toSeq,
          )
        }
      }
    }
  }
}

class EnabledSimpleTopologyAwarePackageSelectionIntegrationTest
    extends SimpleTopologyAwarePackageSelectionIntegrationTest {
  override def featureEnabled: Boolean = true
  override def expectedResult(cmd: => Unit): Unit = cmd
}

class DisabledSimpleTopologyAwarePackageSelectionIntegrationTest
    extends SimpleTopologyAwarePackageSelectionIntegrationTest {
  override def featureEnabled: Boolean = false
  override def expectedResult(cmd: => Unit): Unit =
    assertThrowsAndLogsCommandFailures(
      cmd,
      _.shouldBeCantonErrorCode(InvalidPrescribedSynchronizerId),
    )
}
