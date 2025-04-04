// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pkgdars

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.admin.api.client.data.TemplateId.templateIdsFromJava
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.lfdev.java as M
import com.digitalasset.canton.version.ProtocolVersion

class PackageUploadVersionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with PackageUsableMixin {

  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.H2](
      loggerFactory,
      sequencerGroups = MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .withSetup { env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant2.synchronizers.connect_local(sequencer2, alias = acmeName)
      }

  "package version checks" should {
    "using dev version" when {
      // Note: this test case now only makes sense for stable PVs
      // For alpha protocol versions: ConfigTransform.setProtocolVersion will auto-set alphaVersionSupport=true,
      // which in turn will set unsafeEnableDamlLfDevVersion=true in DAMLe, which will allow the language versions
      if (testedProtocolVersion.isStable || testedProtocolVersion.isBeta) {
        "reject if not enabled" in { implicit env =>
          import env.*
          assertThrowsAndLogsCommandFailures(
            participant2.dars.upload(BaseTest.CantonLfDev),
            _.shouldBeCantonErrorCode(
              CommandExecutionErrors.Package.AllowedLanguageVersions
            ),
          )
        }
      }

      "support if enabled" onlyRunWithOrGreaterThan ProtocolVersion.dev in { implicit env =>
        import env.*

        participant1.dars.upload(BaseTest.CantonLfDev)

        val party =
          participant1.parties.enable("lfdev")

        participant1.ledger_api.javaapi.commands
          .submit(
            Seq(party),
            Seq(
              new M.cantonlfdev.HelloLfDev(
                party.toProtoPrimitive
              ).create.commands.loneElement
            ),
          )

        participant1.ledger_api.state.acs
          .of_party(
            party,
            filterTemplates = templateIdsFromJava(M.cantonlfdev.HelloLfDev.TEMPLATE_ID),
          ) should have length 1

      }

    }
  }

}
