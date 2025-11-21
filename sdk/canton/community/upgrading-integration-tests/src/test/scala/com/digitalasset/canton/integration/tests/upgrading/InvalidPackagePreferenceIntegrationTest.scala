// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.api.v2.commands.Command
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.damltests.upgrade.v1.java.upgrade.{
  UpgradeItCallInterface as UpgradeItCallInterfaceV1,
  UpgradeItTemplate as UpgradeItTemplateV1,
}
import com.digitalasset.canton.damltests.upgrade.v1.java.upgradeif.UpgradeItInterface
import com.digitalasset.canton.damltests.upgrade.v2.java.upgrade.UpgradeItTemplate as UpgradeItTemplateV2
import com.digitalasset.canton.data.ActionDescription.ExerciseActionDescription
import com.digitalasset.canton.data.{
  GenTransactionTree,
  MerkleTree,
  TransactionView,
  ViewParticipantData,
}
import com.digitalasset.canton.integration.tests.security.{
  SecurityTestHelpers,
  SecurityTestLensUtils,
}
import com.digitalasset.canton.integration.util.TestSubmissionService
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.daml.lf.data.Ref
import monocle.macros.GenLens
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*

class InvalidPackagePreferenceIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestLensUtils
    with HasProgrammableSequencer
    with HasCycleUtils
    with SecurityTestHelpers {

  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()
  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1

  private def party(name: String)(implicit env: TestConsoleEnvironment): PartyId =
    env.participant1.parties.list(name).headOption.valueOrFail("where is " + name).party

  "setup the stage" in { implicit env =>
    import env.*
    pureCryptoRef.set(sequencer1.crypto.pureCrypto)
    participant1.synchronizers.connect_local(sequencer1, alias = daName)
    participant1.dars.upload(UpgradingBaseTest.UpgradeV1)
    participant1.dars.upload(UpgradingBaseTest.UpgradeV2)
    participant1.parties.enable("alice")
  }

  val upgradePackageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] =
    TestSubmissionService.packageMapOfCompanions(
      Seq(UpgradeItTemplateV1.COMPANION.PACKAGE, UpgradeItTemplateV2.COMPANION.PACKAGE)
    )

  "interface resolution" should {

    def setupInterface(implicit env: FixtureParam): (PartyId, UpgradeItInterface.ContractId) = {

      import env.participant1

      val alice = party("alice")

      val templateTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new UpgradeItTemplateV1(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        userPackageSelectionPreference =
          Seq(LfPackageId.assertFromString(UpgradeItTemplateV1.PACKAGE_ID)),
      )

      val templateCid: UpgradeItTemplateV1.ContractId =
        JavaDecodeUtil.decodeAllCreated(UpgradeItTemplateV1.COMPANION)(templateTx).loneElement.id

      val interfaceCid1 = templateCid.toInterface(UpgradeItInterface.INTERFACE)

      (alice, interfaceCid1)
    }

    "detect the submission situation where a submitted package preference is invalid" in {
      implicit env =>
        import env.*

        val (alice, interfaceCid1) = setupInterface
        assertThrowsAndLogsCommandFailures(
          {
            val (_, trackingResult) = trackingLedgerEvents(participants.all, Seq.empty) {
              participant1.ledger_api.javaapi.commands.submit(
                Seq(alice),
                interfaceCid1.exerciseUpgradeItStamp("direct").commands().asScala.toSeq,
                userPackageSelectionPreference = Seq(
                  LfPackageId.assertFromString(
                    UpgradeItTemplateV1.TEMPLATE_ID_WITH_PACKAGE_ID.getPackageId
                  ),
                  LfPackageId.assertFromString(
                    UpgradeItTemplateV2.TEMPLATE_ID_WITH_PACKAGE_ID.getPackageId
                  ),
                ),
              )
            }
            trackingResult.assertNoTransactions()
          },
          e => {
            e.level shouldBe Level.ERROR
            e.message should include(
              "The submitted request has invalid arguments: duplicate preference for package-name Upgrade"
            )
          },
        )
    }

    def maliciousPackagePreferenceManipulation(
        packagePreference: Set[LfPackageId],
        expectedWarning: String,
    )(implicit env: FixtureParam): Unit = {
      import env.*
      val (alice, interfaceCid1) = setupInterface

      val preferred = LfPackageId.assertFromString(
        UpgradeItTemplateV1.TEMPLATE_ID_WITH_PACKAGE_ID.getPackageId
      )

      val malicious = MaliciousParticipantNode(
        participant1,
        daId,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
      )

      val callTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new UpgradeItCallInterfaceV1(alice.toProtoPrimitive).create.commands.asScala.toSeq,
      )

      val callCid: UpgradeItCallInterfaceV1.ContractId =
        JavaDecodeUtil.decodeAllCreated(UpgradeItCallInterfaceV1.COMPANION)(callTx).loneElement.id

      val cmd = callCid.exerciseUpgradeItCallStamp(interfaceCid1).commands().loneElement

      // Malicious submission does not succeed
      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        {
          val (_, trackingResult) = trackingLedgerEvents(participants.all, Seq.empty) {

            malicious
              .submitCommand(
                command = CommandsWithMetadata(
                  Seq(Command.fromJavaProto(cmd.toProtoCommand)),
                  Seq(alice),
                  packagePreferenceOverride = Some(Set(preferred)),
                  packageMapOverride = Some(upgradePackageMap),
                ),
                transactionTreeInterceptor = GenTransactionTree.rootViewsUnsafe
                  .andThen(firstElement[TransactionView])
                  .andThen(TransactionView.viewParticipantDataUnsafe)
                  .andThen(MerkleTree.tryUnwrap[ViewParticipantData])
                  .andThen(GenLens[ViewParticipantData].apply(_.actionDescription))
                  .modify {
                    case ex: ExerciseActionDescription =>
                      ExerciseActionDescription.packagePreferenceUnsafe
                        .replace(packagePreference)(ex)
                    case other => other
                  },
              )
              .futureValueUS
              .value
          }
          trackingResult.assertNoTransactions()
        },
        LogEntry.assertLogSeq(
          Seq(
            "TransactionConfirmationResponsesFactory:InvalidPackagePreferenceIntegrationTest"
          ).map(ln =>
            (
              e => {
                e.loggerName should include regex ln
                e.level shouldBe Level.WARN
                e.message should include regex expectedWarning
              },
              s"Didn't find logger: $ln",
            )
          )
        ),
      )

      // Non-malicious does succeed
      participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        callCid.exerciseUpgradeItCallStamp(interfaceCid1).commands().asScala.toSeq,
        userPackageSelectionPreference = Seq(preferred),
      )

    }

    "handle the malicious situation where the package preference is missing" in { implicit env =>
      maliciousPackagePreferenceManipulation(
        packagePreference = Set.empty,
        expectedWarning =
          """(?s)LOCAL_VERDICT_FAILED_MODEL_CONFORMANCE_CHECK.*Rejected transaction due to a failed model conformance check: DAMLeError.*UnresolvedPackageName\("Upgrade"\)""",
      )
    }

    "handle the malicious situation where the package preference is wrong" in { implicit env =>
      maliciousPackagePreferenceManipulation(
        packagePreference = Set(LfPackageId.assertFromString(UpgradeItTemplateV2.PACKAGE_ID)),
        expectedWarning =
          raw"(?s)LOCAL_VERDICT_FAILED_MODEL_CONFORMANCE_CHECK.*Rejected transaction due to a failed model conformance check: ViewReconstructionError.*cause = Reconstructed view differs from received view.",
      )
    }

    "handle the malicious situation where the package preference is ambiguous" in { implicit env =>
      maliciousPackagePreferenceManipulation(
        packagePreference = Set(UpgradeItTemplateV1.PACKAGE_ID, UpgradeItTemplateV2.PACKAGE_ID)
          .map(LfPackageId.assertFromString),
        expectedWarning =
          raw"(?s)LOCAL_VERDICT_FAILED_MODEL_CONFORMANCE_CHECK.*Rejected transaction due to a failed model conformance check: ConflictingNameBindings.*has detected conflicting package name resolutions:.*Upgrade ->",
      )
    }

    "handle the malicious situation where the package preference is unknown" in { implicit env =>
      val madeUpPkg = "MadeUpPkg"
      maliciousPackagePreferenceManipulation(
        packagePreference = Set(LfPackageId.assertFromString(madeUpPkg)),
        expectedWarning =
          raw"(?s)LOCAL_VERDICT_FAILED_MODEL_CONFORMANCE_CHECK.*Rejected transaction due to a failed model conformance check: PackageNotFound.*$madeUpPkg",
      )
    }

  }
}
