// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.pkgdars

import com.daml.ledger.api.v2.commands.Command
import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Integrity
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.crypto.{CryptoPureApi, SigningKeyUsage}
import com.digitalasset.canton.damltests.java.conflicttest.Many
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
import com.digitalasset.canton.examples.java as M
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseProgrammableSequencer}
import com.digitalasset.canton.integration.tests.pkgdars.PackageUsableMixin
import com.digitalasset.canton.integration.tests.security.SecurityTestHelpers
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.NotFound
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.protocol.ProtocolProcessor
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker.UnvettedPackages
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects
import com.digitalasset.canton.protocol.messages.Verdict
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.TopologyManagerError.ParticipantTopologyManagerError
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignSpecificMappings
import com.digitalasset.canton.topology.transaction.{VettedPackage, VettedPackages}
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.daml.lf.archive.{DamlLf, DarParser, DarReader}
import com.digitalasset.daml.lf.data.Ref.PackageId
import org.scalatest.Assertion

import java.io.File
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*

trait PackageVettingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with PackageUsableMixin
    with SecurityTestSuite
    with AccessTestScenario
    with HasCycleUtils
    with SecurityTestHelpers {

  val ledgerIntegrity: SecurityTest =
    SecurityTest(property = Integrity, asset = "virtual shared ledger")

  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()
  def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.local.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
        participant2.dars.upload(CantonTestsPath, synchronizerId = daId)

        pureCryptoRef.set(participant1.crypto.pureCrypto)
      }

  "auto-vetting of dars works and doesn't block on disconnected synchronizers" taggedAs_ {
    ledgerIntegrity.setHappyCase(_)
  } in { implicit env =>
    import env.*

    def getVetted(synchronizerId: SynchronizerId) =
      participant1.topology.vetted_packages
        .list(
          synchronizerId,
          filterParticipant = participant1.id.filterString,
        )
        .flatMap(_.item.packages)
        .toSet

    val before = getVetted(daId)

    // disconnect acme, but keep da connected
    participant1.synchronizers.disconnect(acmeName)

    // vet CantonTests, let upload automatically select the only connected synchronizer
    participant1.dars.upload(CantonTestsPath, vetAllPackages = true, synchronizeVetting = true)

    val darPackageIds =
      DarReader.assertReadArchiveFromFile(new File(CantonTestsPath)).all.map(_.pkgId).toSet

    // check that vettings from the DAR upload were registered with the sequencer
    val newPackagesInSync = getVetted(daId) -- before
    val newPackagesToBeAddedByDar = darPackageIds -- before.map(_.packageId)

    newPackagesInSync.map(_.packageId) shouldBe newPackagesToBeAddedByDar

    // reconnect acme
    participant1.synchronizers.reconnect(acmeName)

    // wait until acme has observed vetting txs
    participant1.packages.synchronize_vetting()

    // check that acme does *not* receive the vetting changes that were applied
    // to the connected synchronizer
    (getVetted(acmeId) -- before) shouldBe empty

    // uploading a second time (this time specifically on each synchronizer) doesn't make me bail
    participant1.dars.upload(
      CantonTestsPath,
      vetAllPackages = true,
      synchronizeVetting = true,
      synchronizerId = daId,
    )
    participant1.dars.upload(
      CantonTestsPath,
      vetAllPackages = true,
      synchronizeVetting = true,
      synchronizerId = acmeId,
    )

  }

  private def exploitUnknownPackage(role: String): Attack = Attack(
    actor = "ledger api user",
    s"submit a command referring to a package unknown to $role",
    "reject the command",
  )

  private def assertSynchronizerDiscardedPackageNotVettedByReason(
      synchronizerId: PhysicalSynchronizerId,
      partyId: PartyId,
      packageNameWithoutVettedPackages: String,
      expectedErrorCode: ErrorCode = CommandExecutionErrors.PackageSelectionFailed,
      expectedPreambleErrorMessage: String =
        "No synchronizers satisfy the topology requirements for the submitted command",
  )(logEntry: LogEntry): Assertion = {
    logEntry.shouldBeCantonErrorCode(expectedErrorCode)
    logEntry.message should include(expectedPreambleErrorMessage)
    logEntry.message should include(
      show"$synchronizerId: Failed to select package-id for package-name '$packageNameWithoutVettedPackages' appearing in a command root node due to: No package with package-name '$packageNameWithoutVettedPackages' is consistently vetted by all hosting participants of party $partyId"
    )
  }

  "cannot submit command referring to a package that has not been vetted by my participant" taggedAs ledgerIntegrity
    .setAttack(exploitUnknownPackage("the submitting participant")) in { implicit env =>
    import env.*

    val p3p = participant3.id.adminParty
    val p2p = participant2.id.adminParty

    assertThrowsAndLogsCommandFailures(
      participant3.ledger_api.javaapi.commands.submit(
        Seq(participant3.id.adminParty),
        new M.iou.Iou(
          p3p.toProtoPrimitive,
          p2p.toProtoPrimitive,
          new M.iou.Amount(3.toBigDecimal, "CHF"),
          List().asJava,
        ).create.commands.overridePackageId(M.iou.Iou.PACKAGE_ID).asScala.toSeq,
      ),
      _.shouldBeCommandFailure(NotFound.Package),
    )

  }

  "cannot submit a command referring to a package that has not been vetted by a confirming participant" taggedAs ledgerIntegrity
    .setAttack(exploitUnknownPackage("a confirming participant")) in { implicit env =>
    import env.*

    val p3p = participant3.id.adminParty
    val p2p = participant2.id.adminParty

    assertThrowsAndLogsCommandFailures(
      participant2.ledger_api.javaapi.commands.submit(
        Seq(participant2.id.adminParty),
        new M.iou.Iou(
          p2p.toProtoPrimitive,
          p3p.toProtoPrimitive,
          new M.iou.Amount(3.toBigDecimal, "CHF"),
          List().asJava,
        ).create.commands.asScala.toSeq,
      ),
      assertSynchronizerDiscardedPackageNotVettedByReason(
        synchronizerId = daId,
        partyId = participant3.adminParty,
        packageNameWithoutVettedPackages = M.iou.Iou.PACKAGE_NAME,
      ),
    )
  }

  "cannot submit a command referring to a package that has not been vetted by an informee participant" taggedAs ledgerIntegrity
    .setAttack(exploitUnknownPackage("an informee participant")) in { implicit env =>
    import env.*

    participant1.dars.upload(CantonTestsPath, synchronizerId = daId)
    participant1.dars.upload(CantonTestsPath, synchronizerId = acmeId)

    val p3p = participant3.id.adminParty
    val p2p = participant2.id.adminParty
    val p1p = participant1.id.adminParty

    assertThrowsAndLogsCommandFailures(
      participant2.ledger_api.javaapi.commands.submit(
        Seq(participant2.id.adminParty),
        new M.iou.Iou(
          p2p.toProtoPrimitive,
          p1p.toProtoPrimitive,
          new M.iou.Amount(3.toBigDecimal, "CHF"),
          List(p3p.toProtoPrimitive).asJava,
        ).create.commands.asScala.toSeq,
      ),
      assertSynchronizerDiscardedPackageNotVettedByReason(
        synchronizerId = daId,
        partyId = participant3.adminParty,
        packageNameWithoutVettedPackages = M.iou.Iou.PACKAGE_NAME,
      ),
    )
  }

  // TODO(#20873): test the interaction between package vetting and package version selection for upgrading

  "rolls back a view referring to a package that has not been vetted by an informee participant" taggedAs ledgerIntegrity
    .setAttack(
      Attack(
        "a malicious participant",
        "submits a request referring to a package unknown to an informee participant",
        "alarm and rollback the request",
      )
    ) in { implicit env =>
    import env.*

    val maliciousP2 =
      MaliciousParticipantNode(
        participant2,
        daId,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
      )

    val rawCmds =
      new M.iou.Iou(
        participant2.adminParty.toProtoPrimitive,
        participant2.adminParty.toProtoPrimitive,
        new M.iou.Amount(3.toBigDecimal, "CHF"),
        List(participant3.adminParty.toProtoPrimitive).asJava,
      ).create.commands
        .overridePackageId(M.iou.Iou.PACKAGE_ID)
        .asScala
        .toSeq
        .map(c => Command.fromJavaProto(c.toProtoCommand))
    val cmd = CommandsWithMetadata(rawCmds, Seq(participant2.adminParty))

    // We need to disable the check ensuring the mediator does not approve a transaction we
    // have rejected because this test will trigger it.
    // TODO(i15395): to be adapted when a more graceful check is implemented
    ProtocolProcessor.withApprovalContradictionCheckDisabled(loggerFactory) {
      val ((_, events), _) = loggerFactory.assertLoggedWarningsAndErrorsSeq(
        replacingConfirmationResult(
          daId,
          sequencer1,
          mediator1,
          withMediatorVerdict(Verdict.Approve(testedProtocolVersion)),
        ) {
          trackingLedgerEvents(Seq(participant2), Seq.empty) {
            maliciousP2.submitCommand(cmd).futureValueUS
          }
        },
        LogEntry.assertLogSeq(
          // Use (?s) to enable java.util.regex.Pattern.DOTALL pattern matching
          mustContainWithClue = Seq(
            (
              _.shouldBeCantonError(
                MalformedRejects.ModelConformance,
                _ should include regex raw"(?s)DAMLeError.*EngineError.*MissingPackage",
              ),
              "unvetted packages error",
            )
          ),
          mayContain = Seq(
            _.loggerName should include(
              "participant=participant2"
            ) // Ignore errors from malicious P2
          ),
        ),
      )

      events.assertNoTransactions()
    }
  }

  "rolls back a view referring to a package with ledger time outside the package validity period" taggedAs ledgerIntegrity
    .setAttack(
      Attack(
        "a malicious participant",
        "submits a request with ledger time not falling in the validity period of a referred package",
        "alarm and rollback the request",
      )
    ) in { implicit env =>
    import env.*

    val iouPackage = PackageId.assertFromString(M.iou.Iou.PACKAGE_ID)
    val validityEnd = environment.clock.now
    val iouVettedPackage = VettedPackage(
      iouPackage,
      validFromInclusive = None,
      validUntilExclusive = Some(validityEnd),
    )
    participant1.topology.vetted_packages.propose_delta(
      participant1,
      adds = Seq(iouVettedPackage),
      store = daId,
    )
    eventually() {
      participant1.topology.vetted_packages
        .list(daId, filterParticipant = participant1.id.filterString)
        .loneElement
        .item
        .packages should contain(iouVettedPackage)
    }

    val maliciousP2 =
      MaliciousParticipantNode(
        participant2,
        daId,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
      )

    val rawCmds =
      new M.iou.Iou(
        participant2.adminParty.toProtoPrimitive,
        participant1.adminParty.toProtoPrimitive,
        new M.iou.Amount(3.toBigDecimal, "CHF"),
        Nil.asJava,
      ).create.commands
        .overridePackageId(M.iou.Iou.PACKAGE_ID)
        .asScala
        .toSeq
        .map(c => Command.fromJavaProto(c.toProtoCommand))
    val cmd = CommandsWithMetadata(
      rawCmds,
      Seq(participant2.adminParty),
      ledgerTime = validityEnd.plusMillis(1L).underlying,
    )

    val (_, events) = loggerFactory.assertLoggedWarningsAndErrorsSeq(
      trackingLedgerEvents(Seq(participant2), Seq.empty) {
        maliciousP2.submitCommand(cmd).futureValueUS
      },
      LogEntry.assertLogSeq(
        mustContainWithClue = Seq(
          (
            _.shouldBeCantonError(
              MalformedRejects.ModelConformance,
              _ should include(UnvettedPackages(Map(participant1.id -> Set(iouPackage))).toString),
            ),
            "unvetted packages error",
          )
        ),
        mayContain = Seq(
          _.loggerName should include(
            "participant=participant2"
          ) // Ignore errors from malicious P2
        ),
      ),
    )

    events.assertNoTransactions()
  }

  "The topology manager" when {

    val archive = tryReadDar(CantonTestsPath)
    val packId = DamlPackageStore.readPackageId(archive.main)

    def unvettedPackages(packageList: List[DamlLf.Archive])(implicit
        env: TestConsoleEnvironment
    ): Set[PackageId] = {
      import env.*
      val packages = packageList.map(DamlPackageStore.readPackageId).toSet
      packages -- participant3.topology.vetted_packages
        .list(
          store = daId,
          filterParticipant = participant3.id.filterString,
        )
        .flatMap(_.item.packages.map(_.packageId))
        .toSet
    }

    def vettingCmd(
        adds: Seq[DamlLf.Archive] = Seq.empty,
        removes: Seq[DamlLf.Archive] = Seq.empty,
        validFrom: Option[CantonTimestamp] = None,
        validUntil: Option[CantonTimestamp] = None,
        force: ForceFlags = ForceFlags.none,
    )(implicit
        env: TestConsoleEnvironment
    ): Unit = {
      import env.*
      participant3.topology.vetted_packages.propose_delta(
        participant3.id,
        store = daId,
        adds =
          adds.map(DamlPackageStore.readPackageId).map(VettedPackage(_, validFrom, validUntil)),
        removes = removes.map(DamlPackageStore.readPackageId),
        force = force,
      )
      // synchronize package vetting, as "raw" vetting commands are unsynced
      participant3.packages.synchronize_vetting()
    }

    "a package has not been uploaded" must {
      "refuse to vet the package" taggedAs_ { mit =>
        ledgerIntegrity.setAttack(
          Attack(
            actor = "participant operator",
            threat = "vet a missing package",
            mitigation = mit,
          )
        )
      } in { implicit env =>
        import env.*
        val currentVettedPackages = participant3.topology.vetted_packages
          .list(
            store = TopologyStoreId.Synchronizer(daId),
            filterParticipant = participant3.id.filterString,
          )
          .flatMap(_.item.packages.map(_.packageId))
          .toSet

        currentVettedPackages should not contain packId

        // cannot vet as package is unknown
        clue("vetting of missing packages") {
          loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
            vettingCmd(adds = List(archive.main)),
            forAll(_)(
              _.shouldBeCantonErrorCode(
                ParticipantTopologyManagerError.CannotVetDueToMissingPackages
              )
            ),
          )
        }
      }
    }

    val darMainPackageId = new AtomicReference[Option[String]](None)
    "upload the dar but without vetting the package" in { implicit env =>
      import env.*

      clue("upload examples") {
        darMainPackageId.set(
          Some(participant3.dars.upload(CantonTestsPath, vetAllPackages = false))
        )
      }
    }

    "all packages are stored, but a dependent package has not been vetted" must {
      "refuse to vet the package" taggedAs_ { mit =>
        ledgerIntegrity.setAttack(
          Attack(
            actor = "participant operator",
            threat = "vet a package with an unvetted dependency",
            mitigation = mit,
          )
        )
      } in { implicit env =>
        import env.*

        participant3.packages.list().filter(_.packageId == packId) should not be empty

        // cannot vet as dependencies are not vetted
        clue("dependencies are not vetted") {
          assertThrowsAndLogsCommandFailures(
            vettingCmd(adds = List(archive.main)),
            _.shouldBeCantonErrorCode(ParticipantTopologyManagerError.DependenciesNotVetted),
          )
        }
      }
    }

    "all packages have been vetted" must {
      "allow us to use the package" taggedAs ledgerIntegrity.setHappyCase(
        "use a package, if it has been vetted (including all dependencies)"
      ) in { implicit env =>
        import env.*

        // can vet dependencies one by one using force
        archive.dependencies.foreach { dep =>
          vettingCmd(adds = List(dep), force = ForceFlags(ForceFlag.AllowUnvettedDependencies))
          eventually() {
            participant3.topology.vetted_packages
              .list(daId, filterParticipant = participant3.filterString)
              .loneElement
              .item
              .packages
              .map(_.packageId) should contain(PackageId.fromString(dep.getHash).value)
          }
        }
        unvettedPackages(archive.dependencies) shouldBe empty

        // can vet dependencies and then main
        participant3.dars.vetting.enable(darMainPackageId.get().getOrElse(fail("Should be here")))
        unvettedPackages(archive.all) shouldBe empty

        assertPackageUsable(participant3, participant3, daId)
      }
    }

    "cannot submit command referring to a package outside of its validity period" taggedAs ledgerIntegrity
      .setAttack(exploitUnknownPackage("the submitting participant")) in { implicit env =>
      import env.*

      // vet packages with a validity end date in the past
      vettingCmd(
        adds = archive.all,
        validUntil = Some(environment.clock.now.minusSeconds(3600)),
      )
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        submitCommand(participant3, participant3, daId),
        assertSynchronizerDiscardedPackageNotVettedByReason(
          synchronizerId = daId,
          partyId = participant3.adminParty,
          packageNameWithoutVettedPackages = Many.PACKAGE_NAME,
          expectedErrorCode = InvalidPrescribedSynchronizerId,
          expectedPreambleErrorMessage =
            show"Cannot submit transaction to prescribed synchronizer `${daId.logical}`",
        ),
      )

      // vet packages with a validity start date in the future
      vettingCmd(
        adds = archive.all,
        validFrom = Some(environment.clock.now.plusSeconds(3600)),
      )
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        submitCommand(participant3, participant3, daId),
        assertSynchronizerDiscardedPackageNotVettedByReason(
          synchronizerId = daId,
          partyId = participant3.adminParty,
          packageNameWithoutVettedPackages = Many.PACKAGE_NAME,
          expectedErrorCode = InvalidPrescribedSynchronizerId,
          expectedPreambleErrorMessage =
            show"Cannot submit transaction to prescribed synchronizer `${daId.logical}`",
        ),
      )

    }

    "the usage of a package falls within the package validity period" must {
      "allow us to use the package" taggedAs ledgerIntegrity.setHappyCase(
        "use a package, if the usage falls within the package validity period"
      ) in { implicit env =>
        import env.*

        // vet packages with validity +/- 1 hour
        vettingCmd(
          adds = archive.all,
          validFrom = Some(environment.clock.now.minusSeconds(3600)),
          validUntil = Some(environment.clock.now.plusSeconds(3600)),
        )

        assertPackageUsable(participant3, participant3, daId)
      }
    }

    "package id is used by an active contract" must {
      "allow to unvet a package with active contracts" in { implicit env =>
        import env.*
        assertPackageUsable(participant3, participant3, daId)
        vettingCmd(removes = Seq(archive.main))

        // vet the package again so that we can archive the contract again for cleanup
        vettingCmd(adds = archive.all)
        archiveContract(participant3)
      }

    }

    "a package is upgrade-incompatible" must {
      val incompatArchive = tryReadDar(UpgradeTestsIncompatPath)
      val compatArchive = tryReadDar(UpgradeTestsCompatPath)

      "refuse to validate the upgrade-incompatible DAR" in { implicit env =>
        import env.*
        participant3.dars.upload(UpgradeTestsPath, vetAllPackages = true, synchronizerId = daId)
        assertThrowsAndLogsCommandFailures(
          participant3.dars.validate(UpgradeTestsIncompatPath),
          _.shouldBeCantonErrorCode(ParticipantTopologyManagerError.Upgradeability),
        )
      }

      "refuse to vet the upgrade-incompatible package" in { implicit env =>
        import env.*
        // it can upload the upgrade-incompatible package without vetting
        participant3.dars.upload(UpgradeTestsIncompatPath, vetAllPackages = false)

        assertThrowsAndLogsCommandFailures(
          vettingCmd(adds = incompatArchive.all),
          _.shouldBeCantonErrorCode(ParticipantTopologyManagerError.Upgradeability),
        )
      }

      "allow to vet the package with the force flag" in { implicit env =>
        vettingCmd(
          adds = incompatArchive.all,
          force = ForceFlags(ForceFlag.AllowVetIncompatibleUpgrades),
        )
      }

      "fail to vet any other package in the same lineage" in { implicit env =>
        import env.*
        participant3.dars.upload(UpgradeTestsCompatPath, vetAllPackages = false)
        assertThrowsAndLogsCommandFailures(
          vettingCmd(adds = compatArchive.all),
          _.shouldBeCantonErrorCode(ParticipantTopologyManagerError.Upgradeability),
        )
      }

      "allow to vet upgrade-compat package after unvetting the upgrade-incompat package" in {
        implicit env =>
          vettingCmd(removes = Seq(incompatArchive.main))
          vettingCmd(adds = compatArchive.all)
      }

      "allow to vet upgrade package that uses an upgrade-compatible dependency" in { implicit env =>
        import env.*
        val vettingMainCompatDar = tryReadDar(VettingMainCompatPath)

        // upload and vet VettingMain and VettingDep
        participant3.dars.upload(VettingMainPath, synchronizerId = daId)
        participant3.dars.upload(VettingDepCompatPath, synchronizerId = daId)

        // upload VettingMainCompat without vetting
        participant3.dars.upload(VettingMainCompatPath, vetAllPackages = false)

        // vetting VettingMainCompat should succeed
        vettingCmd(adds = Seq(vettingMainCompatDar.main))
      }

      "fail to vet upgrade package that uses an upgrade-incompatible dependency" in {
        implicit env =>
          import env.*
          val vettingDepIncompatDar = tryReadDar(VettingDepIncompatPath)
          val vettingMainIncompatDar = tryReadDar(VettingMainIncompatPath)

          // upload VettingDepIncompat and force vetting
          participant3.dars.upload(VettingDepIncompatPath, vetAllPackages = false)
          vettingCmd(
            adds = Seq(vettingDepIncompatDar.main),
            force = ForceFlags(ForceFlag.AllowVetIncompatibleUpgrades),
          )

          // upload VettingMainIncompat without vetting
          participant3.dars.upload(VettingMainIncompatPath, vetAllPackages = false)

          // vetting VettingMainIncompat should fail
          assertThrowsAndLogsCommandFailures(
            vettingCmd(adds = Seq(vettingMainIncompatDar.main)),
            _.shouldBeCantonErrorCode(ParticipantTopologyManagerError.Upgradeability),
          )

          // unvet VettingDepIncompat
          vettingCmd(removes = Seq(vettingDepIncompatDar.main))
      }

      "fail to vet upgrade package that substitutes one of its dependency with another" in {
        implicit env =>
          import env.*
          val vettingDepSubstitutionDar = tryReadDar(VettingDepSubstitutionPath)
          val vettingMainSubstitutionDar = tryReadDar(VettingMainSubstitutionPath)

          // upload VettingDepSubstitution and force vetting
          participant3.dars.upload(VettingDepSubstitutionPath, vetAllPackages = false)
          vettingCmd(
            adds = Seq(vettingDepSubstitutionDar.main),
            force = ForceFlags(ForceFlag.AllowVetIncompatibleUpgrades),
          )

          // upload VettingMainSubstitution without vetting
          participant3.dars.upload(VettingMainSubstitutionPath, vetAllPackages = false)

          // vetting VettingMainSubstitution should fail
          assertThrowsAndLogsCommandFailures(
            vettingCmd(adds = Seq(vettingMainSubstitutionDar.main)),
            _.shouldBeCantonErrorCode(ParticipantTopologyManagerError.Upgradeability),
          )
      }
    }

    "a package is vetted" must {
      "allow to unvet without any force flag" in { implicit env =>
        import env.*
        // first vet packages again.
        vettingCmd(adds = archive.all)

        // unvet the package
        vettingCmd(removes = Seq(archive.main))
        vettingCmd(adds = archive.all)

        // We don't need the force flag to disable a dar.
        participant3.dars.vetting.disable(
          darMainPackageId
            .get()
            .getOrElse(fail("DAR main package-id should have been set")),
          synchronizerId = synchronizer1Id,
        )
        participant3.packages.synchronize_vetting()
        eventually() {
          unvettedPackages(List(archive.main)) should not be empty
        }

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          submitCommand(participant3, participant3, daId),
          assertSynchronizerDiscardedPackageNotVettedByReason(
            synchronizerId = daId,
            partyId = participant3.adminParty,
            packageNameWithoutVettedPackages = Many.PACKAGE_NAME,
            expectedErrorCode = InvalidPrescribedSynchronizerId,
            expectedPreambleErrorMessage =
              show"Cannot submit transaction to prescribed synchronizer `${daId.logical}`",
          ),
        )
      }

      val vettingDepDar = tryReadDar(VettingDepPath)
      val vettingMainDar = tryReadDar(VettingMainPath)

      "refuse to unvet if the package is used as a dependency" in { implicit env =>
        import env.*

        // upload and vet the main dar and its dependencies
        participant3.dars.upload(VettingMainPath, vetAllPackages = true)

        // unvetting the dep package should fail
        assertThrowsAndLogsCommandFailures(
          vettingCmd(removes = Seq(vettingDepDar.main)),
          _.shouldBeCantonErrorCode(ParticipantTopologyManagerError.DependenciesNotVetted),
        )
      }

      "allow to unvet if the package is used as a dependency and AllowUnvettedDependencies is used" in {
        implicit env =>
          vettingCmd(
            removes = Seq(vettingDepDar.main),
            force = ForceFlags(ForceFlag.AllowUnvettedDependencies),
          )
      }

      "refuse to unvet while vetting a dependent package" in { implicit env =>
        // vet the dep package and unvet the main package
        vettingCmd(
          adds = Seq(vettingDepDar.main),
          removes = Seq(vettingMainDar.main),
        )

        assertThrowsAndLogsCommandFailures(
          vettingCmd(
            adds = Seq(vettingMainDar.main),
            removes = Seq(vettingDepDar.main),
          ),
          _.shouldBeCantonErrorCode(ParticipantTopologyManagerError.DependenciesNotVetted),
        )
      }
    }

    "Refuse to issue package vetting command" must {
      "for Alien member" taggedAs_ { _ =>
        ledgerIntegrity.setAttack(
          Attack(
            actor = "sequencer operator",
            threat = "issue a package vetting without force",
            mitigation = "reject the command",
          )
        )
      } in { implicit env =>
        import env.*

        loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
          sequencer1.topology.vetted_packages.propose_delta(
            participant3.id,
            store = synchronizer1Id,
            adds = VettedPackage.unbounded(archive.all.map(DamlPackageStore.readPackageId)),
            // explicitly specifying the desired signing key and force flag to not trigger
            // the error NO_APPROPRIATE_SINGING_KEY_IN_STORE while automatically determining
            // a suitable signing key
            signedBy = Some(sequencer1.id.fingerprint),
            force = ForceFlags(ForceFlag.AllowUnvalidatedSigningKeys),
          ),
          forAll(_)(
            _.shouldBeCantonErrorCode(
              TopologyManagerError.DangerousCommandRequiresForce
            )
          ),
        )

        // generate a new signing key and register a namespace delegation
        val sequencer1SigningKey = sequencer1.keys.secret.generate_signing_key(
          "signing_keys_p3",
          SigningKeyUsage.NamespaceOnly,
        )

        // propose the namespace delegations
        participant3.topology.namespace_delegations.propose_delegation(
          participant3.namespace,
          sequencer1SigningKey,
          CanSignSpecificMappings(VettedPackages),
          store = synchronizer1Id,
        )

        eventually() {
          sequencer1.topology.namespace_delegations.list(
            synchronizer1Id,
            filterNamespace = participant3.namespace.filterString,
            filterTargetKey = Some(
              sequencer1SigningKey.fingerprint
            ),
          ) should not be empty
        }

        sequencer1.topology.vetted_packages.propose_delta(
          participant3.id,
          adds = VettedPackage.unbounded(archive.all.map(DamlPackageStore.readPackageId)),
          store = synchronizer1Id,
          signedBy = Some(sequencer1SigningKey.fingerprint),
          force = ForceFlags(ForceFlag.AlienMember),
        )

        // Only exit test once package vetting is observable to prevent flakes such as #24162.
        eventually() {
          unvettedPackages(archive.all) shouldBe empty
        }
      }
    }

    "the main package is unvetted and vetted again" must {
      "allow us to use the package" taggedAs ledgerIntegrity.setHappyCase(
        "submit a command referring to a package that has been vetted again"
      ) in { implicit env =>
        import env.*

        // unvet main package so that we can test that we can vet and subsequently submit a command
        vettingCmd(removes = Seq(archive.main))
        eventually() {
          unvettedPackages(archive.all) shouldBe Set(archive.main.getHash)
        }

        // and vet again
        participant3.dars.vetting.enable(darMainPackageId.get().getOrElse(fail("Should be here")))
        participant3.packages.synchronize_vetting()
        eventually() {
          unvettedPackages(archive.all) shouldBe empty
        }
        assertPackageUsable(participant3, participant3, daId)
      }
    }
  }

  private def tryReadDar(darPath: String) =
    DarParser
      .readArchiveFromFile(new java.io.File(darPath))
      .getOrElse(fail(s"cannot read DAR: $darPath"))
}

class PackageVettingIntegrationTestInMemory extends PackageVettingIntegrationTest {
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
