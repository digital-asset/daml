// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.pkgdars

import com.daml.ledger.javaapi.data.Command
import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.{Availability, Integrity}
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.console.ConsoleEnvironment.Implicits.*
import com.digitalasset.canton.console.{
  LocalParticipantReference,
  ParticipantReference,
  SequencerReference,
}
import com.digitalasset.canton.damltests.java.conflicttest.Many
import com.digitalasset.canton.damltests.java.iou
import com.digitalasset.canton.damltests.java.iou.Amount
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.pkgdars.PackageUsableMixin
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.{
  CannotRemoveAdminWorkflowPackage,
  CannotRemoveOnlyDarForPackage,
  MainPackageInUse,
  PackageInUse,
  PackageVetted,
}
import com.digitalasset.canton.participant.admin.PackageService.{DarDescription, DarMainPackageId}
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal.ping.Ping
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.performance.model.java.dvp.asset.Asset
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.archive.DarParser
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.value.Value.ContractId
import org.scalatest.*

import java.io.File
import scala.jdk.CollectionConverters.*

trait PackageRemovalIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with PackageUsableMixin
    with SecurityTestSuite
    with AccessTestScenario
    with LoneElement {

  val ledgerIntegrity: SecurityTest =
    SecurityTest(property = Integrity, asset = "virtual shared ledger")

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1

  val inMemory: Boolean

  private val pkg = PackageId.assertFromString(Many.PACKAGE_ID)
  private val adminWorkflowPkg = PackageId.assertFromString(Ping.PACKAGE_ID)
  private val performanceTestPkg = PackageId.assertFromString(Asset.PACKAGE_ID)

  "A participant operator" can {

    def vettedPackagesDA(participant: LocalParticipantReference)(implicit
        env: TestConsoleEnvironment
    ) =
      participant.topology.vetted_packages
        .list(
          store = env.daId,
          filterParticipant = participant.id.filterString,
        )
        .flatMap(_.item.packages)
        .map(_.packageId)
        .toSet

    def mainPkgUnvetted()(implicit env: TestConsoleEnvironment): Unit = {
      import env.*
      eventually() {
        val pkgs = vettedPackagesDA(participant1)
        pkgs should not contain pkg
      }
      // Make sure we have a clean state
      participant1.packages.synchronize_vetting()
    }

    "remove dars" should {

      "prevent removal of the admin workflow DAR" in { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        val dars = participant1.dars.list()
        dars should have length 1
        val adminWorkflowDar = dars.headOption.value

        assertThrowsAndLogsCommandFailures(
          participant1.dars.remove(adminWorkflowDar.mainPackageId),
          entry =>
            entry.shouldBeCommandFailure(
              com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.code,
              new CannotRemoveAdminWorkflowPackage(adminWorkflowPkg).cause,
            ),
        )
      }

      "succeed with the basic case" in { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)

        val initialVetting = vettedPackagesDA(participant1)

        def checkVettingState(vetted: Boolean) = {
          val vettedPkgs = vettedPackagesDA(participant1) -- initialVetting
          if (vetted) {
            vettedPkgs should contain(pkg)
          } else {
            vettedPkgs should have size 0
          }
        }

        mainPkgUnvetted()

        val mainPackageId = participant1.dars.upload(CantonTestsPath)

        eventually() {
          checkVettingState(true)
        }

        val dars1 = participant1.dars.list().map(_.mainPackageId).toList
        dars1 should contain(mainPackageId)

        participant1.dars.remove(mainPackageId)

        val dars2 = participant1.dars.list().map(_.mainPackageId).toList
        dars2 should (not(contain(mainPackageId)))

        val pkgs = participant1.packages.list().map(_.packageId)
        pkgs should (not(contain(pkg)))

        eventually() {
          checkVettingState(false)
        }

      }

      "check the main package of the dar is unused" in { implicit env =>
        import env.*

        mainPkgUnvetted()

        val (darHex, darDescriptor) = setUp(participant1, sequencer1, daName, daId)

        val cid = activeContractForPackage(participant1, daName, pkg)

        assertThrowsAndLogsCommandFailures(
          participant1.dars.remove(darHex),
          entry =>
            entry.shouldBeCommandFailure(
              com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.code,
              new MainPackageInUse(pkg, darDescriptor, cid, daId).cause,
            ),
        )

        archiveContract(participant1, cid)()

        participant1.dars.remove(darHex)

        logger.info(s"DAR should be removed")
        val dars2 = participant1.dars.list().map(_.mainPackageId).toList
        dars2 should (not(contain(darHex)))

        logger.info(s"Main package should be removed")
        val pkgs = participant1.packages.list().map(_.packageId)
        pkgs should (not(contain(pkg)))
      }

      "check the other packages of the DAR can be found elsewhere" in { implicit env =>
        import env.*

        mainPkgUnvetted()

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        // Check the connection works
        participant1.health.ping(participant1)

        val darHash = participant1.dars.upload(CantonTestsPath)
        lazy val darDescriptor = descriptorForHexHash(participant1, darHash)

        val cmd = new iou.Iou(
          participant1.adminParty.toProtoPrimitive,
          participant1.adminParty.toProtoPrimitive,
          new Amount(1.toBigDecimal, "USD"),
          List.empty.asJava,
        ).create.commands.asScala.toSeq

        participant1.ledger_api.javaapi.commands.submit(Seq(participant1.adminParty), cmd)

        val iouPkg = PackageId.assertFromString(iou.Iou.PACKAGE_ID)

        assertThrowsAndLogsCommandFailures(
          participant1.dars.remove(darHash),
          entry =>
            entry.shouldBeCommandFailure(
              com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.code,
              new CannotRemoveOnlyDarForPackage(iouPkg, darDescriptor).cause,
            ),
        )

        participant1.dars.upload(CantonExamplesPath)

        participant1.dars.remove(darHash)

        val dars2 = participant1.dars.list().map(_.mainPackageId).toList
        dars2 should (not(contain(darHash)))

        val pkgs = participant1.packages.list().map(_.packageId)
        pkgs should (not(contain(pkg)))

      }
    }

    "remove packages only once unused and unvetted" taggedAs ledgerIntegrity.setAttack(
      Attack(
        actor = "a Canton user",
        threat = "create a ledger fork by removing packages",
        mitigation = "prevent removal of vetted packages",
      )
    ) in { implicit env =>
      import env.*

      mainPkgUnvetted()

      setUp(participant1, sequencer1, daName, daId)

      checkPackageOnlyRemovedWhenUnusedUnvetted(participant1, pkg, daId)()
    }

    "prevent removal of the ping package" in { implicit env =>
      import env.*
      participant1.synchronizers.connect_local(sequencer1, alias = daName)

      assertThrowsAndLogsCommandFailures(
        participant1.packages.remove(adminWorkflowPkg),
        entry =>
          entry.shouldBeCommandFailure(
            com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.code,
            new CannotRemoveAdminWorkflowPackage(adminWorkflowPkg).cause,
          ),
      )
    }

    "remove packages only once unused on disconnected synchronizers" taggedAs ledgerIntegrity
      .setAttack(
        Attack(
          actor = "a Canton user",
          threat = "create a ledger fork by removing packages",
          mitigation = "prevent removal of packages used on disconnected synchronizers",
        )
      ) in { implicit env =>
      import env.*
      val darPath: String = CantonTestsPath

      setUp(participant1, sequencer1, daName, daId)

      val cid = activeContractForPackage(participant1, daName, pkg)

      logger.info(s"Cannot remove package that is used and vetted")
      packageRemovalFails(participant1, pkg, new PackageInUse(pkg, cid, daId).cause)

      participant1.synchronizers.disconnect_local(daName)

      logger.info(
        s"Cannot remove package that is used and vetted, when disconnected from synchronizer"
      )
      packageRemovalFails(participant1, pkg, new PackageInUse(pkg, cid, daId).cause)

      participant1.packages.list().map(_.packageId) should (contain(pkg))

      participant1.stop()
      participant1.start()

      if (inMemory) {
        participant1.packages.list().map(_.packageId) should not(contain(pkg))
      } else {

        logger.info(
          s"Cannot remove package that is used and vetted, after a restart, when disconnected"
        )
        packageRemovalFails(participant1, pkg, new PackageInUse(pkg, cid, daId).cause)

        participant1.synchronizers.connect_local(sequencer1, alias = daName)

        logger.info(
          s"Cannot remove package that is used and vetted, after a restart, when reconnected"
        )
        packageRemovalFails(participant1, pkg, new PackageInUse(pkg, cid, daId).cause)

        archiveContract(participant1, cid)()

        logger.info(s"Cannot remove package that is vetted")
        packageRemovalFails(participant1, pkg, new PackageVetted(pkg).cause)

        removeVetting(participant1, daId, darPath)

        logger.info(s"Can remove package that is unused and unvetted")
        participant1.packages.remove(pkg)
      }
    }

    "remove packages only once unvetted on disconnected synchronizers" taggedAs ledgerIntegrity
      .setAttack(
        Attack(
          actor = "a Canton user",
          threat = "create a ledger fork by removing packages",
          mitigation = "prevent removal of packages vetted on disconnected synchronizers",
        )
      ) in { implicit env =>
      import env.*
      val darPath: String = CantonTestsPath

      participant1.synchronizers.connect_local(sequencer1, alias = daName)

      participant1.dars.upload(CantonTestsPath)

      logger.info(s"Cannot remove package that is vetted")
      packageRemovalFails(participant1, pkg, new PackageVetted(pkg).cause)

      participant1.synchronizers.disconnect_local(daName)

      logger.info(s"Cannot remove package that is vetted, when disconnected from synchronizer")
      packageRemovalFails(participant1, pkg, new PackageVetted(pkg).cause)

      participant1.packages.list().map(_.packageId) should (contain(pkg))

      participant1.stop()
      participant1.start()

      if (inMemory) {
        participant1.packages.list().map(_.packageId) should not(contain(pkg))
      } else {

        logger.info(
          s"Cannot remove package that is vetted, after a restart, when disconnected"
        )
        packageRemovalFails(participant1, pkg, new PackageVetted(pkg).cause)

        participant1.synchronizers.connect_local(sequencer1, alias = daName)

        logger.info(
          s"Cannot remove package that is vetted, after a restart, when reconnected"
        )
        packageRemovalFails(participant1, pkg, new PackageVetted(pkg).cause)

        removeVetting(participant1, daId, darPath)

        logger.info(s"Can remove package that is unused and unvetted")
        participant1.packages.remove(pkg)
      }
    }

    "remove a package immediately with the force flag" taggedAs_ {
      ledgerIntegrity.setHappyCase(_)
    } in { implicit env =>
      import env.*

      setUp(participant1, sequencer1, daName, daId)

      participant1.packages.list().map(_.packageId) should contain(pkg)

      val cid = activeContractForPackage(participant1, daName, pkg)
      packageRemovalFails(participant1, pkg, new PackageInUse(pkg, cid, daId).cause)

      participant1.packages.remove(pkg, force = true)

      participant1.packages.list().map(_.packageId) should not contain (pkg)

      // Clean up for the next test
      archiveContract(participant1, cid)()

    }

    "remove packages that do not exist" taggedAs_ {
      ledgerIntegrity.setHappyCase(_)
    } in { implicit env =>
      import env.*
      setUp(participant1, sequencer1, daName, daId)

      participant1.packages.remove("this-is-not-a-real-package-id")

      // Clean up
      val cid = activeContractForPackage(participant1, daName, pkg)
      archiveContract(participant1, cid)()
    }

    "disable a package twice" taggedAs_ {
      ledgerIntegrity.setHappyCase(_)
    } in { implicit env =>
      import env.*
      setUp(participant1, sequencer1, daName, daId)

      checkPackageOnlyRemovedWhenUnusedUnvetted(participant1, pkg, daId)()
      logger.info(s"Remove the package a second time")
      participant1.packages.remove(pkg)
    }

    "not crash a participant by passing an invalid package id" taggedAs SecurityTest(
      property = Availability,
      asset = "participant node",
      attack = Attack(
        actor = "a participant operator",
        threat = "crash the participant by passing an invalid package id",
        mitigation = "reject the request",
      ),
    ) in { implicit env =>
      import env.*
      setUp(participant1, sequencer1, daName, daId)

      assertThrowsAndLogsCommandFailures(
        participant1.packages.remove("invalid-package-id-\uD83C\uDF3C\uD83C\uDF38❀✿\uD83C\uDF37"),
        entry => entry.commandFailureMessage should include("INVALID_ARGUMENT"),
      )

      // Clean up
      val cid = activeContractForPackage(participant1, daName, pkg)
      archiveContract(participant1, cid)()
    }

    "remove only specific packages" taggedAs_ {
      ledgerIntegrity.setHappyCase(_)
    } in { implicit env =>
      import env.*

      setUp(participant1, sequencer1, daName, daId)
      uploadDarAndVerifyVettingOnSynchronizer(
        participant1,
        PerformanceTestPath,
        performanceTestPkg,
        daId,
      )

      val adminParty = participant1.id.adminParty

      val cmd = new Asset(
        adminParty.toProtoPrimitive,
        adminParty.toProtoPrimitive,
        "PackageRemovalTestAsset",
        "",
        adminParty.toProtoPrimitive,
        adminParty.toProtoPrimitive,
      ).create.commands.asScala.toSeq

      participant1.ledger_api.javaapi.commands.submit(
        Seq(adminParty),
        cmd,
        Some(daId),
      )

      checkPackageOnlyRemovedWhenUnusedUnvetted(participant1, pkg, daId)()

      checkPackageOnlyRemovedWhenUnusedUnvetted(
        participant1,
        performanceTestPkg,
        daId,
        darPath = PerformanceTestPath,
      )(cid =>
        new Asset.ContractId(cid)
          .exerciseArchive()
          .commands
          .loneElement
      )
    }

    "remove packages only once unused on all synchronizers" taggedAs SecurityTest(
      property = Integrity,
      asset = "participant node",
      attack = Attack(
        actor = "participant operator",
        threat =
          "corrupt the participant state by removing a package referred by an active contract",
        mitigation = "reject the request for removing the package",
      ),
    ) in { implicit env =>
      import env.*
      setUp(participant1, sequencer1, daName, daId)
      setUp(participant1, sequencer2, acmeName, acmeId)

      logger.info(s"Cannot remove package that is used and vetted")

      val cidDa = activeContractForPackage(participant1, daName, pkg)
      val cidAcme = activeContractForPackage(participant1, acmeName, pkg)

      packageRemovalFails(participant1, pkg, new PackageInUse(pkg, cidDa, daId).cause)

      archiveContract(participant1, cidDa)()

      packageRemovalFails(participant1, pkg, new PackageInUse(pkg, cidAcme, acmeId).cause)

      archiveContract(participant1, cidAcme)()

      packageRemovalFails(participant1, pkg, new PackageVetted(pkg).cause)

      removeVetting(participant1, daId)
      removeVetting(participant1, acmeId)

      logger.info(s"Can remove package that is unused and unvetted")
      participant1.packages.remove(pkg)

    }

    "remove packages for contracts that have been reassigned" taggedAs_ {
      ledgerIntegrity.setHappyCase(_)
    } in { implicit env =>
      import env.*

      setUp(participant1, sequencer1, daName, daId)
      participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
      uploadDarAndVerifyVettingOnSynchronizer(
        participant1,
        PerformanceTestPath,
        performanceTestPkg,
        daId,
        acmeId,
      )

      // Ensure that the latest time proof known to the reassigning participant
      // is more recent than the vetting, since the vetting status is checked at
      // the time proof, rather than at the latest known status
      // TODO(i13200) The following line can be removed once the ticket is closed
      participant1.testing.fetch_synchronizer_time(acmeId)

      val activeContractsForPkg = participant1.testing.acs_search(
        daName,
        filterPackage = pkg,
        exactId = "",
        filterTemplate = "",
      )

      val manyId = activeContractsForPkg.headOption.value.contractId

      val pkg2 = PackageId.assertFromString(Asset.PACKAGE_ID)
      val adminParty = participant1.id.adminParty

      val cmd = new Asset(
        adminParty.toProtoPrimitive,
        adminParty.toProtoPrimitive,
        "PackageRemovalTestAsset",
        "",
        adminParty.toProtoPrimitive,
        adminParty.toProtoPrimitive,
      ).create.commands.asScala.toSeq

      val asset = JavaDecodeUtil
        .decodeAllCreated(Asset.COMPANION)(
          participant1.ledger_api.javaapi.commands.submit(
            Seq(adminParty),
            cmd,
            Some(daId),
          )
        )
        .headOption
        .value

      val assetId = asset.id.toLf

      packageRemovalFails(participant1, pkg, new PackageInUse(pkg, manyId, daId).cause)
      packageRemovalFails(participant1, pkg2, new PackageInUse(pkg2, assetId, daId).cause)

      participant1.ledger_api.commands.submit_reassign(
        participant1.adminParty,
        Seq(assetId),
        source = synchronizer1Id,
        target = synchronizer2Id,
      )

      packageRemovalFails(participant1, pkg, new PackageInUse(pkg, manyId, daId).cause)
      packageRemovalFails(participant1, pkg2, new PackageInUse(pkg2, assetId, acmeId).cause)

      val archiveCmd =
        asset.id.exerciseArchive().commands.loneElement

      participant1.ledger_api.javaapi.commands
        .submit(actAs = Seq(participant1.adminParty), commands = Seq(archiveCmd))

      packageRemovalFails(participant1, pkg, new PackageInUse(pkg, manyId, daId).cause)
      packageRemovalFails(participant1, pkg2, new PackageVetted(pkg2).cause)

      removeVetting(participant1, daId, darPath = PerformanceTestPath)
      removeVetting(participant1, acmeId, darPath = PerformanceTestPath)

      participant1.packages.remove(pkg2)

      packageRemovalFails(participant1, pkg, new PackageInUse(pkg, manyId, daId).cause)

      archiveContract(participant1, manyId)()

      packageRemovalFails(participant1, pkg, new PackageVetted(pkg).cause)

      removeVetting(participant1, daId)
      removeVetting(participant1, acmeId)

      participant1.packages.remove(pkg)

    }

  }

  private def checkPackageOnlyRemovedWhenUnusedUnvetted(
      participant: => LocalParticipantReference,
      pkg: PackageId,
      synchronizerId: SynchronizerId,
      darPath: String = CantonTestsPath,
  )(
      mkArchiveCmd: String => Command = cid =>
        new Many.ContractId(cid)
          .exerciseArchive()
          .commands
          .loneElement
  )(implicit env: TestConsoleEnvironment): Unit = {
    logger.info(s"Cannot remove package that is used and vetted")

    val cid = activeContractForPackage(participant, env.daName, pkg)

    packageRemovalFails(participant, pkg, new PackageInUse(pkg, cid, env.daId).cause)

    archiveContract(participant, cid)(mkArchiveCmd)

    logger.info(s"Cannot remove package that is vetted")
    packageRemovalFails(participant, pkg, new PackageVetted(pkg).cause)

    removeVetting(participant, synchronizerId, darPath)

    logger.info(s"Can remove package that is unused and unvetted")
    participant.packages.remove(pkg)
  }

  private def removeVetting(
      participant1: => LocalParticipantReference,
      synchronizerId: SynchronizerId,
      darPath: String = CantonTestsPath,
  ): Unit = {
    val archive = DarParser
      .readArchiveFromFile(new File(darPath))
      .getOrElse(fail("cannot read examples"))

    val pkgs = Seq(DamlPackageStore.readPackageId(archive.main))

    participant1.topology.vetted_packages.propose_delta(
      participant1.id,
      removes = pkgs,
      store = synchronizerId,
    )

    participant1.packages.synchronize_vetting()
  }

  private def activeContractForPackage(
      participant: LocalParticipantReference,
      synchronizerAlias: SynchronizerAlias,
      pkg: String,
  ) = {
    val activeContractsForPkg = participant.testing.acs_search(
      synchronizerAlias,
      filterPackage = pkg,
      exactId = "",
      filterTemplate = "",
    )

    val cid = activeContractsForPkg.headOption.value.contractId

    cid
  }

  private def archiveContract(
      participant1: => LocalParticipantReference,
      cid: ContractId,
  )(
      mkArchiveCmd: String => Command = cid =>
        new Many.ContractId(cid)
          .exerciseArchive()
          .commands
          .loneElement
  ) = {

    val cmd: Command = mkArchiveCmd(cid.coid)
    participant1.ledger_api.javaapi.commands.submit(Seq(participant1.id.adminParty), Seq(cmd))
  }

  private def packageRemovalFails(
      participant1: => LocalParticipantReference,
      pkg: String,
      message: String,
  ) =
    assertThrowsAndLogsCommandFailures(
      participant1.packages.remove(pkg),
      entry =>
        entry.shouldBeCommandFailure(
          com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.code,
          message,
        ),
    )

  private def setUp(
      participant: => LocalParticipantReference,
      sequencerConnection: SequencerReference,
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: SynchronizerId,
  ): (String, DarDescription) = {
    clue("connecting to synchronizer") {
      participant.synchronizers.connect_local(sequencerConnection, alias = synchronizerAlias)
    }
    val darHex =
      uploadDarAndVerifyVettingOnSynchronizer(participant, CantonTestsPath, pkg, synchronizerId)

    assertPackageUsable(participant, participant, synchronizerId)
    val darDescriptor = descriptorForHexHash(participant, darHex)
    darHex -> darDescriptor

  }

  private def uploadDarAndVerifyVettingOnSynchronizer(
      participant: LocalParticipantReference,
      darPath: String,
      pkg: PackageId,
      synchronizerIds: SynchronizerId*
  ): String =
    synchronizerIds
      .map { synchronizerId =>
        val darHex = clue(s"uploading and vetting tests on $synchronizerId") {
          participant.dars.upload(darPath, synchronizerId = synchronizerId)
        }
        eventually() {
          participant.topology.vetted_packages
            .list(
              store = synchronizerId,
              filterParticipant = participant.id.filterString,
            )
            .loneElement
            .item
            .packages
            .map(_.packageId) contains pkg
        }
        darHex
      }
      .headOption
      .value

  def descriptorForHexHash(
      participant: ParticipantReference,
      hexString: String,
  ): DarDescription = {
    val dd =
      participant.dars.list().find(_.mainPackageId == hexString).valueOrFail("Must be there")
    DarDescription(
      mainPackageId = DarMainPackageId.tryCreate(dd.mainPackageId),
      description = String255.tryCreate(dd.description),
      name = String255.tryCreate(dd.name),
      version = String255.tryCreate(dd.version),
    )
  }

}

class PackageRemovalIntegrationTestPostgres extends PackageRemovalIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  override val inMemory: Boolean = false
}
