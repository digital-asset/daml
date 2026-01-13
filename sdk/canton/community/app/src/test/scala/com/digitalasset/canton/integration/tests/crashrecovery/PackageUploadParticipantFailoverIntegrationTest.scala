// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.crashrecovery

import better.files.File
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.console.{ParticipantReference, RemoteParticipantReference}
import com.digitalasset.canton.damltests.java.simpletemplate
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.platform.apiserver.services.admin.PackageTestUtils
import com.digitalasset.canton.platform.apiserver.services.admin.PackageTestUtils.ArchiveOps
import com.digitalasset.canton.topology.TopologyManagerError.ParticipantTopologyManagerError
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.topology.{ForceFlags, SynchronizerId, TopologyManagerError}
import com.digitalasset.daml.lf.archive.{DamlLf, DarParser}
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.google.protobuf.ByteString

import java.io.File as JavaIOFile
import scala.jdk.CollectionConverters.*

trait PackageUploadParticipantFailoverIntegrationTest
    extends ParticipantFailoverIntegrationTestBase {
  private val basePkg = createTestArchive(1)("someParty: Party")
  private val upgradeIncompatiblePkg =
    createTestArchive(2)("someText: Text")
  private val upgradeCompatiblePkg =
    createTestArchive(3)("someParty: Party, someOptionalText: Option Text")
  private val upgradeCompatiblePkg2 = createTestArchive(4)(
    "someParty: Party, someOptionalText: Option Text, someOptionalParty: Option Party"
  )

  val simpleTemplateDarPath = BaseTest.CantonTestsPath
  val simpleTemplateDar: DamlLf.Archive = DarParser
    .readArchiveFromFile(new JavaIOFile(simpleTemplateDarPath))
    .toOption
    .valueOrFail("where is CantonTestsPath?")
    .main

  def swap(p1: ParticipantReference, p2: ParticipantReference): Unit = {
    p1.replication.set_passive()
    waitActive(p2)
  }

  def vettingCmd(
      participant: ParticipantReference,
      synchronizerId: SynchronizerId,
      adds: Seq[DamlLf.Archive] = Seq.empty,
  ) = {
    participant.topology.vetted_packages.propose_delta(
      participant.id,
      store = synchronizerId,
      adds = adds.map(DamlPackageStore.readPackageId).map(VettedPackage(_, None, None)),
      removes = Seq(),
      force = ForceFlags.none,
    )
    // synchronize package vetting, as "raw" vetting commands are unsynced
    participant.packages.synchronize_vetting()
  }

  "Package uploading" when {
    "participants fail-over" should {
      "correctly validate upgradable packages" in { implicit env =>
        import env.*

        val p1 = rp("participant1")
        val p2 = rp("participant2")

        val (firstActiveParticipant, secondActiveParticipant) =
          if (isActive(p1)) (p1, p2) else (p2, p1)

        eventually(timeout) {
          assert(isHealthy(firstActiveParticipant.name))
          assert(!isHealthy(secondActiveParticipant.name))
        }
        p1.synchronizers.connect_local(sequencer1, daName)

        // Upload the first DAR to the active participant
        uploadDarTo(basePkg, firstActiveParticipant)

        // Kill the active participant instance, then the passive one must become healthy
        externalPlugin.kill(firstActiveParticipant.name)
        eventually(timeout) {
          assert(isHealthy(secondActiveParticipant.name))
        }

        secondActiveParticipant.synchronizers.reconnect_all()

        // Uploading and vetting the second DAR that's upgrade-incompatible with the first one
        // must fail on the second participant
        assertThrowsAndLogsCommandFailures(
          uploadDarTo(upgradeIncompatiblePkg, secondActiveParticipant),
          _.shouldBeCommandFailure(
            TopologyManagerError.ParticipantTopologyManagerError.Upgradeability
          ),
        )

        // Uploading an upgrade compatible DAR to the active participant should be possible
        uploadDarTo(upgradeCompatiblePkg, secondActiveParticipant)

        // Transition the first participant back to active
        externalPlugin.start(firstActiveParticipant.name)
        externalPlugin.kill(secondActiveParticipant.name)
        waitActive(firstActiveParticipant)

        firstActiveParticipant.synchronizers.reconnect_all()

        // Uploading and vetting the second DAR that's upgrade-incompatible with the first one
        // must fail on the first participant as well
        assertThrowsAndLogsCommandFailures(
          uploadDarTo(upgradeIncompatiblePkg, firstActiveParticipant),
          _.shouldBeCommandFailure(
            TopologyManagerError.ParticipantTopologyManagerError.Upgradeability
          ),
        )

        // Uploading another upgrade compatible DAR to the active participant should be possible
        uploadDarTo(upgradeCompatiblePkg2, firstActiveParticipant)
      }

      "package service cache is invalidated when switching participants" in { implicit env =>
        import env.*

        val replicatedParticipant1 = rp("participant1")
        val replicatedParticipant2 = rp("participant2")
        val runnerParticipant = lp("participant3")

        val (firstActiveParticipant, secondActiveParticipant) =
          if (isActive(replicatedParticipant1)) (replicatedParticipant1, replicatedParticipant2)
          else (replicatedParticipant2, replicatedParticipant1)

        // Upload to runner participant
        runnerParticipant.health.wait_for_running()
        runnerParticipant.health.wait_for_initialized()
        runnerParticipant.dars.upload(BaseTest.CantonTestsPath, synchronizerId = daId)

        // Try to vet, forcing package to be cached
        assertThrowsAndLogsCommandFailures(
          vettingCmd(firstActiveParticipant, daId, Seq(simpleTemplateDar)),
          _.shouldBeCommandFailure(ParticipantTopologyManagerError.CannotVetDueToMissingPackages),
        )

        swap(firstActiveParticipant, secondActiveParticipant)
        replicatedParticipant2.dars.upload(simpleTemplateDarPath, synchronizerId = daId)
        swap(secondActiveParticipant, firstActiveParticipant)

        // Before fixes introduced in 20904, the CantonSyncService would not
        // refresh the cache for dependencies in the packageService, which meant
        // that a cached "missing package" response would not be cleared when
        // switching to a different replicated participant.
        // This caused a ledger fork and resulting errors, key phrases being:
        // "LOCAL_VERDICT_FAILED_MODEL_CONFORMANCE_CHECK"
        // "Mediator approved a request that we have locally rejected"
        runnerParticipant.ledger_api.javaapi.commands.submit(
          Seq(runnerParticipant.id.adminParty),
          new simpletemplate.SimpleTemplate(
            runnerParticipant.id.adminParty.toProtoPrimitive,
            firstActiveParticipant.id.adminParty.toProtoPrimitive,
          ).create.commands.asScala.toSeq,
        )
      }
    }
  }

  private def uploadDarTo(
      dar: (String, ByteString),
      firstActiveParticipant: RemoteParticipantReference,
  )(implicit env: TestConsoleEnvironment): Unit = {
    val (firstDarName, firstDarPayload) = dar
    val uploadFirstDarPath = File.newTemporaryFile(firstDarName, ".dar")
    logger.debug(s"Writing DAR $firstDarName to ${firstActiveParticipant.name}")
    val file = uploadFirstDarPath.appendByteArray(firstDarPayload.toByteArray)
    firstActiveParticipant.dars.upload(file.pathAsString, synchronizerId = Some(env.daId)).discard
  }

  private def createTestArchive(idx: Int)(discriminatorField: String): (String, ByteString) =
    s"incompatible$idx" -> PackageTestUtils.archiveFromLfDef { implicit parserParameters =>
      p"""
        metadata ( 'incompatibleUpgrade' : '$idx.0.0' )
        module Mod {
          record @serializable T = { actor: Party, $discriminatorField };

          template (this: T) = {
            precondition True;
            signatories Cons @Party [Mod:T {actor} this] (Nil @Party);
            observers Nil @Party;
          };
       }"""
    }().lfArchiveToByteString
}

class PackageUploadParticipantFailoverIntegrationTestPostgres
    extends PackageUploadParticipantFailoverIntegrationTest {
  setupPlugins(new UsePostgres(loggerFactory))
}
