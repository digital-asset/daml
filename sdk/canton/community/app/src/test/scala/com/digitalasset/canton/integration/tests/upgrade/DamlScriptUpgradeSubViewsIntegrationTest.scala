// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade

import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.util.{EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.transaction.{ParticipantPermission, VettedPackage}
import com.digitalasset.canton.topology.{Party, PartyId}
import com.digitalasset.canton.util.SetupPackageVetting
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.value.Value.ContractId

import java.io.File

/*
Creates a transaction of the following shape, then archives #cid2 in a second transaction. Since p1, p2, p3, p4 and p5
are hosted by different participants, this creates many different views. Since #cid2 is disclosed to most parties,
archiving it successfully gives us some confidence that the transaction didn't create a ledger fork.

  │   disclosed to: p1
  └─> p1 exercises MkTransaction on #top with top1 = #top1; top2 = #top2; factoryId1 = #factory1; factoryId2 = #factory2

      │   disclosed to: p1, p2
      └─> p1 exercises I1Choice on #top1 with ctl = p1; factoryId = #factory1

          │   disclosed to: p1, p2, p3
          └─> p1 exercises CreateAsset (v1) on #factory1 with ctl = p1

              │   disclosed to: p1, p2, p3
              │   divulged to: p4
              └─> p3 creates #cid = ScuViewAsset:Asset (v1) with sig = p3


      │   disclosed to: p1, p4
      └─> p1 exercises I2Choice on #top2 with ctl = p1; cid = cid; factoryId = #factory2

          │   disclosed to: p1, p4, p3
          └─> p1 exercises Cycle (v2) on #cid (upgraded to v2) with ctl = p1; factoryId = #factory2

              │   disclosed to: p1, p4, p3, p5
              └─> p3 exercises CreateAsset (v2) on #factory2 with ctl = p3

                  │   disclosed to (since): p1, p4, p3, p5
                  └─> p5 creates #cid2 = ScuViewAsset:Asset (v2) with sig = p5; extra = some "extra"
 */
class DamlScriptUpgradeSubViewsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax {

  private val subViewDarPaths: Set[String] =
    Set(
      SubViewsIfaceV1Path,
      SubViewsAssetV1Path,
      SubViewsAssetV2Path,
      SubViewsMainV1Path,
    )
  private val SubViewsIfaceV1PkgId: LfPackageId = getPkgId(SubViewsIfaceV1Path)
  private val SubViewsAssetV1PkgId: LfPackageId = getPkgId(SubViewsAssetV1Path)
  private val SubViewsAssetV2PkgId: LfPackageId = getPkgId(SubViewsAssetV2Path)
  private val SubViewsMainV1PkgId: LfPackageId = getPkgId(SubViewsMainV1Path)

  private var p1: PartyId = _
  private var p2: PartyId = _
  private var p3: PartyId = _
  private var p4: PartyId = _
  private var p5: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P5_S1M1.withSetup { implicit env =>
      import env.*

      participants.local.foreach { participant =>
        participant.synchronizers.connect_local(sequencer1, alias = daName)
      }

      // Allocate parties
      PartiesAllocator(Set(participant1, participant2, participant3, participant4, participant5))(
        newParties = Seq(
          "p1" -> participant1,
          "p2" -> participant2,
          "p3" -> participant3,
          "p4" -> participant4,
          "p5" -> participant5,
        ),
        targetTopology = Map(
          "p1" -> Map(
            daId -> (PositiveInt.one, Set(participant1.id -> ParticipantPermission.Submission))
          ),
          "p2" -> Map(
            daId -> (PositiveInt.one, Set(participant2.id -> ParticipantPermission.Submission))
          ),
          "p3" -> Map(
            daId -> (PositiveInt.one, Set(
              participant1.id -> ParticipantPermission.Observation,
              participant3.id -> ParticipantPermission.Submission,
            ))
          ),
          "p4" -> Map(
            daId -> (PositiveInt.one, Set(participant4.id -> ParticipantPermission.Submission))
          ),
          "p5" -> Map(
            daId -> (PositiveInt.one, Set(
              participant1.id -> ParticipantPermission.Observation,
              participant5.id -> ParticipantPermission.Submission,
            ))
          ),
        ),
      )
      p1 = "p1".toPartyId(participant1)
      p2 = "p2".toPartyId(participant2)
      p3 = "p3".toPartyId(participant3)
      p4 = "p4".toPartyId(participant4)
      p5 = "p5".toPartyId(participant5)

      // Upload the test DARs and vet their packages
      SetupPackageVetting(
        subViewDarPaths,
        targetTopology = Map(
          daId -> participants.all
            .map(
              _ -> VettedPackage
                .unbounded(
                  Seq(
                    SubViewsIfaceV1PkgId,
                    SubViewsAssetV1PkgId,
                    SubViewsAssetV2PkgId,
                    SubViewsMainV1PkgId,
                  )
                )
                .toSet
            )
            .toMap
        ),
      )
    }

  "A complex command involving upgrades, interfaces and many sub-views doesn't lead to a ledger fork" in {
    implicit env =>
      import env.*

      val top = getContractId(
        participant1.ledger_api.commands
          .submit(Seq(p1), createTop(p1))
      )
      val top1 = getContractId(
        participant1.ledger_api.commands
          .submit(Seq(p1), createTop1(p1, p2))
      )
      val top2 = getContractId(
        participant1.ledger_api.commands
          .submit(Seq(p1), createTop2(p1, p4))
      )
      val factory1 = getContractId(
        participant3.ledger_api.commands
          .submit(Seq(p3), V1.createAssetFactory(p3))
      )
      val factory2 = getContractId(
        participant5.ledger_api.commands
          .submit(Seq(p5), V2.createAssetFactory(p5, Some("extra")))
      )

      val mkTransaction = participant1.ledger_api.commands
        .submit(
          actAs = Seq(p1),
          readAs = Seq(p3, p5),
          commands = exerciseMkTransaction(top1, top2, factory1, factory2, top),
          transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
        )
      val asset = ContractId.assertFromString(
        mkTransaction.events.head.getExercised.getExerciseResult.getContractId
      )
      participant5.ledger_api.commands.submit(Seq(p5), V2.archiveAsset(asset))
  }

  private def getPkgId(darPath: String): LfPackageId =
    DarDecoder.assertReadArchiveFromFile(new File(darPath)).main._1

  private def getContractId(tx: Transaction): ContractId =
    ContractId.assertFromString(tx.events.map(_.getCreated.contractId).loneElement)

  private def createTop(party: Party)(implicit
      env: TestConsoleEnvironment
  ): Seq[Command] = {
    import env.*

    Seq(
      ledger_api_utils.create(
        SubViewsMainV1PkgId,
        "SubViewsMain",
        "Top",
        Map("party" -> party.partyId),
      )
    )
  }

  private def createTop1(sig: Party, obs: Party)(implicit
      env: TestConsoleEnvironment
  ): Seq[Command] = {
    import env.*

    Seq(
      ledger_api_utils.create(
        SubViewsMainV1PkgId,
        "SubViewsMain",
        "Top1",
        Map("sig" -> sig.partyId, "obs" -> obs.partyId),
      )
    )
  }

  private def createTop2(sig: Party, obs: Party)(implicit
      env: TestConsoleEnvironment
  ): Seq[Command] = {
    import env.*

    Seq(
      ledger_api_utils.create(
        SubViewsMainV1PkgId,
        "SubViewsMain",
        "Top2",
        Map("sig" -> sig.partyId, "obs" -> obs.partyId),
      )
    )
  }

  private def exerciseMkTransaction(
      top1: ContractId,
      top2: ContractId,
      factoryId1: ContractId,
      factoryId2: ContractId,
      cid: ContractId,
  )(implicit
      env: TestConsoleEnvironment
  ): Seq[Command] = {
    import env.*

    Seq(
      ledger_api_utils.exercise(
        SubViewsMainV1PkgId,
        "SubViewsMain",
        "Top",
        "MkTransaction",
        Map("top1" -> top1, "top2" -> top2, "factoryId1" -> factoryId1, "factoryId2" -> factoryId2),
        cid.coid,
      )
    )
  }

  private object V1 {
    def createAssetFactory(sig: Party)(implicit
        env: TestConsoleEnvironment
    ): Seq[Command] = {
      import env.*

      Seq(
        ledger_api_utils.create(
          SubViewsAssetV1PkgId,
          "SubViewsAsset",
          "AssetFactory",
          Map("sig" -> sig.partyId),
        )
      )
    }

    def archiveAsset(cid: ContractId)(implicit
        env: TestConsoleEnvironment
    ): Seq[Command] = {
      import env.*

      Seq(
        ledger_api_utils.exercise(
          SubViewsAssetV1PkgId,
          "SubViewsAsset",
          "Asset",
          "Archive",
          Map.empty,
          cid.coid,
        )
      )
    }
  }

  private object V2 {
    @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
    def createAssetFactory(sig: Party, extra: Option[String])(implicit
        env: TestConsoleEnvironment
    ): Seq[Command] = {
      import env.*

      Seq(
        ledger_api_utils.create(
          SubViewsAssetV2PkgId,
          "SubViewsAsset",
          "AssetFactory",
          Map("sig" -> sig.partyId, "extra" -> extra),
        )
      )
    }

    def archiveAsset(cid: ContractId)(implicit
        env: TestConsoleEnvironment
    ): Seq[Command] = {
      import env.*

      Seq(
        ledger_api_utils.exercise(
          SubViewsAssetV2PkgId,
          "SubViewsAsset",
          "Asset",
          "Archive",
          Map.empty,
          cid.coid,
        )
      )
    }
  }
}
