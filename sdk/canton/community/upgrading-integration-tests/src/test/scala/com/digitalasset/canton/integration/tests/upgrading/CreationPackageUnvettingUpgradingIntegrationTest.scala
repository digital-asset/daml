// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.damltests.upgrade.v1.java.upgrade.SigStakeInf as SigStakeInfV1
import com.digitalasset.canton.damltests.upgrade.v2.java.upgrade.{
  SigStakeInf,
  SigStakeInf as SigStakeInfV2,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.util.PartiesAllocator
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.{ParticipantPermission, VettedPackage}
import com.digitalasset.canton.util.SetupPackageVetting
import com.digitalasset.daml.lf.data.Ref

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.chaining.scalaUtilChainingOps

class CreationPackageUnvettingUpgradingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  @volatile private var signatory, nonStakeholder, observer: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.all.foreach(_.synchronizers.connect_local(sequencer1, alias = daName))

        val allocations =
          Map(
            "signatory" -> participant1,
            "nonStakeholder" -> participant2,
            "observer" -> participant3,
          )
        inside(
          PartiesAllocator(allocations.values.toSet)(
            allocations.view.mapValues(_.id).toSeq,
            allocations.view
              .mapValues(participantRef =>
                Map(
                  daId -> (PositiveInt.one, Set(
                    participantRef.id -> (ParticipantPermission.Submission: ParticipantPermission)
                  ))
                )
              )
              .toMap,
          )
        ) { case Seq(signatory, nonStakeholder, observer) =>
          this.signatory = signatory
          this.nonStakeholder = nonStakeholder
          this.observer = observer
        }
      }

  "Command (re-)interpretation" when {
    "the creation package of a disclosed contract is not vetted by the non-stakeholder submitter" should {
      "succeed if the target package is vetted" in { implicit env =>
        import env.*

        SetupPackageVetting(
          Set(UpgradingBaseTest.UpgradeV1, UpgradingBaseTest.UpgradeV2),
          targetTopology = Map(
            synchronizer1Id -> Map[ParticipantReference, Seq[LfPackageId]](
              participant1 -> Seq(
                SigStakeInfV1.PACKAGE_ID.toPackageId,
                SigStakeInfV2.PACKAGE_ID.toPackageId,
              ),
              participant2 -> Seq(SigStakeInfV1.PACKAGE_ID.toPackageId),
              participant3 -> Seq(
                SigStakeInfV1.PACKAGE_ID.toPackageId,
                SigStakeInfV2.PACKAGE_ID.toPackageId,
              ),
            ).view.mapValues(VettedPackage.unbounded(_).toSet).toMap
          ),
        )

        // Create V2 contract
        val contractId: SigStakeInfV2.ContractId = createContract(participant1)

        val disclosedContract = ledger_api_utils
          .fetchContractsAsDisclosed(participant1, signatory, SigStakeInfV1.TEMPLATE_ID)
          .view
          .values
          .loneElement

        // Non-stakeholder exercises the contract via an explicit disclosure
        // It only vetted V1 so it forces its downgrade and must also accept the view
        participant2.ledger_api.javaapi.commands.submit(
          Seq(nonStakeholder),
          contractId
            .exerciseSigStakeInf_NoOp(nonStakeholder.toProtoPrimitive)
            .commands()
            .asScala
            .toList,
          disclosedContracts = Seq(disclosedContract),
        )
      }
    }

    "the creation package is not vetted by a contract observer" should {
      "succeed if the target package is vetted" in { implicit env =>
        import env.*

        SetupPackageVetting(
          Set(UpgradingBaseTest.UpgradeV1, UpgradingBaseTest.UpgradeV2),
          targetTopology = Map(
            synchronizer1Id -> Map[ParticipantReference, Seq[LfPackageId]](
              participant1 -> Seq(
                SigStakeInfV1.PACKAGE_ID.toPackageId,
                SigStakeInfV2.PACKAGE_ID.toPackageId,
              ),
              participant3 -> Seq(SigStakeInfV1.PACKAGE_ID.toPackageId),
            ).view.mapValues(VettedPackage.unbounded(_).toSet).toMap
          ),
        )

        // Create V2 contract
        val contractId: SigStakeInf.ContractId = createContract(participant1)

        // signatory exercises its contract,
        // but the observer which only vetted V1, forces its downgrade and must also accept the view
        participant1.ledger_api.javaapi.commands.submit(
          Seq(signatory),
          contractId
            .exerciseSigStakeInf_NoOp(signatory.toProtoPrimitive)
            .commands()
            .asScala
            .toList,
        )
      }
    }
  }

  private def createContract(
      participant1: => LocalParticipantReference
  ): SigStakeInf.ContractId = {
    val contractPayload = new SigStakeInfV2(signatory.toProtoPrimitive, observer.toProtoPrimitive)
    participant1.ledger_api.javaapi.commands
      .submit(Seq(signatory), contractPayload.create().commands().asScala.toList)
      .pipe(_.getEvents.asScala.headOption.value.getContractId)
      .pipe(new SigStakeInf.ContractId(_))
  }

  private implicit class StrToPkgId(packageIdStr: String) {
    def toPackageId: LfPackageId = Ref.PackageId.assertFromString(packageIdStr)
  }
}
