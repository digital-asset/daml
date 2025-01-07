// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.digitalasset.canton.protocol.{LfContractId, LfLanguageVersion, LfVersionedTransaction}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.version.DamlLfVersionToProtocolVersions
import com.digitalasset.canton.{BaseTest, LfPackageId, LfPartyId, LfValue}
import com.digitalasset.daml.lf.data.Ref.QualifiedName
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.transaction.test.TestNodeBuilder.{
  CreateKey,
  CreateTransactionVersion,
}
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.Implicits.*
import com.digitalasset.daml.lf.transaction.test.TreeTransactionBuilder.*
import com.digitalasset.daml.lf.transaction.test.{
  TestIdFactory,
  TestNodeBuilder,
  TreeTransactionBuilder,
}
import com.digitalasset.daml.lf.value.Value.ValueRecord

private[submission] object DomainSelectionFixture extends TestIdFactory {

  def unknownPackageFor(participantId: ParticipantId, missingPackage: LfPackageId) =
    TransactionTreeFactory.PackageUnknownTo(
      missingPackage,
      participantId,
    )

  /*
   We cannot take the maximum transaction version available. The reason is that if the test is run
   with a low protocol version, then some filter will reject the transaction (because high transaction
   version needs high protocol version).
   */
  lazy val fixtureTransactionVersion: LfLanguageVersion =
    DamlLfVersionToProtocolVersions.damlLfVersionToMinimumProtocolVersions.collect {
      case (txVersion, protocolVersion) if protocolVersion <= BaseTest.testedProtocolVersion =>
        txVersion
    }.last

  /*
  Simple topology, with two parties (signatory, observer) each connected to one
  participant (submittingParticipantId, observerParticipantId)
   */
  object SimpleTopology {
    val submitterParticipantId: ParticipantId = ParticipantId("submitter")
    val observerParticipantId: ParticipantId = ParticipantId("counter")
    val participantId3: ParticipantId = ParticipantId("participant3")

    val signatory: LfPartyId = LfPartyId.assertFromString("signatory::default")
    val observer: LfPartyId = LfPartyId.assertFromString("observer::default")
    val party3: LfPartyId = LfPartyId.assertFromString("party3::default")

    val correctTopology: Map[LfPartyId, List[ParticipantId]] = Map(
      signatory -> List(submitterParticipantId),
      observer -> List(observerParticipantId),
    )

    def defaultTestingIdentityFactory(
        topology: Map[LfPartyId, List[ParticipantId]],
        packages: Seq[VettedPackage] = Seq(),
    ): TopologySnapshot = {
      val participants = topology.values.flatten
      val testingIdentityFactory = TestingTopology
        .from(
          topology = topology.map { case (partyId, participantIds) =>
            partyId -> participantIds.map(_ -> Submission).toMap
          },
          participants = participants.map(_ -> ParticipantAttributes(Submission)).toMap,
          packages = participants.view.map(_ -> packages).toMap,
        )
        .build()

      testingIdentityFactory.topologySnapshot()
    }
  }

  object Transactions {

    private[this] val DefaultLfVersion =
      LfLanguageVersion.StableVersions(LfLanguageVersion.Major.V2).max

    def buildExerciseNode(
        version: LfLanguageVersion,
        inputContractId: LfContractId,
        signatory: LfPartyId,
        observer: LfPartyId,
        interfaceId: Option[Ref.Identifier] = None,
    ): Node.Exercise = {
      val createNode = TestNodeBuilder.create(
        id = inputContractId,
        templateId = "M:T",
        argument = LfValue.ValueUnit,
        signatories = List(signatory),
        observers = List(observer),
        key = CreateKey.NoKey,
        version = CreateTransactionVersion.Version(version),
      )

      TestNodeBuilder.exercise(
        contract = createNode,
        choice = "someChoice",
        consuming = true,
        actingParties = Set(signatory),
        argument = LfValue.ValueUnit,
        interfaceId = interfaceId,
        result = Some(LfValue.ValueUnit),
        choiceObservers = Set.empty,
        byKey = false,
      )

    }

    object Create {
      val correctPackages: Seq[VettedPackage] = VettedPackage.unbounded(Seq(defaultPackageId))

      def tx(
          version: LfLanguageVersion = DefaultLfVersion
      ): LfVersionedTransaction = {
        import SimpleTopology.*
        TreeTransactionBuilder.toVersionedTransaction(
          TestNodeBuilder.create(
            id = newCid,
            templateId = "M:T",
            argument = ValueRecord(None, ImmArray.Empty),
            signatories = Seq(signatory),
            observers = Seq(observer),
            key = CreateKey.NoKey,
            version = CreateTransactionVersion.Version(version),
          )
        )
      }
    }

    final case class ThreeExercises(
        version: LfLanguageVersion = DefaultLfVersion
    ) {

      import SimpleTopology.*

      val inputContract1Id: LfContractId = newCid
      val inputContract2Id: LfContractId = newCid
      val inputContract3Id: LfContractId = newCid
      val inputContractIds: Seq[LfContractId] =
        Seq(inputContract1Id, inputContract2Id, inputContract3Id)

      private val value =
        inputContractIds.map[NodeWrapper](buildExerciseNode(version, _, signatory, observer))
      val tx: LfVersionedTransaction = toVersionedTransaction(value*)

    }

    object ExerciseByInterface {
      /* To be sure that we have two different package ID (one for the create
      and the other for the interface id).
       */
      val interfacePackageId = s"$defaultPackageId for interface"

      val correctPackages: Seq[VettedPackage] =
        VettedPackage.unbounded(Seq(defaultPackageId, interfacePackageId))
    }

    final case class ExerciseByInterface(
        version: LfLanguageVersion = DefaultLfVersion
    ) {
      import ExerciseByInterface.*
      import SimpleTopology.*

      val inputContractId: LfContractId = newCid

      val tx: LfVersionedTransaction = toVersionedTransaction(
        buildExerciseNode(
          version,
          inputContractId,
          signatory,
          observer,
          interfaceId = Some(
            Ref.Identifier(interfacePackageId, QualifiedName.assertFromString("module:template"))
          ),
        )
      )

    }
  }
}
