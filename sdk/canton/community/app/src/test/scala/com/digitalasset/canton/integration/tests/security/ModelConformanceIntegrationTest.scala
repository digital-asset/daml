// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.javaapi.data
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.damltests.java.refs.Refs
import com.digitalasset.canton.damltestsdev.java.basickeys.{BasicKey, KeyOps}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.integration.util.TestUtils.damlSet
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.protocol.ProtocolProcessor
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects.ModelConformance
import com.digitalasset.canton.protocol.messages.Verdict
import com.digitalasset.canton.protocol.{LfContractId, LfSubmittedTransaction}
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.canton.version.ProtocolVersion

import java.util
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.jdk.CollectionConverters.*

trait ModelConformanceIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with HasCycleUtils
    with SecurityTestHelpers {

  private val partyCounter = new AtomicInteger(1)

  private var maliciousP1: MaliciousParticipantNode = _

  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()
  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1.withSetup { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      if (testedProtocolVersion.isDev) participant1.dars.upload(CantonTestsDevPath)
      participant1.dars.upload(CantonTestsPath)

      maliciousP1 = MaliciousParticipantNode(
        participant1,
        daId,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
      )
      pureCryptoRef.set(participant1.crypto.pureCrypto)
    }

  private def allocateUniqueParty(implicit env: TestConsoleEnvironment): PartyId =
    env.participant1.parties.enable(
      s"McParty${partyCounter.getAndIncrement()}"
    )

  "A participant" should {

    "fail gracefully on forward references in transactions" in { implicit env =>
      import env.*

      def mkCreateCommand(cids: Refs.ContractId*): Seq[data.Command] = {
        val cidsJava = new util.ArrayList[Refs.ContractId]()
        cids.foreach(cidsJava.add)

        new Refs(participant1.adminParty.toProtoPrimitive, cidsJava)
          .create()
          .commands()
          .asScala
          .toSeq
      }

      // Create a dummy contract id for later use
      val dummyCid = JavaDecodeUtil
        .decodeAllCreated(Refs.COMPANION)(
          participant1.ledger_api.javaapi.commands.submit(
            Seq(participant1.adminParty),
            mkCreateCommand(),
          )
        )
        .loneElement
        .id
      val dummyLfCid = LfContractId.assertFromString(dummyCid.contractId)

      // Transaction with two root nodes:
      // 1. Create Refs([dummyCid])
      // 2. Create Refs([])
      val cmds = mkCreateCommand(dummyCid) ++ mkCreateCommand()
      val cmdsWithMetadata = CommandsWithMetadata(
        cmds.map(c => Command.fromJavaProto(c.toProtoCommand)),
        Seq(participant1.adminParty),
      )

      // Submit the command, extract the contract id of the second created contract by throwing an exception.
      case class CidException(cid: LfContractId) extends RuntimeException

      val cidOfNode2 = inside(
        maliciousP1
          .submitCommand(
            cmdsWithMetadata,
            transactionTreeInterceptor = tree => {
              val cidOfNode2 = tree.rootViews
                .toSeq(1)
                .tryUnwrap
                .viewParticipantData
                .tryUnwrap
                .createdCore
                .loneElement
                .contract
                .contractId

              throw CidException(cidOfNode2)
            },
          )
          .value
          .failed
          .futureValueUS
      ) { case CidException(cid) =>
        cid
      }

      // Now submit the command for real.
      // Use an interceptor to effectively submit the transaction with the following 2 root nodes:
      // 1. create([cidOfNode2])
      // 2. create([])
      // So the id cidOfNode2 is used before the creation of the underlying contract.
      val ((result, events), _) = loggerFactory.assertLogs(
        ProtocolProcessor.withApprovalContradictionCheckDisabled(loggerFactory) {
          replacingConfirmationResult(
            daId,
            sequencer1,
            mediator1,
            withMediatorVerdict(Verdict.Approve(testedProtocolVersion)),
          ) {
            trackingLedgerEvents(Seq(participant1), Seq.empty) {
              maliciousP1
                .submitCommand(
                  cmdsWithMetadata,
                  transactionInterceptor = tx => {
                    LfSubmittedTransaction(
                      tx.mapCid(cid => if (cid == dummyLfCid) cidOfNode2 else cid)
                    )
                  },
                )
                .futureValueUS
            }
          }
        },
        _.shouldBeCantonError(
          ModelConformance,
          _ should fullyMatch regex raw"Rejected transaction due to a failed model conformance check: " +
            raw"Contract id \S+ created in node NodeId\(1\) is referenced before in NodeId\(0\)",
        ),
      )

      result.valueOrFail("Unexpected send failure")
      events.assertStatusOk(participant1)

      assertPingSucceeds(participant1, participant1)
    }

    "validate LookupByKey call" onlyRunWithOrGreaterThan ProtocolVersion.dev in { implicit env =>
      import env.*

      val alice = allocateUniqueParty
      val bob = allocateUniqueParty

      val createBasicKey =
        new BasicKey(
          damlSet(Set(alice.toProtoPrimitive)),
          damlSet(Set(bob.toProtoPrimitive)),
        ).create.commands.asScala.toSeq
      val createKeyOps = new KeyOps(
        damlSet(Set(alice.toProtoPrimitive)),
        bob.toProtoPrimitive,
      ).create.commands.asScala.toSeq

      val keyOps = JavaDecodeUtil
        .decodeAllCreated(KeyOps.COMPANION)(
          participant1.ledger_api.javaapi.commands
            .submit(Seq(alice), createKeyOps ++ createBasicKey)
        )
        .loneElement

      val lookupBasicKey = keyOps.id.exerciseLookup(alice.toProtoPrimitive).commands.asScala.toSeq
      participant1.ledger_api.javaapi.commands.submit(Seq(alice), lookupBasicKey)

      succeed
    }
  }

  // There should be a test in this suite that verifies the behavior in the following scenario:
  //
  //   * a request fails the model conformance check;
  //   * the mediator nonetheless approves the request;
  //   * a subset of the transaction passes the model conformance check.
  //
  // In this case, we want the honest participants to rollback the root view, but commit the valid subtransaction,
  // in order to preserve transparency. This results in a "transaction accepted" event that covers the subtransaction.
  //
  // This scenario is covered by the following test in [[LedgerAuthorizationIntegrationTest]]:
  //
  // "all honest participants roll back a view with a security alert" when_ { mitigation =>
  //    "the mediator approves the request" when {
  //      "the root action paired with its dynamic authorizers is not well-authorized" taggedAs_ { ...

  // Similarly, the behavior must be verified for the following scenario:
  //
  //   * a request fails the model conformance check;
  //   * the mediator nonetheless approves the request;
  //   * no subset of the transaction passes the model conformance check.
  //
  // In this case, the honest participants can reject the transaction, resulting in a "transaction rejected" event,
  // because no participant will commit a subset and violate transparency.
  //
  // This scenario is covered by several tests in [[LedgerAuthorizationIntegrationTest]] under:
  //
  // "every honest participant rolls back the request with a security alert" when_ { mitigation => ...

}

class ModelConformanceIntegrationTestPostgres extends ModelConformanceIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
