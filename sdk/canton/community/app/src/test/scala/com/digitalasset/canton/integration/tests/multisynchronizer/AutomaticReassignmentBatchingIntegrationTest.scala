// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.damltests.java.automaticreassignmenttransactions.Single
import com.digitalasset.canton.integration.plugins.UseProgrammableSequencer
import com.digitalasset.canton.integration.tests.SynchronizerRouterIntegrationTestSetup.{
  createAggregate,
  createSingle,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.sequencing.protocol.SubmissionRequest
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencerPolicies.isConfirmationResponse
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  SendDecision,
  SendPolicyWithoutTraceContext,
}

import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.*

class AutomaticReassignmentBatchingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  override def environmentDefinition: EnvironmentDefinition = EnvironmentDefinition.P1_S1M1_S1M1
    .addConfigTransform(
      ProgrammableSequencer.configOverride(this.getClass.toString, loggerFactory)
    )
    .withSetup { implicit env =>
      import env.*
      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
      participant1.dars.upload(CantonTestsPath)
    }

  "automatic reassignment" should {
    "batch when possible" in { implicit env =>
      import env.*

      val party1 = participant1.parties.enable("party1", synchronizer = daName)

      // Allocate the party on second synchronizer to avoid
      //   GrpcRequestRefusedByServer: NOT_FOUND/PACKAGE_NAME_DISCARDED_DUE_TO_UNVETTED_PACKAGES(11,0): Command interpretation failed: No packages with valid vetting exist that conform to topology restrictions for CantonTests, encountered in Template(#CantonTests:AutomaticReassignmentTransactions:Single).
      // when creating the Aggregate contract
      participant1.parties.enable(party1.identifier.unwrap, synchronizer = acmeName)

      // Create a bunch of contracts on synchronizer 1
      val contractIds: Seq[Single.ContractId] = Seq(
        // 3 contracts on synchronizer 1 -- these will need to be reassigned
        createSingle(party1, Some(synchronizer1Id), participant1, party1).id,
        createSingle(party1, Some(synchronizer1Id), participant1, party1).id,
        createSingle(party1, Some(synchronizer1Id), participant1, party1).id,
      )

      logger.debug(s"contract ids are $contractIds")

      // Create a contract on synchronizer 2, referencing those on synchronizer 1
      val aggregate = createAggregate(participant1, party1, contractIds, Some(synchronizer2Id))

      // Keep count of ConfirmationResponse's per sequencer
      val confResp = Seq(sequencer1, sequencer2).map(_.name -> new CountConfirmationResponses).toMap
      confResp.foreach { case (seqName, policy) =>
        getProgrammableSequencer(seqName).setPolicy_(CountConfirmationResponses.name)(policy)
      }

      // Submit transaction to make contracts on synchronizer 1 be reassigned to synchronizer 2
      participant1.ledger_api.javaapi.commands.submit(
        actAs = Seq(party1),
        commands = aggregate.id.exerciseCountAll().commands.asScala.toSeq,
        synchronizerId = Some(synchronizer2Id),
      )

      confResp(sequencer1.name).count shouldBe 1 // 1 for unassign (rather than batch size of 3)
      confResp(sequencer2.name).count shouldBe (1 + 1) // 1 for assign + 1 for transaction itself
    }
  }
}

class CountConfirmationResponses extends (SendPolicyWithoutTraceContext) {
  val counter = new AtomicInteger()
  def apply(req: SubmissionRequest): SendDecision = {
    if (isConfirmationResponse(req)) counter.incrementAndGet()
    SendDecision.Process
  }
  def count = counter.get
}
object CountConfirmationResponses {
  val name = "Count ConfirmationResponses"
}
