// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.submission

import com.daml.ledger.javaapi.data.codegen.ContractId as CodeGenCID
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.damltests.java.localcontract.Holder
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.ExternalParty

/** Test that external signing supports locally created contracts used in subviews. This is a
  * consequence of a bug reported in 3.3 and fixed via a participant feature flag. See
  * https://github.com/DACH-NY/canton/issues/27883
  */
trait LocalContractIntegrationTestSetup
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BaseInteractiveSubmissionTest
    with HasCycleUtils {

  protected var aliceE: ExternalParty = _
  protected var bobE: ExternalParty = _
  protected var charlieE: ExternalParty = _

  protected def aliceHost(implicit env: TestConsoleEnvironment): LocalParticipantReference =
    env.participant3
  protected def bobHost(implicit env: TestConsoleEnvironment): LocalParticipantReference =
    env.participant2
  protected def charlieHost(implicit env: TestConsoleEnvironment): LocalParticipantReference =
    env.participant1

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)

        participants.all.dars.upload(CantonTestsPath)
        aliceE = aliceHost.parties.testing.external.enable("Alice")
        bobE = bobHost.parties.testing.external.enable("Bob")
        charlieE = charlieHost.parties.testing.external.enable("Charlie")
      }
      .addConfigTransform(ConfigTransforms.enableInteractiveSubmissionTransforms)
}

class LocalContractIntegrationTest extends LocalContractIntegrationTestSetup {
  "Externally signing" should {
    "support transactions with local contracts used in subviews" in { implicit env =>
      val createdEventHolderTx = aliceHost.ledger_api.javaapi.commands.submit(
        Seq(aliceE),
        Seq(
          new Holder(aliceE.toProtoPrimitive, charlieE.toProtoPrimitive).create.commands.loneElement
        ),
      )

      val holder =
        JavaDecodeUtil.decodeAllCreated(Holder.COMPANION)(createdEventHolderTx).loneElement

      // Exercise the CreateAndUse choice
      val exerciseCommand = Holder.ContractId
        .fromContractId(new CodeGenCID(holder.id.contractId))
        .exerciseCreateAndUse(bobE.toProtoPrimitive)

      val preparedTransaction = aliceHost.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(exerciseCommand.commands().loneElement),
      )

      aliceHost.ledger_api.commands.external.submit_prepared(
        aliceE,
        preparedTransaction,
      )
    }
  }
}
