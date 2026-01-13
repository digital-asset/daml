// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.transaction.Transaction.toJavaProto
import com.daml.ledger.api.v2.value.Value
import com.daml.ledger.api.v2.value.Value.Sum
import com.daml.ledger.javaapi.data.Transaction
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.damltestsdev.java.basickeys.{BasicKey, KeyOps}
import com.digitalasset.canton.examples.java.cycle.Cycle
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.util.TestSubmissionService
import com.digitalasset.canton.integration.util.TestSubmissionService.{
  CommandsWithMetadata,
  LastAssignedKeyResolver,
}
import com.digitalasset.canton.integration.util.TestUtils.damlSet
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.Status.Code

import scala.jdk.CollectionConverters.*

abstract sealed class TestSubmissionServiceIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  def cantonTestsPath: String

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.dars.upload(cantonTestsPath)
      }

  protected def submitMaliciousCommand(
      participant: LocalParticipantReference,
      command: CommandsWithMetadata,
      submitter: PartyId,
      testSubmissionServiceOverrideO: Option[TestSubmissionService] = None,
  )(implicit env: TestConsoleEnvironment): Completion = {
    import env.*

    val maliciousParticipantNode = MaliciousParticipantNode(
      participant,
      daId,
      testedProtocolVersion,
      timeouts,
      loggerFactory,
      testSubmissionServiceOverrideO = testSubmissionServiceOverrideO,
    )

    val ledgerEnd = participant.ledger_api.state.end()

    maliciousParticipantNode.submitCommand(command).futureValueUS // success

    participant.ledger_api.completions.list(submitter, 1, ledgerEnd).loneElement
  }

}

abstract sealed class TestSubmissionServiceIntegrationTestStableLf
    extends TestSubmissionServiceIntegrationTest {
  override def cantonTestsPath: String = CantonTestsPath

  "Process a single command" in { implicit env =>
    import env.*

    val submissionService =
      TestSubmissionService(
        participant1
      )

    val submitter = participant1.adminParty
    val createCommands =
      CommandsWithMetadata(
        new Cycle("foo", submitter.toProtoPrimitive).create.commands.asScala.toSeq
          .map(c => Command.fromJavaProto(c.toProtoCommand)),
        Seq(submitter),
      )

    val cycle = JavaDecodeUtil
      .decodeAllCreated(Cycle.COMPANION)(
        Transaction.fromProto(
          toJavaProto(
            submissionService
              .submitAndWaitForTransaction(participant1, createCommands)
              .futureValue
          )
        )
      )
      .loneElement

    // Use "def" to ensure a fresh command id on every usage.
    def exerciseCommands: CommandsWithMetadata = CommandsWithMetadata(
      cycle.id
        .exerciseArchive()
        .commands
        .asScala
        .toSeq
        .map(c => Command.fromJavaProto(c.toProtoCommand)),
      Seq(submitter),
    )
    submissionService
      .submitAndWaitForTransaction(participant1, exerciseCommands)
      .futureValue

    val completion =
      submitMaliciousCommand(participant1, exerciseCommands, submitter, Some(submissionService))

    completion.status.value.code shouldBe Code.NOT_FOUND.value()
    completion.status.value.message should startWith("LOCAL_VERDICT_INACTIVE_CONTRACTS")
  }
}

abstract sealed class TestSubmissionServiceIntegrationTestDevLf
    extends TestSubmissionServiceIntegrationTest {
  override def cantonTestsPath: String = CantonTestsDevPath

  "Create, fetch and lookup by key" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
    implicit env =>
      import env.*

      val submissionServiceWithActiveKeyResolver =
        TestSubmissionService(
          participant1,
          enableLfDev = true,
        )
      val submissionServiceWithLastAssignedKeyResolver =
        TestSubmissionService(
          participant = participant1,
          customKeyResolver = Some(LastAssignedKeyResolver(participant1)),
          enableLfDev = true,
        )

      val submitter = participant1.adminParty

      // create KeyOps contract
      val createOpsCommands =
        CommandsWithMetadata(
          new KeyOps(
            damlSet(Set(submitter.toProtoPrimitive)),
            submitter.toProtoPrimitive,
          ).create.commands.asScala.toSeq
            .map(c => Command.fromJavaProto(c.toProtoCommand)),
          Seq(submitter),
        )
      val keyOps = JavaDecodeUtil
        .decodeAllCreated(KeyOps.COMPANION)(
          Transaction.fromProto(
            toJavaProto(
              submissionServiceWithActiveKeyResolver
                .submitAndWaitForTransaction(participant1, createOpsCommands)
                .futureValue
            )
          )
        )
        .loneElement

      // Directly create a BasicKey contract
      val createCommands =
        CommandsWithMetadata(
          new BasicKey(
            damlSet(Set(submitter.toProtoPrimitive)),
            damlSet(Set.empty),
          ).create.commands.asScala.toSeq
            .map(c => Command.fromJavaProto(c.toProtoCommand)),
          Seq(submitter),
        )
      submissionServiceWithActiveKeyResolver
        .submitAndWaitForTransaction(participant1, createCommands)
        .futureValue

      // Use the KeyOps contract to fetch-by-key the BasicKey contract
      // Use "def" to allow for reusing the command.
      def fetchKeyCommands: CommandsWithMetadata =
        CommandsWithMetadata(
          keyOps.id
            .exerciseFetch(submitter.toProtoPrimitive)
            .commands
            .asScala
            .toSeq
            .map(c => Command.fromJavaProto(c.toProtoCommand)),
          Seq(submitter),
        )
      submissionServiceWithActiveKeyResolver
        .submitAndWaitForTransaction(participant1, fetchKeyCommands)
        .futureValue

      def lookupByKeyCommands: CommandsWithMetadata =
        CommandsWithMetadata(
          keyOps.id
            .exerciseLookup(submitter.toProtoPrimitive)
            .commands
            .asScala
            .toSeq
            .map(c => Command.fromJavaProto(c.toProtoCommand)),
          Seq(submitter),
        )

      def lookup(): Option[Value] = {
        val lookupTransactionTree =
          submissionServiceWithActiveKeyResolver
            .submitAndWaitForTransaction(participant1, lookupByKeyCommands)
            .futureValue
        IntegrationTestUtilities.extractSubmissionResult(lookupTransactionTree) match {
          case Sum.Optional(res) => res.value
          case r => fail(s"Unexpected result type: $r")
        }
      }

      // Use the KeyOps contract to lookup-by-key the BasicKey contract
      lookup() should not be empty

      // Submit a transaction that erroneously resolves the key to None.
      // Expecting a Canton specific error in the command completion
      // TODO(#16065) Re-enable these assertions once contract keys are supported in 3.x
      //    val submissionServiceWithEmptyKeyResolver =
      //      TestSubmissionService(participant1, Some(EmptyKeyResolver))
      //    val c1: Completion = submissionServiceWithEmptyKeyResolver
      //      .submitAndWaitForCompletion(participant1, lookupByKeyCommands)
      //      .futureValue
      //    c1.status.value.code shouldBe Code.FAILED_PRECONDITION.value()
      //    c1.status.value.message should startWith("INCONSISTENT_CONTRACT_KEY")

      // Use the KeyOps contract to archive-by-key the BasicKey contract
      val exerciseByKeyCommands =
        CommandsWithMetadata(
          keyOps.id
            .exerciseDeleteKey(submitter.toProtoPrimitive, damlSet(Set(submitter.toProtoPrimitive)))
            .commands
            .asScala
            .toSeq
            .map(c => Command.fromJavaProto(c.toProtoCommand)),
          Seq(submitter),
        )
      submissionServiceWithActiveKeyResolver
        .submitAndWaitForTransaction(participant1, exerciseByKeyCommands)
        .futureValue

      // Use the KeyOps contract to lookup-by-key the BasicKey contract
      lookup() shouldBe empty

      // Submit a failing fetch-by-key command.
      // Expecting that DAMLe fails the submission
      val ex = submissionServiceWithActiveKeyResolver
        .submitAndWaitForCompletion(participant1, fetchKeyCommands)
        .failed
        .futureValue
      ex.getMessage should include("ContractKeyNotFound")

      // Submit a fetch-by-key transaction that erroneously resolves the key to the archived contract.
      // Expecting a Canton specific error in the command completion
      val c2 = submitMaliciousCommand(
        participant1,
        fetchKeyCommands,
        submitter,
        Some(submissionServiceWithLastAssignedKeyResolver),
      )
      c2.status.value.code shouldBe Code.NOT_FOUND.value()
      c2.status.value.message should startWith("LOCAL_VERDICT_INACTIVE_CONTRACTS")

      // Submit a lookup-by-key transaction that erroneously resolves the key to the archived contract.
      // Expecting a Canton specific error in the command completion
      val c3 = submitMaliciousCommand(
        participant1,
        lookupByKeyCommands,
        submitter,
        Some(submissionServiceWithLastAssignedKeyResolver),
      )
      c3.status.value.code shouldBe Code.NOT_FOUND.value()
      c3.status.value.message should startWith("LOCAL_VERDICT_INACTIVE_CONTRACTS")
  }
}

trait TestSubmissionServiceReferenceSequencerPostgresTest {
  self: SharedEnvironment =>
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class ReferenceTestSubmissionServiceIntegrationTestPostgresStableLf
    extends TestSubmissionServiceIntegrationTestStableLf
    with TestSubmissionServiceReferenceSequencerPostgresTest

class ReferenceTestSubmissionServiceIntegrationTestPostgresDevLf
    extends TestSubmissionServiceIntegrationTestDevLf
    with TestSubmissionServiceReferenceSequencerPostgresTest
