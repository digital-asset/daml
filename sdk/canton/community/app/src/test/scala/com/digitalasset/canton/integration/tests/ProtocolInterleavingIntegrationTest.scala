// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import cats.syntax.parallel.*
import com.daml.ledger.javaapi.data.codegen.ContractId
import com.digitalasset.base.error.ErrorResource
import com.digitalasset.base.error.utils.DecodedCantonError
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.damltests.java.conflicttest.{Many, Single}
import com.digitalasset.canton.integration.IntegrationTestUtilities.assertIncreasingRecordTime
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  IsolatedEnvironments,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.sequencing.protocol.MemberRecipient
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencerPolicies.isConfirmationResponse
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.daml.lf.data.ImmArray
import io.grpc.Status
import org.scalactic.source.Position

import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.nowarn
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters.*

trait ProtocolInterleavingIntegrationTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with HasProgrammableSequencer {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .withSetup { env =>
        env.sequencer1.topology.synchronizer_parameters.propose_update(
          env.daId,
          _.update(
            confirmationResponseTimeout = config.NonNegativeFiniteDuration.ofMinutes(4),
            mediatorReactionTimeout = config.NonNegativeFiniteDuration.ofMinutes(2),
          ),
        )
      }

  "process requests in between phase 3 and 7 of another request" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, daName)
    participant2.synchronizers.connect_local(sequencer1, daName)

    val participant1Id =
      participant1.id // Do not inline into the policy because this internally performs a gRPC call
    val participant2Id = participant2.id

    val sequencer = getProgrammableSequencer(sequencer1.name)

    val firstTransactionStartedPromise = Promise[Unit]()
    val secondTransactionCompletePromise = Promise[Unit]()

    // Hold back all non-ACS commitment messages from participant 2 until participant 1 has pinged itself
    sequencer.setPolicy_("wait for participant1 self-ping") { submissionRequest =>
      if (submissionRequest.sender == participant1Id) {
        firstTransactionStartedPromise.trySuccess(())
        SendDecision.Process
      } else if (
        submissionRequest.sender == participant2Id &&
        (submissionRequest.isConfirmationRequest || isConfirmationResponse(submissionRequest))
      )
        SendDecision.HoldBack(secondTransactionCompletePromise.future)
      else SendDecision.Process
    }

    val firstPing = Future(assertPingSucceeds(participant1, participant2))
    val secondPing = firstTransactionStartedPromise.future.map { _ =>
      assertPingSucceeds(participant1, participant1)
      secondTransactionCompletePromise.success(())
    }

    val patience = defaultPatience.copy(timeout = 100.seconds)
    firstPing.futureValue(patience, Position.here)
    secondPing.futureValue(patience, Position.here)

    assertIncreasingRecordTime(participant1)
    assertIncreasingRecordTime(participant2)
  }

  @nowarn("msg=match may not be exhaustive")
  def setupConflictTest(implicit
      env: TestConsoleEnvironment
  ): (
      PartyId,
      LocalParticipantReference,
      Single.Contract,
      Many.Contract,
      Future[Unit],
  ) = {

    import env.*

    participants.all.synchronizers.connect_local(sequencer1, daName)
    participants.all.dars.upload(CantonTestsPath)

    val mediatorId = mediator1.id

    val participantAlice = participant1
    val participantBob = participant2

    val alice =
      participantAlice.parties.enable(
        "Alice",
        synchronizeParticipants = Seq(participantBob),
      )
    val participantAliceId =
      participantAlice.id // Do not inline into the policy because this internally performs a gRPC call

    val bob = participantBob.parties.enable(
      "Bob",
      synchronizeParticipants = Seq(participantAlice),
    )
    val participantBobId =
      participantBob.id // Do not inline into the policy because this internally performs a gRPC call

    val bobAlice =
      new Many(
        bob.toProtoPrimitive,
        List(alice.toProtoPrimitive).asJava,
      ).create.commands.asScala.toSeq
    val Seq(many) =
      JavaDecodeUtil.decodeAllCreatedTree(Many.COMPANION)(
        participantBob.ledger_api.javaapi.commands.submit(Seq(bob), bobAlice)
      )

    val aliceCreate = new Single(alice.toProtoPrimitive).create.commands.asScala.toSeq
    val Seq(single) =
      JavaDecodeUtil.decodeAllCreatedTree(Single.COMPANION)(
        participantAlice.ledger_api.javaapi.commands.submit(Seq(alice), aliceCreate)
      )

    val sequencer = getProgrammableSequencer(sequencer1.name)

    val firstRequestConfirmedPromise = Promise[Unit]()
    sequencer.setPolicy_("delay Bob's confirmation") { submissionRequest =>
      if (
        submissionRequest.sender == participantBobId &&
        isConfirmationResponse(submissionRequest)
      ) {
        // Hold back Bob's confirmation for the first request until the mediator sends the result to Alice for the second request.
        SendDecision.OnHoldUntil { sent =>
          sent.sender == mediatorId && sent.batch.envelopes
            .forall(
              _.recipients.allRecipients.forgetNE == Set(MemberRecipient(participantAlice.id))
            )
        }
      } else {
        val alicesConfirmationResponse =
          submissionRequest.sender == participantAliceId &&
            isConfirmationResponse(submissionRequest)
        if (alicesConfirmationResponse) firstRequestConfirmedPromise.trySuccess(())
        SendDecision.Process
      }
    }

    (alice, participantAlice, single, many, firstRequestConfirmedPromise.future)
  }

  "pessimistically reject on conflicts" in { implicit env =>
    import env.*

    val (alice, participantAlice, single, many, firstRequestConfirmedF) =
      setupConflictTest(env)

    val archiveSingleF = Future {
      val cmd = single.id
        .exerciseFetchAndArchive(many.id)
        .commands
        .asScala
        .toSeq
      participantAlice.ledger_api.javaapi.commands.submit(
        Seq(alice),
        cmd,
        commandId = "Alice-FetchAndArchive",
      )
    }

    val followUpF = firstRequestConfirmedF.map { _ =>
      val cmd = single.id.exerciseFetchAsExercise().commands.asScala.toSeq
      assertThrowsAndLogsCommandFailures(
        participantAlice.ledger_api.javaapi.commands
          .submit(Seq(alice), cmd, commandId = "Alice-FetchAsExercise"),
        { logError =>
          val msg = logError.commandFailureMessage
          msg should
            include(
              """Request failed for participant1.
                        |  GrpcRequestRefusedByServer: ABORTED/LOCAL_VERDICT_LOCKED_CONTRACTS""".stripMargin
            )
          msg should include("Request: SubmitAndWaitTransactionTree")
        },
      )
    }

    val patience = defaultPatience.copy(timeout = 100.seconds)
    archiveSingleF.futureValue(patience, Position.here)
    followUpF.futureValue(patience, Position.here)
  }

  "non-consuming choices do not cause contention" in { implicit env =>
    import env.*

    val (alice, participantAlice, single, many, firstRequestConfirmedF) =
      setupConflictTest(env)

    val useSingleF = Future {
      val cmd =
        single.id.exerciseFetchOnly(many.id).commands.asScala.toSeq
      participantAlice.ledger_api.javaapi.commands.submit(
        Seq(alice),
        cmd,
        commandId = "Alice-FetchOnly",
      )
    }

    val followUpF = firstRequestConfirmedF.map { _ =>
      val cmd = single.id.exerciseDelete().commands.asScala.toSeq
      participantAlice.ledger_api.javaapi.commands
        .submit(Seq(alice), cmd, commandId = "Alice-Delete")
    }

    val patience = defaultPatience.copy(timeout = 100.seconds)
    useSingleF.futureValue(patience, Position.here)
    followUpF.futureValue(patience, Position.here)
  }

  "detect contention on contracts" in { implicit env =>
    import env.*

    // Create many contracts with Alice and Bob as stakeholders
    // Then each of them picks many subsets and tries to archive each subset in a single transaction
    // Check that each contract is archived at most once
    // We cannot check that there are no unnecessary rejections as Canton uses pessimistic rejections.

    val total = 200

    participants.all.synchronizers.connect_local(sequencer1, daName)
    participants.all.dars.upload(CantonTestsPath)

    val participantAlice = participant1
    val participantBob = participant2

    val alice =
      participantAlice.parties.enable(
        "Alice",
        synchronizeParticipants = Seq(participantBob),
      )

    val bob = participantBob.parties.enable(
      "Bob",
      synchronizeParticipants = Seq(participantAlice),
    )

    val createBobMany = new Many(
      bob.toProtoPrimitive,
      List(alice.toProtoPrimitive).asJava,
    ).create.commands.loneElement
    val manys = JavaDecodeUtil.decodeAllCreated(Many.COMPANION)(
      participantBob.ledger_api.javaapi.commands
        .submit_flat(Seq(bob), Seq.fill(total)(createBobMany), commandId = "setup")
    )
    manys.size shouldBe total

    val manyIds = manys.map(_.id).to(ImmArray)

    val aliceledgerCompletionOffset = participantAlice.ledger_api.state.end()
    val bobledgerCompletionOffset = participantBob.ledger_api.state.end()

    val rand = scala.util.Random
    val batches = new TrieMap[String, Seq[ContractId[Many]]]()
    val batchSize = 5

    // Batch types
    // 1. Consecutive contracts
    //    Alice {0,1,2,3,4}, {10,11,12,13,14}, ...
    //    Bob   {5,6,7,8,9}, {15,16,17,18,19}, ...
    val bobOffset = batchSize
    val consecutiveGroupSize = batchSize
    val consecutiveBatches = total / (2 * consecutiveGroupSize)
    val aliceConsecutive =
      Seq.tabulate(consecutiveBatches)(i =>
        Seq.tabulate(batchSize)(j => i * (2 * consecutiveGroupSize) + j)
      )
    val bobConsecutive =
      Seq.tabulate(consecutiveBatches)(i =>
        Seq.tabulate(batchSize)(j => i * (2 * consecutiveGroupSize) + j + bobOffset)
      )

    // 2. Non-consecutive contracts that the other party tries to archive with #1
    //    Alice {5,15,25,35,45}, {55,65,75,85,95}, ...
    //    Bob   {0,10,20,30,40}, {50,60,70,80,90}, ...
    val otherStep = 2 * batchSize
    val othersGroupSize = otherStep * batchSize
    val othersBatches = total / othersGroupSize
    val aliceBobMixed =
      Seq.tabulate(othersBatches)(i =>
        Seq.tabulate(batchSize)(j => i * othersGroupSize + j * otherStep + bobOffset)
      )
    val bobAliceMixed =
      Seq.tabulate(othersBatches)(i =>
        Seq.tabulate(batchSize)(j => i * othersGroupSize + j * otherStep)
      )

    // 3. Mix contracts archived by own consecutive batches and other's consecutive batches; doesn't overlap with #2.
    //    Alice {1,6,11,16,21}, {26,31,36,41,46}, ...
    //    Bob {2,7,12,17,22}, {27,32,37,42,47}, ...
    val mixedGroupSize = batchSize * batchSize
    val mixedBatches = total / mixedGroupSize
    val aliceMixed =
      Seq.tabulate(mixedBatches)(i =>
        Seq.tabulate(batchSize)(j => i * mixedGroupSize + j * batchSize + 1)
      )
    val bobMixed =
      Seq.tabulate(mixedBatches)(i =>
        Seq.tabulate(batchSize)(j => i * mixedGroupSize + j * batchSize + 2)
      )

    val aliceBatches = rand.shuffle(aliceConsecutive ++ aliceBobMixed ++ aliceMixed)
    val bobBatches = rand.shuffle(bobConsecutive ++ bobAliceMixed ++ bobMixed)

    val delayResults = Promise[Unit]()
    // Bob's participant must confirm all archivals.
    // Alice's participant must confirm the archivals where Alice is the actor, i.e., only those requests that Alice submits.
    // So we expect batchCount * 3 confirmation responses.
    val batchCount = consecutiveBatches + othersBatches + mixedBatches
    val expectedResponses = batchCount * 3
    val outstandingResponses = new AtomicInteger(expectedResponses)

    val sequencer = getProgrammableSequencer(sequencer1.name)
    sequencer.setPolicy("queue all mediator messages until all confirmation responses are there") {
      implicit traceContext => submissionRequest =>
        submissionRequest.sender match {
          case _: MediatorId => SendDecision.HoldBack(delayResults.future)
          case _: SequencerId => SendDecision.Process
          case _: ParticipantId =>
            if (isConfirmationResponse(submissionRequest)) {
              val newOutstanding = outstandingResponses.decrementAndGet()
              if (newOutstanding == 0) {
                delayResults.success(())
              } else if (newOutstanding < 0) {
                logger.error(s"Received more confirmation responses than expected")
              }
            }
            SendDecision.Process
        }
    }

    def archiveBatch(
        name: String,
        party: PartyId,
        participant: LocalParticipantReference,
        batch: Seq[Int],
    ): Future[Unit] = Future {
      val cids = batch.map(manyIds(_))
      val commandId = s"$name-${batch.mkString("-")}"
      batches.put(commandId, cids)

      val cmds = cids.map(_.exerciseDeleteMany(party.toProtoPrimitive).commands.loneElement)
      participant.ledger_api.javaapi.commands
        .submit_async(Seq(party), cmds, commandId = commandId)
      Thread.`yield`()
    }

    val aliceSubmissions =
      aliceBatches.parTraverse_(batch => archiveBatch("Alice", alice, participantAlice, batch))
    val bobSubmissions =
      bobBatches.parTraverse_(batch => archiveBatch("Bob", bob, participantBob, batch))

    val patience = defaultPatience.copy(timeout = defaultPatience.timeout.scaledBy(10))
    aliceSubmissions.futureValue(patience, Position.here)
    bobSubmissions.futureValue(patience, Position.here)

    val timeout = 2.minutes
    val aliceCompletions = participant1.ledger_api.completions.list(
      alice,
      batchCount,
      aliceledgerCompletionOffset,
      timeout = timeout,
      filter = _.commandId.startsWith("Alice"),
    )
    val bobCompletions = participant2.ledger_api.completions.list(
      bob,
      batchCount,
      bobledgerCompletionOffset,
      timeout = timeout,
      filter = _.commandId.startsWith("Bob"),
    )
    val allCompletions = aliceCompletions ++ bobCompletions
    val (acceptedCompletions, rejectedCompletions) =
      allCompletions.partition(_.status.forall(_.code == Status.OK.getCode.value))

    // The batches can conflict only groups of size lcm(consecutiveGroupSize, othersGroupSize, mixedGroupSize)
    val conflictGroupSize =
      Seq(consecutiveGroupSize, othersGroupSize, mixedGroupSize)
        .foldLeft(BigInt(1)) { (lcm, next) =>
          val nextB = BigInt(next)
          lcm.*(nextB)./(lcm.gcd(nextB))
        }
        .toInt

    val archivedContracts = mutable.Set.empty[ContractId[Many]]
    acceptedCompletions.foreach { completion =>
      val commandId = completion.commandId
      val batch =
        batches.remove(commandId).getOrElse(fail(s"No batch found for command ID $commandId"))
      batch.foreach { cid =>
        val noDoubleSpend = archivedContracts.add(cid)
        assert(noDoubleSpend, s"double archival of contract $cid in $completion")
      }
    }

    forEvery(rejectedCompletions) { completion =>
      val status = completion.status.value
      status.code should be(Status.ABORTED.getCode.value)
      val errorResources = DecodedCantonError.fromGrpcStatus(status).value.resources
      errorResources.groupMap(_._1)(_._2).get(ErrorResource.ContractId) match {
        case Some(lockedMatch :: Nil) =>
          val batch = batches(completion.commandId).map(_.contractId)
          // There can only be one contract in each rejection message because each view contains only one create node and
          // the mediator sends around only the rejection reason for the first view that gets rejected.
          val prettyCid = lockedMatch
          assert(batch.contains(prettyCid))
        case Some(rest) => fail(s"expected exactly one resource info in $rest, $status")
        case None => fail(s"No locked contract error in completion $completion")
      }
    }

    assert(
      acceptedCompletions.size >= total / conflictGroupSize,
      s"Didn't get a contract accepted from each conflict group of size $conflictGroupSize.",
    )

    val boundOnRejections = batchCount * 2 - (total / batchSize)
    assert(
      rejectedCompletions.size >= boundOnRejections,
      s"Got fewer batches rejected than expected.",
    )
  }
}

// class ProtocolInterleavingReferenceIntegrationTestDefault extends ProtocolInterleavingIntegrationTest {
//   registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
//   registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
// }

// class ProtocolInterleavingBftOrderingIntegrationTestDefault
//     extends ProtocolInterleavingIntegrationTest {
//   registerPlugin(new UseBftOrderingBlockSequencer(loggerFactory))
//   registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
// }

class ProtocolInterleavingReferenceIntegrationTestPostgres
    extends ProtocolInterleavingIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}

class ProtocolInterleavingBftOrderingIntegrationTestPostgres
    extends ProtocolInterleavingIntegrationTest {
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
