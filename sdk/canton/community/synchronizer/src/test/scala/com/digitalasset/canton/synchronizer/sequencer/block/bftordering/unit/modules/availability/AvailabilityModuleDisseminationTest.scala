// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.availability

import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability.LocalDissemination.LocalBatchStoredSigned
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability.{
  LocalDissemination,
  RemoteDissemination,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.*
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference

class AvailabilityModuleDisseminationTest
    extends AnyWordSpec
    with BftSequencerBaseTest
    with AvailabilityModuleTestUtils {

  "The availability module" when {

    "it receives Dissemination.LocalBatchCreated (from local mempool)" should {

      "should store in the local store" in {
        val disseminationProtocolState = new DisseminationProtocolState()
        val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])

        val availability = createAvailability[IgnoringUnitTestEnv](
          availabilityStore = availabilityStore,
          disseminationProtocolState = disseminationProtocolState,
        )
        availability.receive(
          LocalDissemination.LocalBatchCreated(Seq(anOrderingRequest))
        )

        disseminationProtocolState.disseminationProgress should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
        verify(availabilityStore, times(1)).addBatch(ABatchId, ABatch)
      }

      "should create the batch using the known epoch number" in {
        val disseminationProtocolState = new DisseminationProtocolState()
        val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])

        val epochNumber = EpochNumber(50)

        val availability = createAvailability[IgnoringUnitTestEnv](
          availabilityStore = availabilityStore,
          disseminationProtocolState = disseminationProtocolState,
          initialEpochNumber = epochNumber,
        )
        availability.receive(
          LocalDissemination.LocalBatchCreated(Seq(anOrderingRequest))
        )

        val batch = OrderingRequestBatch.create(
          Seq(anOrderingRequest),
          epochNumber,
        )

        verify(availabilityStore, times(1)).addBatch(BatchId.from(batch), batch)
      }
    }
  }

  "it receives Dissemination.LocalBatchStored (from local store), " +
    "there are no consensus requests and " +
    "there are no other nodes (so, F == 0)" should {

      "clear dissemination progress and " +
        "mark the batch ready for ordering" in {
          implicit val ctx
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext()
          val disseminationProtocolState = new DisseminationProtocolState()

          val cryptoProvider = newMockCrypto

          val me = Node0
          val availability = createAvailability[ProgrammableUnitTestEnv](
            disseminationProtocolState = disseminationProtocolState,
            myId = me,
            cryptoProvider = cryptoProvider,
          )
          availability.receive(LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)))
          ctx.runPipedMessagesAndReceiveOnModule(availability) // Perform signing

          verify(cryptoProvider).signHash(
            AvailabilityAck.hashFor(ABatchId, anEpochNumber, me)
          )

          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.batchesReadyForOrdering should
            contain only BatchReadyForOrderingNode0Vote
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)
        }
    }

  "it receives Dissemination.LocalBatchStored (from local store), " +
    "there are no consensus requests and " +
    "F > 0" should {

      "just update dissemination progress" in {
        implicit val ctx
            : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext()
        val disseminationProtocolState = new DisseminationProtocolState()

        val me = Node0
        val cryptoProvider = newMockCrypto

        val availability = createAvailability[ProgrammableUnitTestEnv](
          otherNodes = Node1To3,
          myId = me,
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        availability.receive(LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)))
        ctx.runPipedMessagesAndReceiveOnModule(availability) // Perform signing

        verify(cryptoProvider).signHash(
          AvailabilityAck.hashFor(ABatchId, anEpochNumber, me)
        )

        disseminationProtocolState.disseminationProgress should
          contain only ABatchDisseminationProgressNode0To3WithNode0Vote
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      }
    }

  "it receives Dissemination.LocalBatchStored (from local store), " +
    "there are consensus requests, " +
    "there are no other nodes (so, F == 0) " should {

      "just send proposal to local consensus" in {
        implicit val ctx
            : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext()
        val disseminationProtocolState = new DisseminationProtocolState()
        disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
        val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
        val cryptoProvider = newMockCrypto
        val me = Node0
        val availability = createAvailability[ProgrammableUnitTestEnv](
          consensus = fakeCellModule(consensusCell),
          myId = me,
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        availability.receive(LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)))
        ctx.runPipedMessagesAndReceiveOnModule(availability) // Perform signing

        verify(cryptoProvider).signHash(
          AvailabilityAck.hashFor(ABatchId, anEpochNumber, me)
        )

        disseminationProtocolState.disseminationProgress should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
        disseminationProtocolState.batchesReadyForOrdering should not be empty

        consensusCell.get() should contain(ABatchProposalNode0VoteNode0InTopology)
        availability.receive(Availability.Consensus.Ordered(Seq(ABatchId)))
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
      }
    }

  "it receives Dissemination.LocalBatchStored (from local store), " +
    "there are consensus requests, " +
    "there are other nodes and " +
    "F == 0" should {

      "send proposal to local consensus and " +
        "broadcast Dissemination.RemoteBatch" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
          val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
          val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)
          val me = Node0
          val cryptoProvider = spy(ProgrammableUnitTestEnv.noSignatureCryptoProvider)

          implicit val context
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext
          val availability = createAvailability[ProgrammableUnitTestEnv](
            otherNodes = Node1And2,
            myId = me,
            cryptoProvider = cryptoProvider,
            consensus = fakeCellModule(consensusCell),
            p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
            disseminationProtocolState = disseminationProtocolState,
          )
          availability.receive(LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)))

          verify(cryptoProvider).signHash(
            AvailabilityAck.hashFor(ABatchId, anEpochNumber, me)
          )

          availability.receive(
            LocalDissemination.LocalBatchesStoredSigned(
              Seq(LocalBatchStoredSigned(ABatchId, ABatch, Right(Signature.noSignature)))
            )
          )

          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)

          disseminationProtocolState.batchesReadyForOrdering should not be empty
          consensusCell.get() should contain(ABatchProposalNode0VoteNodes0To2InTopology)
          availability.receive(Availability.Consensus.Ordered(Seq(ABatchId)))
          disseminationProtocolState.batchesReadyForOrdering should be(empty)

          p2pNetworkOutCell.get() shouldBe None
          val remoteBatch = RemoteDissemination.RemoteBatch
            .create(ABatchId, ABatch, Node0)
          verify(cryptoProvider).signMessage(
            remoteBatch,
            AuthenticatedMessageType.BftSignedAvailabilityMessage,
          )

          context.runPipedMessagesAndReceiveOnModule(availability)

          p2pNetworkOutCell.get() should contain(
            P2PNetworkOut.Multicast(
              P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
                remoteBatch.fakeSign
              ),
              Set(Node1, Node2),
            )
          )
        }
    }

  "it receives Dissemination.LocalBatchStored (from local store), " +
    "there are consensus requests and " +
    "F > 0 (so, there must also be other nodes)" should {

      "update dissemination progress and " +
        "broadcast Dissemination.RemoteBatch" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
          val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)
          val me = Node0
          val cryptoProvider = spy(ProgrammableUnitTestEnv.noSignatureCryptoProvider)
          implicit val context
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext
          val availability = createAvailability[ProgrammableUnitTestEnv](
            otherNodes = Node1To3,
            myId = me,
            cryptoProvider = cryptoProvider,
            p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
            disseminationProtocolState = disseminationProtocolState,
          )
          availability.receive(LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)))

          verify(cryptoProvider).signHash(
            AvailabilityAck.hashFor(ABatchId, anEpochNumber, me)
          )

          availability.receive(
            LocalDissemination.LocalBatchesStoredSigned(
              Seq(LocalBatchStoredSigned(ABatchId, ABatch, Right(Signature.noSignature)))
            )
          )

          disseminationProtocolState.disseminationProgress should
            contain only ABatchDisseminationProgressNode0To3WithNode0Vote
          disseminationProtocolState.batchesReadyForOrdering should be(empty)
          disseminationProtocolState.toBeProvidedToConsensus should contain only AToBeProvidedToConsensus
          p2pNetworkOutCell.get() shouldBe None
          val remoteBatch = RemoteDissemination.RemoteBatch.create(ABatchId, ABatch, Node0)
          verify(cryptoProvider).signMessage(
            remoteBatch,
            AuthenticatedMessageType.BftSignedAvailabilityMessage,
          )

          context.runPipedMessagesAndReceiveOnModule(availability)

          p2pNetworkOutCell.get() should contain(
            P2PNetworkOut.Multicast(
              P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
                remoteBatch.fakeSign
              ),
              Node1To3,
            )
          )
        }
    }

  "it receives Dissemination.RemoteBatch (from node)" should {

    "store in the local store" in {
      val disseminationProtocolState = new DisseminationProtocolState()

      val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])
      val availability = createAvailability[IgnoringUnitTestEnv](
        availabilityStore = availabilityStore,
        disseminationProtocolState = disseminationProtocolState,
      )
      availability.receive(
        RemoteDissemination.RemoteBatch.create(ABatchId, ABatch, from = Node1)
      )

      disseminationProtocolState.disseminationProgress should be(empty)
      disseminationProtocolState.batchesReadyForOrdering should be(empty)
      disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      verify(availabilityStore, times(1)).addBatch(ABatchId, ABatch)
    }

    "not store if it is the wrong batchId" in {
      val disseminationProtocolState = new DisseminationProtocolState()

      val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])
      val availability = createAvailability[IgnoringUnitTestEnv](
        availabilityStore = availabilityStore,
        disseminationProtocolState = disseminationProtocolState,
      )
      loggerFactory.assertLogs(
        availability.receive(
          RemoteDissemination.RemoteBatch.create(WrongBatchId, ABatch, from = Node1)
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message should include("BatchId doesn't match digest")
        },
      )

      disseminationProtocolState.disseminationProgress should be(empty)
      disseminationProtocolState.batchesReadyForOrdering should be(empty)
      disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      verifyZeroInteractions(availabilityStore)
    }

    "not store if too many requests in a batch" in {
      val disseminationProtocolState = new DisseminationProtocolState()

      val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])
      val availability = createAvailability[IgnoringUnitTestEnv](
        availabilityStore = availabilityStore,
        disseminationProtocolState = disseminationProtocolState,
        maxRequestsInBatch = 0,
      )
      loggerFactory.assertLogs(
        availability.receive(
          RemoteDissemination.RemoteBatch.create(ABatchId, ABatch, from = Node1)
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message should include(
            "Batch BatchId(SHA-256:f9fbd79100fb...) from 'node1' contains more requests (1) than allowed (0), skipping"
          )
        },
      )

      disseminationProtocolState.disseminationProgress should be(empty)
      disseminationProtocolState.batchesReadyForOrdering should be(empty)
      disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      verifyZeroInteractions(availabilityStore)
    }

    "not store if batch is expired or too far in the future" in {
      val disseminationProtocolState = new DisseminationProtocolState()

      val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])
      val initialEpochNumber = EpochNumber(OrderingRequestBatch.BatchValidityDurationEpochs + 1L)
      val availability = createAvailability[IgnoringUnitTestEnv](
        availabilityStore = availabilityStore,
        disseminationProtocolState = disseminationProtocolState,
        initialEpochNumber = initialEpochNumber,
      )

      loggerFactory.assertLogs(
        availability.receive(
          RemoteDissemination.RemoteBatch.create(ABatchId, ABatch, from = Node1)
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message should include(
            "Batch BatchId(SHA-256:f9fbd79100fb...) from 'node1' contains an expired batch at epoch number 0 which is 500 epochs or more older than last known epoch 501, skipping"
          )
        },
      )

      val tooFarInTheFutureBatch = OrderingRequestBatch.create(
        Seq(anOrderingRequest),
        EpochNumber(initialEpochNumber + OrderingRequestBatch.BatchValidityDurationEpochs * 2),
      )

      loggerFactory.assertLogs(
        availability.receive(
          RemoteDissemination.RemoteBatch
            .create(BatchId.from(tooFarInTheFutureBatch), tooFarInTheFutureBatch, from = Node1)
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message should include(
            "Batch BatchId(SHA-256:c8c74ab985cb...) from 'node1' contains a batch whose epoch number 1501 is too far in the future compared to last known epoch 501, skipping"
          )
        },
      )

      disseminationProtocolState.disseminationProgress should be(empty)
      disseminationProtocolState.batchesReadyForOrdering should be(empty)
      disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      verifyZeroInteractions(availabilityStore)
    }
  }

  "it receives Dissemination.RemoteBatchStored (from local store)" should {

    "just acknowledge the originating node" in {
      val disseminationProtocolState = new DisseminationProtocolState()
      val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]

      val myId = Node0
      val availability = createAvailability[IgnoringUnitTestEnv](
        myId = myId,
        disseminationProtocolState = disseminationProtocolState,
        cryptoProvider = cryptoProvider,
      )
      availability.receive(
        LocalDissemination.RemoteBatchStored(ABatchId, anEpochNumber, from = Node1)
      )

      disseminationProtocolState.disseminationProgress should be(empty)
      disseminationProtocolState.batchesReadyForOrdering should be(empty)
      disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      verify(cryptoProvider).signHash(
        AvailabilityAck.hashFor(ABatchId, anEpochNumber, myId)
      )
    }
  }

  "it receives Dissemination.RemoteBatchStoredSigned" should {

    "just acknowledge the originating node" in {
      val disseminationProtocolState = new DisseminationProtocolState()
      val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)

      implicit val context
          : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext
      val availability = createAvailability[ProgrammableUnitTestEnv](
        p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
        disseminationProtocolState = disseminationProtocolState,
        cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
      )
      val signature = Signature.noSignature
      availability.receive(
        LocalDissemination.RemoteBatchStoredSigned(ABatchId, from = Node1, signature)
      )

      p2pNetworkOutCell.get() shouldBe None

      context.runPipedMessagesAndReceiveOnModule(availability)

      p2pNetworkOutCell.get() should contain(
        P2PNetworkOut.Multicast(
          P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
            RemoteDissemination.RemoteBatchAcknowledged
              .create(ABatchId, Node0, signature)
              .fakeSign
          ),
          Set(Node1),
        )
      )
    }
  }

  "it receives one Dissemination.RemoteBatchAcknowledged (from node) but " +
    "the batch is not being disseminated [anymore]" should {

      "do nothing" in {
        val disseminationProtocolState = new DisseminationProtocolState()

        val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
        val availability = createAvailability[IgnoringUnitTestEnv](
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        val msg = remoteBatchAcknowledged(idx = 1)
        availability.receive(msg)

        disseminationProtocolState.disseminationProgress should be(empty)
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
        verifyZeroInteractions(cryptoProvider)
      }
    }

  "F == 0 (i.e., proof of availability is complete), " +
    "it receives one Dissemination.RemoteBatchAcknowledged from node, " +
    "the batch is being disseminated, " +
    "there are no consensus requests" should {

      "ignore the ACK" in {
        val disseminationProtocolState = new DisseminationProtocolState()

        disseminationProtocolState.disseminationProgress.addOne(
          ABatchDisseminationProgressNode0And1WithNode0Vote
        )
        val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
        val availability = createAvailability[IgnoringUnitTestEnv](
          otherNodes = Set(Node1),
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        val msg = remoteBatchAcknowledged(idx = 1)
        availability.receive(msg)

        verify(cryptoProvider).verifySignature(
          AvailabilityAck.hashFor(msg.batchId, anEpochNumber, msg.from),
          msg.from,
          msg.signature,
        )

        availability.receive(
          LocalDissemination.RemoteBatchAcknowledgeVerified(msg.batchId, msg.from, msg.signature)
        )

        disseminationProtocolState.disseminationProgress should be(empty)
        disseminationProtocolState.batchesReadyForOrdering should
          contain only BatchReadyForOrderingNode0And1Votes
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      }
    }

  "F > 0, " +
    "it receives `>= quorum-1` Dissemination.RemoteBatchAcknowledged from node " +
    "(i.e., proof of availability is complete), " +
    "the batch is being disseminated and " +
    "there are no consensus requests" should {

      "reset dissemination progress and " +
        "mark the batch ready for ordering" in {
          val disseminationProtocolState = new DisseminationProtocolState()

          disseminationProtocolState.disseminationProgress.addOne(
            ABatchDisseminationProgressNode0To3WithNode0Vote
          )
          val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
          val availability = createAvailability[IgnoringUnitTestEnv](
            otherNodes = Node1To3,
            cryptoProvider = cryptoProvider,
            disseminationProtocolState = disseminationProtocolState,
          )
          QuorumAcksForNode0To3.tail.foreach { quorumAck =>
            availability.receive(quorumAck)
            verify(cryptoProvider).verifySignature(
              AvailabilityAck.hashFor(quorumAck.batchId, anEpochNumber, quorumAck.from),
              quorumAck.from,
              quorumAck.signature,
            )
          }

          QuorumAcksForNode0To3.tail.foreach { quorumAck =>
            availability.receive(
              LocalDissemination.RemoteBatchAcknowledgeVerified(
                quorumAck.batchId,
                quorumAck.from,
                quorumAck.signature,
              )
            )
          }

          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.batchesReadyForOrdering should
            contain only BatchReadyForOrdering4NodesQuorumVotes
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)
        }
    }

  "F > 0, " +
    "it receives `< quorum-1` Dissemination.RemoteBatchAcknowledged from nodes " +
    "(i.e., proof of availability is incomplete), " +
    "the batch is being disseminated, " +
    "there are no consensus requests" should {

      "just update dissemination progress" in {
        val disseminationProtocolState = new DisseminationProtocolState()

        disseminationProtocolState.disseminationProgress.addOne(
          ABatchDisseminationProgressNode0To6WithNode0Vote
        )
        val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
        val availability = createAvailability[IgnoringUnitTestEnv](
          otherNodes = Node1To6,
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        NonQuorumAcksForNode0To6.tail.foreach { quorumAck =>
          availability.receive(quorumAck)
          verify(cryptoProvider).verifySignature(
            AvailabilityAck.hashFor(quorumAck.batchId, anEpochNumber, quorumAck.from),
            quorumAck.from,
            quorumAck.signature,
          )
        }

        NonQuorumAcksForNode0To6.tail.foreach { quorumAck =>
          availability.receive(
            LocalDissemination.RemoteBatchAcknowledgeVerified(
              quorumAck.batchId,
              quorumAck.from,
              quorumAck.signature,
            )
          )
        }

        disseminationProtocolState.disseminationProgress should
          contain only ABatchDisseminationProgressNode0To6WithNonQuorumVotes
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      }
    }

  "F == 0 (i.e., proof of availability is complete), " +
    "it receives Dissemination.RemoteBatchAcknowledged from node, " +
    "the batch is being disseminated and " +
    "there are consensus requests" should {

      "reset dissemination progress and " +
        "send proposal to local consensus" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          disseminationProtocolState.disseminationProgress.addOne(
            ABatchDisseminationProgressNode0And1WithNode0Vote
          )
          disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
          val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
          val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
          val availability = createAvailability[IgnoringUnitTestEnv](
            otherNodes = Set(Node1),
            cryptoProvider = cryptoProvider,
            consensus = fakeCellModule(consensusCell),
            disseminationProtocolState = disseminationProtocolState,
          )
          val msg = remoteBatchAcknowledged(idx = 1)
          availability.receive(msg)
          verify(cryptoProvider).verifySignature(
            AvailabilityAck.hashFor(msg.batchId, anEpochNumber, msg.from),
            msg.from,
            msg.signature,
          )

          availability.receive(
            LocalDissemination.RemoteBatchAcknowledgeVerified(msg.batchId, msg.from, msg.signature)
          )

          consensusCell.get() should contain(ABatchProposalNode0And1Votes)
          disseminationProtocolState.batchesReadyForOrdering should not be empty
          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)

          availability.receive(Availability.Consensus.Ordered(Seq(ABatchId)))
          disseminationProtocolState.batchesReadyForOrdering should be(empty)
        }
    }

  "F > 0, " +
    "it receives `>= quorum-1` Dissemination.RemoteBatchAcknowledged from nodes " +
    "(i.e., proof of availability is complete), " +
    "the batch is being disseminated, " +
    "there are consensus requests" should {

      "reset dissemination progress and " +
        "send proposal to local consensus" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          disseminationProtocolState.disseminationProgress.addOne(
            ABatchDisseminationProgressNode0To3WithNode0Vote
          )
          disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
          val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
          val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
          val availability = createAvailability[IgnoringUnitTestEnv](
            otherNodes = Node1To3,
            cryptoProvider = cryptoProvider,
            consensus = fakeCellModule(consensusCell),
            disseminationProtocolState = disseminationProtocolState,
          )
          QuorumAcksForNode0To3.tail.foreach { quorumAck =>
            availability.receive(quorumAck)
            verify(cryptoProvider).verifySignature(
              AvailabilityAck.hashFor(quorumAck.batchId, anEpochNumber, quorumAck.from),
              quorumAck.from,
              quorumAck.signature,
            )
            availability.receive(
              LocalDissemination.RemoteBatchAcknowledgeVerified(
                quorumAck.batchId,
                quorumAck.from,
                quorumAck.signature,
              )
            )
          }

          consensusCell.get() should contain(ABatchProposal4NodesQuorumVotes)
          disseminationProtocolState.batchesReadyForOrdering should not be empty
          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)

          availability.receive(Availability.Consensus.Ordered(Seq(ABatchId)))
          disseminationProtocolState.batchesReadyForOrdering should be(empty)
        }
    }

  "F > 0, " +
    "it receives `< quorum-1` Dissemination.RemoteBatchAcknowledged from nodes " +
    "(i.e., proof of availability is incomplete), " +
    "the batch is being disseminated, " +
    "there are consensus requests" should {

      "just update dissemination progress" in {
        val disseminationProtocolState = new DisseminationProtocolState()
        disseminationProtocolState.disseminationProgress.addOne(
          ABatchDisseminationProgressNode0To6WithNode0Vote
        )
        disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
        val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
        val availability = createAvailability[IgnoringUnitTestEnv](
          otherNodes = Node1To6,
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        NonQuorumAcksForNode0To6.tail.foreach { quorumAck =>
          availability.receive(quorumAck)

          verify(cryptoProvider).verifySignature(
            AvailabilityAck.hashFor(quorumAck.batchId, anEpochNumber, quorumAck.from),
            quorumAck.from,
            quorumAck.signature,
          )

          availability.receive(
            LocalDissemination.RemoteBatchAcknowledgeVerified(
              quorumAck.batchId,
              quorumAck.from,
              quorumAck.signature,
            )
          )
        }

        disseminationProtocolState.disseminationProgress should
          contain only ABatchDisseminationProgressNode0To6WithNonQuorumVotes
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should contain only AToBeProvidedToConsensus
      }
    }
}
