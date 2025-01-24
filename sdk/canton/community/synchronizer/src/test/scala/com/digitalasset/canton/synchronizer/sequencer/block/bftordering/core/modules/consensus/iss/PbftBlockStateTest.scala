// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.PbftBlockState.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation.PbftMessageValidator
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  PrepareCertificate,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PbftNetworkMessage,
  PrePrepare,
  Prepare,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusStatus
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.SequencerId
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level.{INFO, WARN}

import java.time.Duration

class PbftBlockStateTest extends AsyncWordSpec with BftSequencerBaseTest {

  import PbftBlockStateTest.*

  private val clock = new SimClock(loggerFactory = loggerFactory)

  "PbftBlockState" should {
    "store only valid PrePrepares" in {
      val blockState = createBlockState()

      // PrePrepare from invalid peer; ignored
      val wrongLeaderPP = createPrePrepare(otherPeers.head)
      assertLogs(
        blockState
          .processMessage(wrongLeaderPP),
        log => {
          log.level shouldBe WARN
          log.message should include("wrong peer")
        },
      ) shouldBe false

      // Valid PrePrepare
      assertNoLogs(blockState.processMessage(prePrepare)) shouldBe true

      // Duplicate PrePrepare; ignored
      assertLogs(
        blockState.processMessage(prePrepare),
        log => {
          log.level shouldBe INFO
          log.message should include("already exists")
        },
      ) shouldBe false
    }

    "store only valid Prepares" in {
      val blockState = createBlockState()

      // Valid Prepare
      assertNoLogs(blockState.processMessage(createPrepare(myId))) shouldBe true

      // Duplicate Prepare (same hash)
      assertLogs(
        blockState.processMessage(createPrepare(myId)),
        log => {
          log.level shouldBe INFO
          log.message should include("matching hash")
        },
      ) shouldBe false

      // Prepare already exists (conflicting hash)
      assertLogs(
        blockState.processMessage(createPrepare(myId, wrongHash)),
        log => {
          log.level shouldBe WARN
          log.message should include("different hash")
        },
      ) shouldBe false
    }

    "store only valid Commits" in {
      val blockState = createBlockState()

      // Valid Commit
      assertNoLogs(blockState.processMessage(createCommit(myId))) shouldBe true

      // Duplicate Commit (same hash)
      assertLogs(
        blockState.processMessage(createCommit(myId)),
        log => {
          log.level shouldBe INFO
          log.message should include("matching hash")
        },
      ) shouldBe false

      // Commit already exists (conflicting hash)
      assertLogs(
        blockState.processMessage(createCommit(myId, wrongHash)),
        log => {
          log.level shouldBe WARN
          log.message should include("different hash")
        },
      ) shouldBe false
    }

    "complete block with only one node" in {
      val blockState = createBlockState()

      // With N=1, a PrePrepare should complete the entire block
      assertNoLogs(blockState.processMessage(prePrepare)) shouldBe true
      val processResults = blockState.advance()
      val prepare = createPrepare(myId)
      processResults should contain theSameElementsInOrderAs List(
        SendPbftMessage(prePrepare, store = Some(StorePrePrepare(prePrepare))),
        SignPbftMessage(prepare.message),
      )

      assertNoLogs(blockState.processMessage(prepare)) shouldBe true
      blockState.advance() should contain theSameElementsInOrderAs List(
        SendPbftMessage(prepare, None)
      )

      blockState.confirmPrePrepareStored()
      blockState.advance() should contain theSameElementsInOrderAs List(
        SignPbftMessage(createCommit(myId).message)
      )

      assertNoLogs(blockState.processMessage(createCommit(myId))) shouldBe true
      blockState.advance() should contain theSameElementsInOrderAs List(
        SendPbftMessage(createCommit(myId), Some(StorePrepares(Seq(prepare))))
      )

      blockState.confirmPreparesStored()
      blockState.advance() shouldBe empty
      blockState.commitCertificate should contain(
        CommitCertificate(prePrepare, Seq(createCommit(myId)))
      )
    }

    (2 to 16).foreach { n =>
      s"complete block w/ N=$n as leader" in {
        val peers = (1 until n).map { index =>
          fakeSequencerId(
            s"peer$index"
          )
        }.toSet
        val membership = Membership(myId, peers)
        val blockState = createBlockState(peers)

        // PrePrepare from leader (self)
        assertNoLogs(blockState.processMessage(prePrepare)) shouldBe true
        val prePrepareResult = blockState.advance()
        prePrepareResult should contain theSameElementsInOrderAs List(
          SendPbftMessage(prePrepare, store = Some(StorePrePrepare(prePrepare))),
          SignPbftMessage(createPrepare(myId).message),
        )
        assertNoLogs(blockState.processMessage(createPrepare(myId))) shouldBe true
        blockState.advance() should contain theSameElementsInOrderAs List(
          SendPbftMessage(createPrepare(myId), store = None)
        )

        val myPrepare = createPrepare(myId)
        val peerPrepares = peers.map(createPrepare(_))
        val myCommit = createCommit(myId)
        val peerCommits = peers.map(createCommit(_))

        // Receive all but one needed Prepare to make progress, and ensure advance() is empty;
        // In this case, that is StrongQuorum - 2 Prepares (already have local Prepare)
        peerPrepares.tail.take(membership.orderingTopology.strongQuorum - 2).foreach { prepare =>
          assertNoLogs(blockState.processMessage(prepare)) shouldBe true
        }
        blockState.advance() shouldBe empty

        // Prepare from last peer
        peerPrepares.headOption.foreach { prepare =>
          assertNoLogs(blockState.processMessage(prepare)) shouldBe true
        }
        blockState.advance() shouldBe empty

        // Advance should reach threshold; local commit is returned
        blockState.confirmPrePrepareStored()

        blockState.advance() should contain theSameElementsInOrderAs List(
          SignPbftMessage(createCommit(myId, ppHash).message)
        )
        assertNoLogs(blockState.processMessage(createCommit(myId, ppHash))) shouldBe true

        val prepareResult = blockState.advance()
        inside(prepareResult) {
          case List(SendPbftMessage(commit, Some(StorePrepares(preparesToBeStored)))) =>
            commit shouldBe createCommit(myId, ppHash)
            preparesToBeStored should contain theSameElementsAs (peerPrepares.take(
              membership.orderingTopology.strongQuorum - 1
            ) + myPrepare)
        }

        // Receive all but one needed Commit to make progress, and ensure advance() is empty;
        // In this case, that is StrongQuorum - 2 Commits (already have local Commit)
        peerCommits.tail.take(membership.orderingTopology.strongQuorum - 2).foreach { commit =>
          assertNoLogs(blockState.processMessage(commit)) shouldBe true
        }
        blockState.advance() shouldBe empty

        // Commit from last peer
        peerCommits.headOption.foreach { commit =>
          assertNoLogs(blockState.processMessage(commit)) shouldBe true
        }

        blockState.advance() shouldBe empty
        blockState.confirmPreparesStored()

        // Advance should reach threshold; block is complete
        blockState.advance() shouldBe empty
        inside(blockState.commitCertificate) { case Some(commitCertificate) =>
          commitCertificate.prePrepare shouldBe (prePrepare)
          commitCertificate.commits should contain theSameElementsAs
            (peerCommits.take(membership.orderingTopology.strongQuorum - 1) + myCommit)
        }
        blockState.isBlockComplete shouldBe true
      }
    }

    (2 to 16).foreach { n =>
      s"complete block w/ N=$n as follower" in {
        val peers = (1 until n).map { index =>
          fakeSequencerId(s"peer$index")
        }
        val leader = peers.head
        val membership = Membership(myId, peers.toSet)
        val blockState = createBlockState(peers.toSet, leader)

        // PrePrepare from leader (peer1)
        val pp = createPrePrepare(leader)
        val hash = pp.message.hash

        val myPrepare = createPrepare(myId, hash)
        val peerPrepares = peers.map(createPrepare(_, hash))
        val myCommit = createCommit(myId, hash)
        val peerCommits = peers.map(createCommit(_, hash))

        // Processing PrePrepare should result in local Prepare
        assertNoLogs(blockState.processMessage(pp)) shouldBe true
        blockState.advance() should contain theSameElementsInOrderAs List(
          SignPbftMessage(myPrepare.message)
        )
        assertNoLogs(blockState.processMessage(myPrepare)) shouldBe true
        blockState.advance() should contain theSameElementsInOrderAs List(
          SendPbftMessage(myPrepare, store = Some(StorePrePrepare(pp)))
        )

        // Receive all but one needed Prepare to make progress, and ensure advance() is empty;
        // In this case, that is StrongQuorum - 2 Prepares (already have local Prepare)
        peerPrepares.tail.take(membership.orderingTopology.strongQuorum - 2).foreach { prepare =>
          assertNoLogs(blockState.processMessage(prepare)) shouldBe true
        }
        blockState.advance() shouldBe empty

        // Prepare from the last peer (the leader in this case)
        peerPrepares.headOption.foreach { prepare =>
          assertNoLogs(blockState.processMessage(prepare)) shouldBe true
        }

        blockState.advance() shouldBe empty
        blockState.confirmPrePrepareStored()

        blockState.advance() should contain theSameElementsInOrderAs List(
          SignPbftMessage(myCommit.message)
        )

        assertNoLogs(blockState.processMessage(myCommit)) shouldBe true
        inside(blockState.advance()) {
          case List(SendPbftMessage(commit, Some(StorePrepares(preparesToBeStored)))) =>
            commit shouldBe myCommit
            val expectedPrepares =
              (myPrepare +: peerPrepares).take(membership.orderingTopology.strongQuorum)
            preparesToBeStored should contain theSameElementsAs expectedPrepares
        }

        // Receive all but one needed Commit to make progress, and ensure advance() is empty;
        // In this case, that is StrongQuorum - 2 Commits (already have local Commit)
        peerCommits.tail.take(membership.orderingTopology.strongQuorum - 2).foreach { commit =>
          assertNoLogs(blockState.processMessage(commit)) shouldBe true
        }
        blockState.advance() shouldBe empty

        // Final commit from leader should complete the block
        peerCommits.headOption.foreach { commit =>
          assertNoLogs(blockState.processMessage(commit)) shouldBe true
        }

        blockState.advance() shouldBe empty
        blockState.confirmPreparesStored()

        // Advance should reach threshold; block is complete
        blockState.advance() shouldBe empty
        inside(blockState.commitCertificate) { case Some(commitCertificate) =>
          commitCertificate.prePrepare shouldBe (pp)
          commitCertificate.commits should contain theSameElementsAs
            (peerCommits.take(membership.orderingTopology.strongQuorum - 1) :+ myCommit)
        }
        blockState.isBlockComplete shouldBe true
      }
    }

    "not count votes with wrong hash" in {
      val blockState = createBlockState(otherPeers.toSet, myId)

      // PrePrepare from leader (self)
      assertNoLogs(blockState.processMessage(prePrepare)) shouldBe true
      blockState.advance() should contain theSameElementsInOrderAs List(
        SendPbftMessage(prePrepare, store = Some(StorePrePrepare(prePrepare))),
        SignPbftMessage(createPrepare(myId).message),
      )
      blockState.confirmPrePrepareStored()
      assertNoLogs(blockState.processMessage(createPrepare(myId))) shouldBe true
      blockState.advance() should contain theSameElementsInOrderAs List(
        SendPbftMessage(createPrepare(myId), store = None)
      )

      // Prepare with BAD hash (won't count)
      assertNoLogs(blockState.processMessage(createPrepare(otherPeer1, wrongHash))) shouldBe true
      suppressProblemLogs(blockState.advance()) shouldBe empty

      // Prepare with GOOD hash. Normally now we should have enough votes to create a commit, but the bad hash vote did not count
      assertNoLogs(blockState.processMessage(createPrepare(otherPeer2))) shouldBe true
      suppressProblemLogs(blockState.advance()) shouldBe empty

      // Prepare with GOOD hash. Now we can commit
      assertNoLogs(blockState.processMessage(createPrepare(otherPeer3))) shouldBe true
      suppressProblemLogs(blockState.advance()) should contain theSameElementsInOrderAs List(
        SignPbftMessage(createCommit(myId).message)
      )

      assertNoLogs(blockState.processMessage(createCommit(myId))) shouldBe true
      inside(blockState.advance()) {
        case List(SendPbftMessage(commit, Some(StorePrepares(preparesToBeStored)))) =>
          commit shouldBe createCommit(myId)
          preparesToBeStored should contain theSameElementsAs List(
            createPrepare(
              otherPeer1,
              wrongHash,
            ), // probably dont need to store the ones with wrong hashes
            createPrepare(otherPeer2),
            createPrepare(otherPeer3),
            createPrepare(myId),
          )
      }
      blockState.confirmPreparesStored()

      // Commit with BAD hash (won't count)
      assertNoLogs(blockState.processMessage(createCommit(otherPeer1, wrongHash))) shouldBe true
      suppressProblemLogs(blockState.advance()) shouldBe empty

      // Commit with GOOD hash. Normally now we should have enough votes to complete the block, but the vote with bad hash did not count
      assertNoLogs(blockState.processMessage(createCommit(otherPeer2))) shouldBe true
      suppressProblemLogs(blockState.advance()) shouldBe empty
      blockState.isBlockComplete shouldBe false

      // Commit with GOOD hash. Now block can be completed.
      assertNoLogs(blockState.processMessage(createCommit(otherPeer3))) shouldBe true
      suppressProblemLogs(blockState.advance()) shouldBe empty

      blockState.commitCertificate should contain(
        CommitCertificate(
          prePrepare,
          Seq(createCommit(otherPeer2), createCommit(otherPeer3), createCommit(myId)),
        )
      )
      blockState.isBlockComplete shouldBe true
    }

    "do not set PrePrepare after failed validation" in {
      val leader = otherPeers.head
      val blockState = createBlockState(
        otherPeers.toSet,
        leader,
        pbftMessageValidator = (_: PrePrepare, _: Boolean) => Left("validation failure"),
      )

      // Receive a PrePrepare as a follower
      val pp = createPrePrepare(leader)

      assertLogs(
        blockState.processMessage(pp),
        log => {
          log.level shouldBe WARN
          log.message should include("validation failure")
        },
      ) shouldBe false
      val incompleteVotes = List.fill(4)(false)
      blockState.status shouldBe ConsensusStatus.BlockStatus.InProgress(
        prePrepared = false,
        incompleteVotes,
        incompleteVotes,
      )
    }

    "complete block as leader with out-of-order Pbft messages" in {
      val blockState = createBlockState(otherPeers.toSet, myId)
      val myPrepare = createPrepare(myId)
      val myCommit = createCommit(myId)

      // As a leader, the PrePrepare must always come first
      assertNoLogs(blockState.processMessage(prePrepare)) shouldBe true
      blockState.advance() should contain theSameElementsInOrderAs List(
        SendPbftMessage(prePrepare, store = Some(StorePrePrepare(prePrepare))),
        SignPbftMessage(myPrepare.message),
      )
      assertNoLogs(blockState.processMessage(myPrepare)) shouldBe true
      blockState.advance() should contain theSameElementsInOrderAs List(
        SendPbftMessage(myPrepare, store = None)
      )
      blockState.confirmPrePrepareStored()

      // Receive some commits first, but not enough yet to complete the block
      otherPeers.dropRight(1).foreach { peer =>
        assertNoLogs(blockState.processMessage(createCommit(peer))) shouldBe true
        blockState.advance() shouldBe empty
      }

      // Receive one prepare from a peer, which should be one short of threshold to send a local commit
      assertNoLogs(blockState.processMessage(createPrepare(otherPeers.head))) shouldBe true
      blockState.advance() shouldBe empty

      // Receive the final prepare that should complete the block
      assertNoLogs(blockState.processMessage(createPrepare(otherPeer2))) shouldBe true
      blockState.advance() should contain theSameElementsInOrderAs List(
        SignPbftMessage(myCommit.message)
      )

      assertNoLogs(blockState.processMessage(myCommit)) shouldBe true
      inside(blockState.advance()) {
        case List(SendPbftMessage(commit, Some(StorePrepares(preparesToStore)))) =>
          commit shouldBe myCommit
          val expectedPreparesToStore = otherPeers.take(2).map(createPrepare(_)) :+ myPrepare
          preparesToStore should contain theSameElementsAs expectedPreparesToStore
      }

      // Confirm storage of the prepares, which should then enable block completion
      blockState.confirmPreparesStored()
      blockState.advance() shouldBe empty
      blockState.commitCertificate should contain(
        CommitCertificate(
          prePrepare,
          Seq(createCommit(otherPeer1), createCommit(otherPeer2), myCommit),
        )
      )
    }

    "complete block as follower with out-of-order Pbft messages" in {
      val leader = otherPeers.head
      val blockState = createBlockState(otherPeers.toSet, leader)

      // As a follower, receive the PrePrepare and send a Prepare
      val pp = createPrePrepare(leader)
      val hash = pp.message.hash
      val myPrepare = createPrepare(myId, hash)
      val myCommit = createCommit(myId, hash)

      assertNoLogs(blockState.processMessage(pp)) shouldBe true
      blockState.advance() should contain theSameElementsInOrderAs List(
        SignPbftMessage(myPrepare.message)
      )
      assertNoLogs(blockState.processMessage(myPrepare)) shouldBe true
      blockState.advance() should contain theSameElementsInOrderAs List(
        SendPbftMessage(myPrepare, store = Some(StorePrePrepare(pp)))
      )
      blockState.confirmPrePrepareStored()

      // Receive some commits first, but not enough yet to complete the block
      otherPeers.dropRight(1).foreach { peer =>
        assertNoLogs(blockState.processMessage(createCommit(peer, hash))) shouldBe true
        blockState.advance() shouldBe empty
      }

      // Receive another prepare from a peer, which should be one short of threshold to send a local commit
      assertNoLogs(blockState.processMessage(createPrepare(otherPeers.head, hash))) shouldBe true
      blockState.advance() shouldBe empty

      // Receive the final prepare that should result in a local commit sent
      assertNoLogs(blockState.processMessage(createPrepare(otherPeer2, hash))) shouldBe true
      blockState.advance() should contain theSameElementsInOrderAs List(
        SignPbftMessage(myCommit.message)
      )
      assertNoLogs(blockState.processMessage(myCommit)) shouldBe true
      inside(blockState.advance()) {
        case List(SendPbftMessage(commit, Some(StorePrepares(preparesToStore)))) =>
          commit shouldBe myCommit
          val expectedPreparesToStore =
            Seq(myPrepare, createPrepare(otherPeer2, hash), createPrepare(otherPeers.head, hash))
          preparesToStore should contain theSameElementsAs expectedPreparesToStore
      }

      // Confirm storage of the prepares, which should then enable block completion
      blockState.confirmPreparesStored()

      blockState.advance() shouldBe empty
      blockState.commitCertificate should contain(
        CommitCertificate(
          pp,
          Seq(
            createCommit(otherPeer1, hash),
            createCommit(otherPeer2, hash),
            myCommit,
          ),
        )
      )
    }

    "complete block as leader with only PrePrepare and non-local Commits" in {
      val blockState = createBlockState(otherPeers.toSet, myId)
      val myPrepare = createPrepare(myId)

      // As a leader, the PrePrepare must always come first
      assertNoLogs(blockState.processMessage(prePrepare)) shouldBe true
      blockState.advance() shouldBe List(
        SendPbftMessage(prePrepare, store = Some(StorePrePrepare(prePrepare))),
        SignPbftMessage(myPrepare.message),
      )
      blockState.confirmPrePrepareStored()

      assertNoLogs(blockState.processMessage(myPrepare)) shouldBe true
      blockState.advance() shouldBe List(
        SendPbftMessage(myPrepare, store = None)
      )

      // Receive commits from all but the last peer, which should not complete the block
      otherPeers.dropRight(1).foreach { peer =>
        assertNoLogs(blockState.processMessage(createCommit(peer))) shouldBe true
        blockState.advance() shouldBe empty
      }

      // Receive a commit from the last peer, which should be a strong quorum of non-local commits
      otherPeers.takeRight(1).foreach { peer =>
        assertNoLogs(blockState.processMessage(createCommit(peer))) shouldBe true
        blockState.advance() shouldBe empty
        blockState.commitCertificate should contain(
          CommitCertificate(
            prePrepare,
            Seq(createCommit(otherPeer1), createCommit(otherPeer2), createCommit(otherPeer3)),
          )
        )
      }

      // Finish receiving a quorum of prepares and assert that the local commit action is NOT fired
      otherPeers.dropRight(1).foreach { peer =>
        assertNoLogs(blockState.processMessage(createPrepare(peer))) shouldBe true
      }
      blockState.advance() shouldBe empty
    }

    "complete block as follower with only PrePrepare and non-local Commits" in {
      val leader = otherPeers.head
      val blockState = createBlockState(otherPeers.toSet, leader)

      // As a follower, receive the PrePrepare and send a Prepare
      val pp = createPrePrepare(leader)
      val hash = pp.message.hash
      val myPrepare = createPrepare(myId, hash)

      assertNoLogs(blockState.processMessage(pp)) shouldBe true
      blockState.advance() should contain theSameElementsInOrderAs List(
        SignPbftMessage(myPrepare.message)
      )
      blockState.confirmPrePrepareStored()

      assertNoLogs(blockState.processMessage(myPrepare)) shouldBe true
      blockState.advance() should contain theSameElementsInOrderAs List(
        SendPbftMessage(myPrepare, store = Some(StorePrePrepare(pp)))
      )

      // Receive commits from all but the last peer, which should not complete the block
      otherPeers.dropRight(1).foreach { peer =>
        assertNoLogs(blockState.processMessage(createCommit(peer, hash))) shouldBe true
        blockState.advance() shouldBe empty
      }

      // Receive a commit from the last peer, which should be a strong quorum of non-local commits
      otherPeers.takeRight(1).foreach { peer =>
        assertNoLogs(blockState.processMessage(createCommit(peer, hash))) shouldBe true
        blockState.advance() shouldBe empty
        blockState.commitCertificate should contain(
          CommitCertificate(
            pp,
            Seq(
              createCommit(otherPeer1, hash),
              createCommit(otherPeer2, hash),
              createCommit(otherPeer3, hash),
            ),
          )
        )
      }

      // Finish receiving a quorum of prepares and assert that the local commit action is NOT fired
      otherPeers.dropRight(1).foreach { peer =>
        assertNoLogs(blockState.processMessage(createPrepare(peer, hash))) shouldBe true
      }
      blockState.advance() shouldBe empty
    }

    "produce correct consensus certificate" in {
      val blockState = createBlockState(otherPeers.toSet, myId)
      val myPrepare = createPrepare(myId)
      val myCommit = createCommit(myId)

      assertNoLogs(blockState.processMessage(prePrepare)) shouldBe true
      blockState.advance()
      blockState.prepareCertificate shouldBe None
      blockState.commitCertificate shouldBe None
      blockState.confirmPrePrepareStored()

      val prepare1 = createPrepare(otherPeer1)
      blockState.processMessage(prepare1)
      blockState.advance()
      blockState.prepareCertificate shouldBe None
      blockState.commitCertificate shouldBe None

      val prepare2 = createPrepare(otherPeer2)
      blockState.processMessage(prepare2)
      blockState.advance()

      blockState.prepareCertificate shouldBe None
      blockState.commitCertificate shouldBe None
      blockState.processMessage(myPrepare)
      blockState.advance()
      blockState.confirmPreparesStored()

      val prepareCert = blockState.prepareCertificate
      prepareCert shouldBe Some(PrepareCertificate(prePrepare, Seq(prepare1, prepare2, myPrepare)))
      blockState.commitCertificate shouldBe None

      val commit1 = createCommit(otherPeer1)
      blockState.processMessage(commit1)
      blockState.advance()
      blockState.commitCertificate shouldBe None

      val commit2 = createCommit(otherPeer2)
      blockState.processMessage(commit2)
      blockState.advance()
      blockState.commitCertificate shouldBe None

      blockState.processMessage(myCommit)
      blockState.advance()
      blockState.commitCertificate shouldBe Some(
        CommitCertificate(prePrepare, Seq(commit1, commit2, myCommit))
      )
    }

    "be able to restore prepare before pre-prepare" in {
      val blockState = createBlockState(Set(fakeSequencerId("peer1"), fakeSequencerId("peer2")))
      clock.advance(Duration.ofMinutes(5))

      val prepare = createPrepare(myId)

      assertNoLogs(blockState.processMessage(prepare)) shouldBe true
      val processResults1 = assertNoLogs(blockState.advance())
      processResults1 shouldBe empty

      // when processing our pre-prepare we should attempt to create the prepare, but see that it is already there and
      // use the pre-existing one
      assertNoLogs(blockState.processMessage(prePrepare)) shouldBe true
      val processResults2 = assertNoLogs(blockState.advance())
      inside(processResults2) {
        case Seq(
              SendPbftMessage(SignedMessage(_: PrePrepare, _), _),
              SendPbftMessage(SignedMessage(p, _), _),
            ) =>
          // if a new prepare had been created, its timestamp would have been influenced by the clock advancement
          // but that didn't happen, we know the rehydrated prepare was picked
          p.localTimestamp shouldBe CantonTimestamp.Epoch
      }
      clock.reset()
      succeed
    }

    "create status message and messages to retransmit" in {
      val numberOfpeers = 4
      val peers = (1 until numberOfpeers).map { index =>
        fakeSequencerId(
          s"peer$index"
        )
      }.toSet
      val membership = Membership(myId, peers)
      val blockState = createBlockState(peers)
      val strongQuorum = membership.orderingTopology.strongQuorum
      val noProgressBlockStatus = ConsensusStatus.BlockStatus.InProgress(
        prePrepared = false,
        preparesPresent = Seq.fill(numberOfpeers)(false),
        commitsPresent = Seq.fill(numberOfpeers)(false),
      )
      val myPrepare = createPrepare(myId)
      val peerPrepares = peers.toSeq.sorted.map(createPrepare(_))
      val myCommit = createCommit(myId)
      val peerCommits = peers.toSeq.sorted.map(createCommit(_))

      blockState.status shouldBe noProgressBlockStatus

      assertNoLogs(blockState.processMessage(prePrepare)) shouldBe true
      blockState.advance() should contain(SignPbftMessage(myPrepare.message))
      assertNoLogs(blockState.processMessage(myPrepare)) shouldBe true
      blockState.advance()

      blockState.status shouldBe ConsensusStatus.BlockStatus.InProgress(
        prePrepared = true,
        preparesPresent = Seq.fill(numberOfpeers - 1)(false) ++ Seq(true),
        commitsPresent = Seq.fill(numberOfpeers)(false),
      )

      blockState.messagesToRetransmit(noProgressBlockStatus) shouldBe empty

      // only retransmit pre-prepare and local prepare after confirming pre-prepare has been stored
      blockState.confirmPrePrepareStored()
      blockState.advance()
      blockState.messagesToRetransmit(
        noProgressBlockStatus
      ) should contain theSameElementsInOrderAs Seq[SignedMessage[PbftNetworkMessage]](
        prePrepare,
        myPrepare,
      )

      peerPrepares.zipWithIndex.map { case (prepare, index) =>
        assertNoLogs(blockState.processMessage(prepare)) shouldBe true
        blockState.status shouldBe ConsensusStatus.BlockStatus.InProgress(
          prePrepared = true,
          preparesPresent =
            Seq.fill(index + 1)(true) ++ Seq.fill(numberOfpeers - index - 2)(false) ++ Seq(true),
          commitsPresent = Seq.fill(numberOfpeers)(false),
        )

        blockState.messagesToRetransmit(
          noProgressBlockStatus
        ) should contain theSameElementsAs Seq[SignedMessage[PbftNetworkMessage]](
          prePrepare
        ) ++ (peerPrepares.take(index + 1) ++ Seq(myPrepare))
          .take(strongQuorum)
      }

      blockState.advance() should contain(SignPbftMessage(myCommit.message))

      blockState.processMessage(myCommit) shouldBe true
      blockState.advance()

      blockState.status shouldBe ConsensusStatus.BlockStatus.InProgress(
        prePrepared = true,
        preparesPresent = Seq.fill(numberOfpeers)(true),
        commitsPresent = Seq.fill(numberOfpeers - 1)(false) ++ Seq(true),
      )

      // we only retransmit local commit after the prepares were stored
      val noCommitsBlockStatus = ConsensusStatus.BlockStatus.InProgress(
        prePrepared = true,
        preparesPresent = Seq.fill(numberOfpeers)(true),
        commitsPresent = Seq.fill(numberOfpeers)(false),
      )
      blockState.messagesToRetransmit(noCommitsBlockStatus) shouldBe empty
      blockState.confirmPreparesStored()
      blockState.advance() shouldBe empty
      blockState.messagesToRetransmit(noCommitsBlockStatus) should contain only myCommit

      // Receive all but one needed Commit to make progress
      peerCommits.zipWithIndex.take(strongQuorum - 2).foreach { case (commit, index) =>
        assertNoLogs(blockState.processMessage(commit)) shouldBe true
        blockState.status shouldBe ConsensusStatus.BlockStatus.InProgress(
          prePrepared = true,
          preparesPresent = Seq.fill(numberOfpeers)(true),
          commitsPresent =
            Seq.fill(index + 1)(true) ++ Seq.fill(numberOfpeers - index - 2)(false) ++ Seq(true),
        )

        blockState.messagesToRetransmit(
          noCommitsBlockStatus
        ) should contain theSameElementsAs (peerCommits.take(index + 1) ++ Seq(myCommit))
          .take(strongQuorum)
      }
      blockState.advance() shouldBe empty

      assertNoLogs(blockState.processMessage(peerCommits(strongQuorum - 2))) shouldBe true
      blockState.advance() shouldBe empty
      blockState.commitCertificate shouldBe defined

      blockState.status shouldBe ConsensusStatus.BlockStatus.Complete
    }

  }

  private def createBlockState(
      otherPeers: Set[SequencerId] = Set.empty,
      leader: SequencerId = myId,
      pbftMessageValidator: PbftMessageValidator = (_: PrePrepare, _: Boolean) => Right(()),
  ) =
    new PbftBlockState(
      Membership(myId, otherPeers),
      clock,
      pbftMessageValidator,
      leader,
      EpochNumber.First,
      ViewNumber.First,
      firstInSegment = false, // does not matter in these tests
      abort = fail(_),
      SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
      loggerFactory,
    )(MetricsContext.Empty)
}

object PbftBlockStateTest {

  private val myId = fakeSequencerId("self")
  private val otherPeers = (1 to 3).map { index =>
    fakeSequencerId(
      s"peer$index"
    )
  }
  private val otherPeer1 = otherPeers.head
  private val otherPeer2 = otherPeers(1)
  private val otherPeer3 = otherPeers(2)
  private val canonicalCommitSet = CanonicalCommitSet(
    Set(
      createCommit(
        myId,
        Hash.digest(HashPurpose.BftOrderingPbftBlock, ByteString.EMPTY, HashAlgorithm.Sha256),
      )
    )
  )
  private val prePrepare = createPrePrepare(myId)
  private val ppHash = prePrepare.message.hash
  private val wrongHash = Hash.digest(
    HashPurpose.BftOrderingPbftBlock,
    ByteString.copyFromUtf8("bad data"),
    HashAlgorithm.Sha256,
  )

  private def createPrePrepare(p: SequencerId): SignedMessage[PrePrepare] =
    PrePrepare
      .create(
        BlockMetadata.mk(EpochNumber.First, BlockNumber.First),
        ViewNumber.First,
        CantonTimestamp.Epoch,
        OrderingBlock(Seq()),
        canonicalCommitSet,
        from = p,
      )
      .fakeSign

  private def createPrepare(p: SequencerId, hash: Hash = ppHash): SignedMessage[Prepare] =
    Prepare
      .create(
        BlockMetadata.mk(EpochNumber.First, BlockNumber.First),
        ViewNumber.First,
        hash,
        CantonTimestamp.Epoch,
        from = p,
      )
      .fakeSign

  private def createCommit(p: SequencerId, hash: Hash = ppHash): SignedMessage[Commit] =
    Commit
      .create(
        BlockMetadata.mk(EpochNumber.First, BlockNumber.First),
        ViewNumber.First,
        hash,
        CantonTimestamp.Epoch,
        from = p,
      )
      .fakeSign
}
