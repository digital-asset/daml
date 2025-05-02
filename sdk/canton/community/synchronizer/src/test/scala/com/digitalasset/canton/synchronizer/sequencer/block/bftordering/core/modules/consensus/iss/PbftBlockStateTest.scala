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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
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
import com.digitalasset.canton.version.ProtocolVersion
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

      // PrePrepare from invalid node; ignored
      val wrongLeaderPP = createPrePrepare(otherIds.head)
      assertLogs(
        blockState
          .processMessage(wrongLeaderPP),
        log => {
          log.level shouldBe WARN
          log.message should include("wrong node")
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

      // We don't send a local Prepare until the local PrePrepare is stored
      assertNoLogs(blockState.processMessage(prepare)) shouldBe true
      blockState.advance() shouldBe empty

      blockState.confirmPrePrepareStored()
      blockState.advance() should contain theSameElementsInOrderAs List(
        SendPbftMessage(prepare, None),
        SignPbftMessage(createCommit(myId).message),
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
        val nodes = (1 until n).map { index =>
          BftNodeId(s"node$index")
        }.toSet
        val membership = Membership.forTesting(myId, nodes)
        val blockState = createBlockState(nodes)

        // PrePrepare from leader (self)
        assertNoLogs(blockState.processMessage(prePrepare)) shouldBe true
        val prePrepareResult = blockState.advance()
        prePrepareResult should contain theSameElementsInOrderAs List(
          SendPbftMessage(prePrepare, store = Some(StorePrePrepare(prePrepare))),
          SignPbftMessage(createPrepare(myId).message),
        )
        blockState.confirmPrePrepareStored()
        assertNoLogs(blockState.processMessage(createPrepare(myId))) shouldBe true
        blockState.advance() should contain theSameElementsInOrderAs List(
          SendPbftMessage(createPrepare(myId), store = None)
        )

        val myPrepare = createPrepare(myId)
        val nodePrepares = nodes.map(createPrepare(_))
        val myCommit = createCommit(myId)
        val nodeCommits = nodes.map(createCommit(_))

        // Receive all but one needed Prepare to make progress, and ensure advance() is empty;
        // In this case, that is StrongQuorum - 2 Prepares (already have local Prepare)
        nodePrepares.tail.take(membership.orderingTopology.strongQuorum - 2).foreach { prepare =>
          assertNoLogs(blockState.processMessage(prepare)) shouldBe true
        }
        blockState.advance() shouldBe empty

        // Prepare from last node
        nodePrepares.headOption.foreach { prepare =>
          assertNoLogs(blockState.processMessage(prepare)) shouldBe true
        }
        blockState.advance() should contain theSameElementsInOrderAs List(
          SignPbftMessage(createCommit(myId, ppHash).message)
        )
        assertNoLogs(blockState.processMessage(createCommit(myId, ppHash))) shouldBe true

        val prepareResult = blockState.advance()
        inside(prepareResult) {
          case List(SendPbftMessage(commit, Some(StorePrepares(preparesToBeStored)))) =>
            commit shouldBe createCommit(myId, ppHash)
            preparesToBeStored should contain theSameElementsAs (nodePrepares.take(
              membership.orderingTopology.strongQuorum - 1
            ) + myPrepare)
        }

        // Receive all but one needed Commit to make progress, and ensure advance() is empty;
        // In this case, that is StrongQuorum - 2 Commits (already have local Commit)
        nodeCommits.tail.take(membership.orderingTopology.strongQuorum - 2).foreach { commit =>
          assertNoLogs(blockState.processMessage(commit)) shouldBe true
        }
        blockState.advance() shouldBe empty

        // Commit from last node
        nodeCommits.headOption.foreach { commit =>
          assertNoLogs(blockState.processMessage(commit)) shouldBe true
        }

        blockState.advance() shouldBe empty
        blockState.confirmPreparesStored()

        // Advance should reach threshold; block is complete
        blockState.advance() shouldBe empty
        inside(blockState.commitCertificate) { case Some(commitCertificate) =>
          commitCertificate.prePrepare shouldBe prePrepare
          commitCertificate.commits should contain theSameElementsAs
            (nodeCommits.take(membership.orderingTopology.strongQuorum - 1) + myCommit)
        }
        blockState.isBlockComplete shouldBe true
      }
    }

    (2 to 16).foreach { n =>
      s"complete block w/ N=$n as follower" in {
        val nodes = (1 until n).map { index =>
          BftNodeId(s"node$index")
        }
        val leader = nodes.head
        val membership = Membership.forTesting(myId, nodes.toSet)
        val blockState = createBlockState(nodes.toSet, leader)

        // PrePrepare from leader (node1)
        val pp = createPrePrepare(leader)
        val hash = pp.message.hash

        val myPrepare = createPrepare(myId, hash)
        val nodePrepares = nodes.map(createPrepare(_, hash))
        val myCommit = createCommit(myId, hash)
        val nodeCommits = nodes.map(createCommit(_, hash))

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
        nodePrepares.tail.take(membership.orderingTopology.strongQuorum - 2).foreach { prepare =>
          assertNoLogs(blockState.processMessage(prepare)) shouldBe true
        }
        blockState.advance() shouldBe empty

        // Prepare from the last node (the leader in this case)
        nodePrepares.headOption.foreach { prepare =>
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
              (myPrepare +: nodePrepares).take(membership.orderingTopology.strongQuorum)
            preparesToBeStored should contain theSameElementsAs expectedPrepares
        }

        // Receive all but one needed Commit to make progress, and ensure advance() is empty;
        // In this case, that is StrongQuorum - 2 Commits (already have local Commit)
        nodeCommits.tail.take(membership.orderingTopology.strongQuorum - 2).foreach { commit =>
          assertNoLogs(blockState.processMessage(commit)) shouldBe true
        }
        blockState.advance() shouldBe empty

        // Final commit from leader should complete the block
        nodeCommits.headOption.foreach { commit =>
          assertNoLogs(blockState.processMessage(commit)) shouldBe true
        }

        blockState.advance() shouldBe empty
        blockState.confirmPreparesStored()

        // Advance should reach threshold; block is complete
        blockState.advance() shouldBe empty
        inside(blockState.commitCertificate) { case Some(commitCertificate) =>
          commitCertificate.prePrepare shouldBe pp
          commitCertificate.commits should contain theSameElementsAs
            (nodeCommits.take(membership.orderingTopology.strongQuorum - 1) :+ myCommit)
        }
        blockState.isBlockComplete shouldBe true
      }
    }

    "not count votes with wrong hash" in {
      val blockState = createBlockState(otherIds.toSet, myId)

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
      assertNoLogs(blockState.processMessage(createPrepare(otherId1, wrongHash))) shouldBe true
      suppressProblemLogs(blockState.advance()) shouldBe empty

      // Prepare with GOOD hash. Normally now we should have enough votes to create a commit, but the bad hash vote did not count
      assertNoLogs(blockState.processMessage(createPrepare(otherId2))) shouldBe true
      suppressProblemLogs(blockState.advance()) shouldBe empty

      // Prepare with GOOD hash. Now we can commit
      assertNoLogs(blockState.processMessage(createPrepare(otherId3))) shouldBe true
      suppressProblemLogs(blockState.advance()) should contain theSameElementsInOrderAs List(
        SignPbftMessage(createCommit(myId).message)
      )

      assertNoLogs(blockState.processMessage(createCommit(myId))) shouldBe true
      inside(blockState.advance()) {
        case List(SendPbftMessage(commit, Some(StorePrepares(preparesToBeStored)))) =>
          commit shouldBe createCommit(myId)
          preparesToBeStored should contain theSameElementsAs List(
            createPrepare(
              otherId1,
              wrongHash,
            ), // probably dont need to store the ones with wrong hashes
            createPrepare(otherId2),
            createPrepare(otherId3),
            createPrepare(myId),
          )
      }
      blockState.confirmPreparesStored()

      // Commit with BAD hash (won't count)
      assertNoLogs(blockState.processMessage(createCommit(otherId1, wrongHash))) shouldBe true
      suppressProblemLogs(blockState.advance()) shouldBe empty

      // Commit with GOOD hash. Normally now we should have enough votes to complete the block, but the vote with bad hash did not count
      assertNoLogs(blockState.processMessage(createCommit(otherId2))) shouldBe true
      suppressProblemLogs(blockState.advance()) shouldBe empty
      blockState.isBlockComplete shouldBe false

      // Commit with GOOD hash. Now block can be completed.
      assertNoLogs(blockState.processMessage(createCommit(otherId3))) shouldBe true
      suppressProblemLogs(blockState.advance()) shouldBe empty

      blockState.commitCertificate should contain(
        CommitCertificate(
          prePrepare,
          Seq(createCommit(otherId2), createCommit(otherId3), createCommit(myId)),
        )
      )
      blockState.isBlockComplete shouldBe true
    }

    "do not set PrePrepare after failed validation" in {
      val leader = otherIds.head
      val blockState = createBlockState(
        otherIds.toSet,
        leader,
        pbftMessageValidator = (_: PrePrepare) => Left("validation failure"),
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
      val blockState = createBlockState(otherIds.toSet, myId)
      val myPrepare = createPrepare(myId)
      val myCommit = createCommit(myId)

      // As a leader, the PrePrepare must always come first
      assertNoLogs(blockState.processMessage(prePrepare)) shouldBe true
      blockState.advance() should contain theSameElementsInOrderAs List(
        SendPbftMessage(prePrepare, store = Some(StorePrePrepare(prePrepare))),
        SignPbftMessage(myPrepare.message),
      )
      assertNoLogs(blockState.processMessage(myPrepare)) shouldBe true
      blockState.confirmPrePrepareStored()
      blockState.advance() should contain theSameElementsInOrderAs List(
        SendPbftMessage(myPrepare, store = None)
      )

      // Receive some commits first, but not enough yet to complete the block
      otherIds.dropRight(1).foreach { node =>
        assertNoLogs(blockState.processMessage(createCommit(node))) shouldBe true
        blockState.advance() shouldBe empty
      }

      // Receive one prepare from a node, which should be one short of threshold to send a local commit
      assertNoLogs(blockState.processMessage(createPrepare(otherIds.head))) shouldBe true
      blockState.advance() shouldBe empty

      // Receive the final prepare that should complete the block
      assertNoLogs(blockState.processMessage(createPrepare(otherId2))) shouldBe true
      blockState.advance() should contain theSameElementsInOrderAs List(
        SignPbftMessage(myCommit.message)
      )

      assertNoLogs(blockState.processMessage(myCommit)) shouldBe true
      inside(blockState.advance()) {
        case List(SendPbftMessage(commit, Some(StorePrepares(preparesToStore)))) =>
          commit shouldBe myCommit
          val expectedPreparesToStore = otherIds.take(2).map(createPrepare(_)) :+ myPrepare
          preparesToStore should contain theSameElementsAs expectedPreparesToStore
      }

      // Confirm storage of the prepares, which should then enable block completion
      blockState.confirmPreparesStored()
      blockState.advance() shouldBe empty
      blockState.commitCertificate should contain(
        CommitCertificate(
          prePrepare,
          Seq(createCommit(otherId1), createCommit(otherId2), myCommit),
        )
      )
    }

    "complete block as follower with out-of-order Pbft messages" in {
      val leader = otherIds.head
      val blockState = createBlockState(otherIds.toSet, leader)

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
      otherIds.dropRight(1).foreach { node =>
        assertNoLogs(blockState.processMessage(createCommit(node, hash))) shouldBe true
        blockState.advance() shouldBe empty
      }

      // Receive another prepare from a node, which should be one short of threshold to send a local commit
      assertNoLogs(blockState.processMessage(createPrepare(otherIds.head, hash))) shouldBe true
      blockState.advance() shouldBe empty

      // Receive the final prepare that should result in a local commit sent
      assertNoLogs(blockState.processMessage(createPrepare(otherId2, hash))) shouldBe true
      blockState.advance() should contain theSameElementsInOrderAs List(
        SignPbftMessage(myCommit.message)
      )
      assertNoLogs(blockState.processMessage(myCommit)) shouldBe true
      inside(blockState.advance()) {
        case List(SendPbftMessage(commit, Some(StorePrepares(preparesToStore)))) =>
          commit shouldBe myCommit
          val expectedPreparesToStore =
            Seq(myPrepare, createPrepare(otherId2, hash), createPrepare(otherIds.head, hash))
          preparesToStore should contain theSameElementsAs expectedPreparesToStore
      }

      // Confirm storage of the prepares, which should then enable block completion
      blockState.confirmPreparesStored()

      blockState.advance() shouldBe empty
      blockState.commitCertificate should contain(
        CommitCertificate(
          pp,
          Seq(
            createCommit(otherId1, hash),
            createCommit(otherId2, hash),
            myCommit,
          ),
        )
      )
    }

    "complete block as leader with only PrePrepare and non-local Commits" in {
      val blockState = createBlockState(otherIds.toSet, myId)
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

      // Receive commits from all but the last node, which should not complete the block
      otherIds.dropRight(1).foreach { node =>
        assertNoLogs(blockState.processMessage(createCommit(node))) shouldBe true
        blockState.advance() shouldBe empty
      }

      // Receive a commit from the last node, which should be a strong quorum of non-local commits
      otherIds.takeRight(1).foreach { node =>
        assertNoLogs(blockState.processMessage(createCommit(node))) shouldBe true
        blockState.advance() shouldBe empty
        blockState.commitCertificate should contain(
          CommitCertificate(
            prePrepare,
            Seq(createCommit(otherId1), createCommit(otherId2), createCommit(otherId3)),
          )
        )
      }

      // Finish receiving a quorum of prepares and assert that the local commit action is NOT fired
      otherIds.dropRight(1).foreach { node =>
        assertNoLogs(blockState.processMessage(createPrepare(node))) shouldBe true
      }
      blockState.advance() shouldBe empty
    }

    "complete block as follower with only PrePrepare and non-local Commits" in {
      val leader = otherIds.head
      val blockState = createBlockState(otherIds.toSet, leader)

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

      // Receive commits from all but the last node, which should not complete the block
      otherIds.dropRight(1).foreach { node =>
        assertNoLogs(blockState.processMessage(createCommit(node, hash))) shouldBe true
        blockState.advance() shouldBe empty
      }

      // Receive a commit from the last node, which should be a strong quorum of non-local commits
      otherIds.takeRight(1).foreach { node =>
        assertNoLogs(blockState.processMessage(createCommit(node, hash))) shouldBe true
        blockState.advance() shouldBe empty
        blockState.commitCertificate should contain(
          CommitCertificate(
            pp,
            Seq(
              createCommit(otherId1, hash),
              createCommit(otherId2, hash),
              createCommit(otherId3, hash),
            ),
          )
        )
      }

      // Finish receiving a quorum of prepares and assert that the local commit action is NOT fired
      otherIds.dropRight(1).foreach { node =>
        assertNoLogs(blockState.processMessage(createPrepare(node, hash))) shouldBe true
      }
      blockState.advance() shouldBe empty
    }

    "produce correct consensus certificate" in {
      val blockState = createBlockState(otherIds.toSet, myId)
      val myPrepare = createPrepare(myId)
      val myCommit = createCommit(myId)

      assertNoLogs(blockState.processMessage(prePrepare)) shouldBe true
      blockState.advance()
      blockState.prepareCertificate shouldBe None
      blockState.commitCertificate shouldBe None
      blockState.confirmPrePrepareStored()

      val prepare1 = createPrepare(otherId1)
      blockState.processMessage(prepare1)
      blockState.advance()
      blockState.prepareCertificate shouldBe None
      blockState.commitCertificate shouldBe None

      val prepare2 = createPrepare(otherId2)
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

      val commit1 = createCommit(otherId1)
      blockState.processMessage(commit1)
      blockState.advance()
      blockState.commitCertificate shouldBe None

      val commit2 = createCommit(otherId2)
      blockState.processMessage(commit2)
      blockState.advance()
      blockState.commitCertificate shouldBe None

      blockState.processMessage(myCommit)
      blockState.advance()
      blockState.commitCertificate shouldBe Some(
        CommitCertificate(prePrepare, Seq(commit1, commit2, myCommit))
      )
    }

    "as leader, only send local Prepare once PrePrepare is stored" in {
      val blockState = createBlockState(otherIds.toSet, myId)
      val myPrepare = createPrepare(myId)

      // Process the local PrePrepare
      assertNoLogs(blockState.processMessage(prePrepare)) shouldBe true
      blockState.advance() shouldBe List(
        SendPbftMessage(prePrepare, store = Some(StorePrePrepare(prePrepare))),
        SignPbftMessage(myPrepare.message),
      )

      // Before confirming that the PrePrepare is stored, the local Prepare is not sent
      assertNoLogs(blockState.processMessage(myPrepare)) shouldBe true
      blockState.advance() shouldBe empty

      // After confirming the PrePrepare is stored, the send action triggers
      blockState.confirmPrePrepareStored()
      blockState.advance() shouldBe List(
        SendPbftMessage(myPrepare, store = None)
      )
    }

    "as any node, only send local Prepare after view change once NewView is stored" in {
      val blockState = createBlockState(otherIds.toSet, myId, viewNumber = ViewNumber(1L))
      val myPrepare = createPrepare(myId, view = ViewNumber(1L))

      assertNoLogs(blockState.processMessage(prePrepare)) shouldBe true
      blockState.advance() shouldBe List(
        SignPbftMessage(myPrepare.message)
      )

      // Before confirming that the PrePrepare is stored, the local Prepare is not sent
      assertNoLogs(blockState.processMessage(myPrepare)) shouldBe true
      blockState.advance() shouldBe empty

      // After confirming the PrePrepare is stored, the send action triggers
      blockState.confirmPrePrepareStored()
      blockState.advance() shouldBe List(
        SendPbftMessage(myPrepare, store = None)
      )
    }

    "be able to restore prepare before pre-prepare" in {
      val blockState = createBlockState(Set(BftNodeId("node1"), BftNodeId("node2")))
      clock.advance(Duration.ofMinutes(5))

      val prepare = createPrepare(myId)
      val cryptoEvidence = prepare.message.getCryptographicEvidence

      assertNoLogs(blockState.processMessage(prepare)) shouldBe true
      assertNoLogs(blockState.advance()) shouldBe empty

      // after processing our pre-prepare, we should only see a send result (and no sign result) for the Prepare,
      // since we are using the recovered (pre-existing) prepare
      assertNoLogs(blockState.processMessage(prePrepare)) shouldBe true
      assertNoLogs(blockState.advance()) shouldBe List(
        SendPbftMessage(prePrepare, store = Some(StorePrePrepare(prePrepare)))
      )
      blockState.confirmPrePrepareStored()

      val processResults = assertNoLogs(blockState.advance())
      inside(processResults) {
        case Seq(
              SendPbftMessage(SignedMessage(p, _), _)
            ) =>
          // if a new prepare had been created, its memoized bytes should be different,
          // but that didn't happen, so we know the rehydrated prepare was picked
          p.getCryptographicEvidence shouldBe cryptoEvidence
      }
      clock.reset()
      succeed
    }

    "create status message and messages to retransmit" in {
      val numberOfNodes = 4
      val nodes = (1 until numberOfNodes).map { index =>
        BftNodeId(s"node$index")
      }.toSet
      val membership = Membership.forTesting(myId, nodes)
      val blockState = createBlockState(nodes)
      val strongQuorum = membership.orderingTopology.strongQuorum
      val noProgressBlockStatus = ConsensusStatus.BlockStatus.InProgress(
        prePrepared = false,
        preparesPresent = Seq.fill(numberOfNodes)(false),
        commitsPresent = Seq.fill(numberOfNodes)(false),
      )
      val myPrepare = createPrepare(myId)
      val nodePrepares = nodes.toSeq.sorted.map(createPrepare(_))
      val myCommit = createCommit(myId)
      val nodeCommits = nodes.toSeq.sorted.map(createCommit(_))

      blockState.status shouldBe noProgressBlockStatus

      assertNoLogs(blockState.processMessage(prePrepare)) shouldBe true
      blockState.advance() should contain(SignPbftMessage(myPrepare.message))
      assertNoLogs(blockState.processMessage(myPrepare)) shouldBe true
      blockState.advance()

      blockState.status shouldBe ConsensusStatus.BlockStatus.InProgress(
        prePrepared = true,
        preparesPresent = Seq.fill(numberOfNodes - 1)(false) ++ Seq(true),
        commitsPresent = Seq.fill(numberOfNodes)(false),
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

      nodePrepares.zipWithIndex.map { case (prepare, index) =>
        assertNoLogs(blockState.processMessage(prepare)) shouldBe true
        blockState.status shouldBe ConsensusStatus.BlockStatus.InProgress(
          prePrepared = true,
          preparesPresent =
            Seq.fill(index + 1)(true) ++ Seq.fill(numberOfNodes - index - 2)(false) ++ Seq(true),
          commitsPresent = Seq.fill(numberOfNodes)(false),
        )

        blockState.messagesToRetransmit(
          noProgressBlockStatus
        ) should contain theSameElementsAs Seq[SignedMessage[PbftNetworkMessage]](
          prePrepare
        ) ++ (nodePrepares.take(index + 1) ++ Seq(myPrepare))
          .take(strongQuorum)
      }

      blockState.advance() should contain(SignPbftMessage(myCommit.message))

      blockState.processMessage(myCommit) shouldBe true
      blockState.advance()

      blockState.status shouldBe ConsensusStatus.BlockStatus.InProgress(
        prePrepared = true,
        preparesPresent = Seq.fill(numberOfNodes)(true),
        commitsPresent = Seq.fill(numberOfNodes - 1)(false) ++ Seq(true),
      )

      // we only retransmit local commit after the prepares were stored
      val noCommitsBlockStatus = ConsensusStatus.BlockStatus.InProgress(
        prePrepared = true,
        preparesPresent = Seq.fill(numberOfNodes)(true),
        commitsPresent = Seq.fill(numberOfNodes)(false),
      )
      blockState.messagesToRetransmit(noCommitsBlockStatus) shouldBe empty
      blockState.confirmPreparesStored()
      blockState.advance() shouldBe empty
      blockState.messagesToRetransmit(noCommitsBlockStatus) should contain only myCommit

      // Receive all but one needed Commit to make progress
      nodeCommits.zipWithIndex.take(strongQuorum - 2).foreach { case (commit, index) =>
        assertNoLogs(blockState.processMessage(commit)) shouldBe true
        blockState.status shouldBe ConsensusStatus.BlockStatus.InProgress(
          prePrepared = true,
          preparesPresent = Seq.fill(numberOfNodes)(true),
          commitsPresent =
            Seq.fill(index + 1)(true) ++ Seq.fill(numberOfNodes - index - 2)(false) ++ Seq(true),
        )

        blockState.messagesToRetransmit(
          noCommitsBlockStatus
        ) should contain theSameElementsAs (nodeCommits.take(index + 1) ++ Seq(myCommit))
          .take(strongQuorum)
      }
      blockState.advance() shouldBe empty

      assertNoLogs(blockState.processMessage(nodeCommits(strongQuorum - 2))) shouldBe true
      blockState.advance() shouldBe empty
      blockState.commitCertificate shouldBe defined

      blockState.status shouldBe ConsensusStatus.BlockStatus.Complete
    }

  }

  private def createBlockState(
      otherIds: Set[BftNodeId] = Set.empty,
      leader: BftNodeId = myId,
      pbftMessageValidator: PbftMessageValidator = (_: PrePrepare) => Right(()),
      viewNumber: ViewNumber = ViewNumber.First,
  )(implicit synchronizerProtocolVersion: ProtocolVersion) =
    new PbftBlockState(
      Membership.forTesting(myId, otherIds),
      clock,
      pbftMessageValidator,
      leader,
      EpochNumber.First,
      viewNumber,
      abort = fail(_),
      SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
      loggerFactory,
    )(synchronizerProtocolVersion, MetricsContext.Empty)

  private lazy val canonicalCommitSet =
    CanonicalCommitSet(
      Set(
        createCommit(
          myId,
          Hash.digest(HashPurpose.BftOrderingPbftBlock, ByteString.EMPTY, HashAlgorithm.Sha256),
        )
      )
    )
  private lazy val prePrepare =
    createPrePrepare(myId)
  private lazy val ppHash =
    prePrepare.message.hash
  private lazy val wrongHash = Hash.digest(
    HashPurpose.BftOrderingPbftBlock,
    ByteString.copyFromUtf8("bad data"),
    HashAlgorithm.Sha256,
  )

  private def createPrePrepare(
      p: BftNodeId
  ): SignedMessage[PrePrepare] =
    PrePrepare
      .create(
        BlockMetadata.mk(EpochNumber.First, BlockNumber.First),
        ViewNumber.First,
        OrderingBlock(Seq()),
        canonicalCommitSet,
        from = p,
      )
      .fakeSign

  private def createPrepare(
      p: BftNodeId,
      hash: Hash = ppHash,
      view: ViewNumber = ViewNumber.First,
  ): SignedMessage[Prepare] =
    Prepare
      .create(
        BlockMetadata.mk(EpochNumber.First, BlockNumber.First),
        view,
        hash,
        from = p,
      )
      .fakeSign

  private def createCommit(
      p: BftNodeId,
      hash: Hash = ppHash,
      view: ViewNumber = ViewNumber.First,
  ): SignedMessage[Commit] =
    Commit
      .create(
        BlockMetadata.mk(EpochNumber.First, BlockNumber.First),
        view,
        hash,
        CantonTimestamp.Epoch,
        from = p,
      )
      .fakeSign
}

object PbftBlockStateTest {

  private val myId = BftNodeId("self")
  private val otherIds = (1 to 3).map { index =>
    BftNodeId(s"node$index")
  }
  private val otherId1 = otherIds.head
  private val otherId2 = otherIds(1)
  private val otherId3 = otherIds(2)
}
