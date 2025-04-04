// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.availability

import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability.LocalDissemination.LocalBatchStoredSigned
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability.{
  LocalOutputFetch,
  RemoteOutputFetch,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.*
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class AvailabilityModuleOutputFetchTest
    extends AnyWordSpec
    with BftSequencerBaseTest
    with AvailabilityModuleTestUtils {

  "The availability module" when {

    "it receives OutputFetch.FetchBatchDataFromNodes (from local store) and " +
      "it is already fetching it" should {

        "do nothing" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()

          outputFetchProtocolState.localOutputMissingBatches.addOne(
            ABatchMissingBatchStatusNode1And2AcksWithNoAttemptsLeft
          )
          val availability = createAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState
          )
          availability.receive(
            LocalOutputFetch.FetchBatchDataFromNodes(
              ProofOfAvailabilityNode1And2AcksNode1And2InTopology,
              OrderedBlockForOutput.Mode.FromConsensus,
            )
          )

          outputFetchProtocolState.localOutputMissingBatches should contain only ABatchMissingBatchStatusNode1And2AcksWithNoAttemptsLeft
          outputFetchProtocolState.incomingBatchRequests should be(empty)
        }
      }

    "it receives OutputFetch.FetchBatchDataFromNodes (from local store) and " +
      "it is not already fetching it" should {

        "update the fetch progress, " +
          "set a fetch timeout and " +
          "send OutputFetch.FetchRemoteBatchData to the currently attempted node" in {
            val outputFetchProtocolState = new MainOutputFetchProtocolState()
            val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)

            implicit val context
                : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
              new ProgrammableUnitTestContext
            val availability = createAvailability[ProgrammableUnitTestEnv](
              outputFetchProtocolState = outputFetchProtocolState,
              cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
              p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
            )
            availability.receive(
              LocalOutputFetch.FetchBatchDataFromNodes(
                ProofOfAvailabilityNode1And2AcksNode1And2InTopology,
                OrderedBlockForOutput.Mode.FromConsensus,
              )
            )

            outputFetchProtocolState.localOutputMissingBatches should
              contain only ABatchId -> AMissingBatchStatusNode1And2AcksWithNode2ToTry
            outputFetchProtocolState.incomingBatchRequests should be(empty)
            context.delayedMessages should contain(
              LocalOutputFetch.FetchRemoteBatchDataTimeout(ABatchId)
            )

            p2pNetworkOutCell.get() shouldBe None
            context.runPipedMessagesAndReceiveOnModule(availability)

            p2pNetworkOutCell.get() should contain(
              P2PNetworkOut.Multicast(
                P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
                  RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, Node0).fakeSign
                ),
                Set(Node1),
              )
            )
          }
      }

    "it receives OutputFetch.FetchRemoteBatchData (from node) and " +
      "there is an incoming request for the batch already" should {

        "just record the new requesting node" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()

          outputFetchProtocolState.incomingBatchRequests.addOne(ABatchId -> Set(Node1))
          val availability = createAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState
          )
          availability.receive(RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, Node2))

          outputFetchProtocolState.localOutputMissingBatches should be(empty)
          outputFetchProtocolState.incomingBatchRequests should contain only ABatchId -> Set(
            Node1,
            Node2,
          )
        }
      }

    "it receives OutputFetch.FetchRemoteBatchData (from node) and " +
      "there is no incoming request for the batch" should {

        "record the first requesting node and " +
          "fetch batch from local store" in {
            val outputFetchProtocolState = new MainOutputFetchProtocolState()

            val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])
            val availability = createAvailability[IgnoringUnitTestEnv](
              outputFetchProtocolState = outputFetchProtocolState,
              availabilityStore = availabilityStore,
            )
            availability.receive(RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, Node1))

            outputFetchProtocolState.localOutputMissingBatches should be(empty)
            outputFetchProtocolState.incomingBatchRequests should contain only ABatchId -> Set(
              Node1
            )
            verify(availabilityStore).fetchBatches(Seq(ABatchId))
          }
      }

    "it receives OutputFetch.AttemptedBatchDataLoad (from local store) and " +
      "the batch was not found" should {

        "do nothing" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()

          val availability = createAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState
          )
          availability.receive(LocalOutputFetch.AttemptedBatchDataLoadForNode(ABatchId, None))

          outputFetchProtocolState.localOutputMissingBatches should be(empty)
          outputFetchProtocolState.incomingBatchRequests should be(empty)
        }
      }

    "it receives OutputFetch.AttemptedBatchDataLoad and " +
      "the batch was found and " +
      "there is no incoming request for the batch" should {

        "do nothing" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()

          val availability = createAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState
          )
          availability.receive(
            LocalOutputFetch.AttemptedBatchDataLoadForNode(ABatchId, Some(ABatch))
          )

          outputFetchProtocolState.localOutputMissingBatches should be(empty)
          outputFetchProtocolState.incomingBatchRequests should be(empty)
        }
      }

    "it receives OutputFetch.AttemptedBatchDataLoad and " +
      "the batch is found and " +
      "there is an incoming request for the batch" should {

        "send OutputFetch.RemoteBatchDataFetched to all requesting node and " +
          "remove the batch from incoming requests" in {
            val outputFetchProtocolState = new MainOutputFetchProtocolState()
            val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)

            implicit val context
                : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
              new ProgrammableUnitTestContext
            outputFetchProtocolState.incomingBatchRequests.addOne(ABatchId -> Set(Node1))
            val availability = createAvailability[ProgrammableUnitTestEnv](
              outputFetchProtocolState = outputFetchProtocolState,
              p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
              cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
            )
            availability.receive(
              LocalOutputFetch.AttemptedBatchDataLoadForNode(ABatchId, Some(ABatch))
            )

            outputFetchProtocolState.localOutputMissingBatches should be(empty)
            outputFetchProtocolState.incomingBatchRequests should be(empty)

            p2pNetworkOutCell.get() shouldBe None

            context.runPipedMessagesAndReceiveOnModule(availability)
            p2pNetworkOutCell.get() should contain(
              P2PNetworkOut.Multicast(
                P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
                  RemoteOutputFetch.RemoteBatchDataFetched
                    .create(Node0, ABatchId, ABatch)
                    .fakeSign
                ),
                Set(Node1),
              )
            )
          }
      }

    "it receives OutputFetch.AttemptedBatchDataLoad and " +
      "the batch is NOT found and " +
      "there is an incoming request for the batch" should {

        "just remove the batch from incoming requests" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()
          val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)

          implicit val context
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext
          outputFetchProtocolState.incomingBatchRequests.addOne(ABatchId -> Set(Node1))
          val availability = createAvailability[ProgrammableUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState,
            p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
            cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
          )
          availability.receive(
            LocalOutputFetch.AttemptedBatchDataLoadForNode(ABatchId, None)
          )

          outputFetchProtocolState.localOutputMissingBatches should be(empty)
          outputFetchProtocolState.incomingBatchRequests should be(empty)

          p2pNetworkOutCell.get() shouldBe None

          context.runPipedMessagesAndReceiveOnModule(availability)
          p2pNetworkOutCell.get() shouldBe None
        }
      }

    "it receives OutputFetch.RemoteBatchDataFetched and " +
      "the batch is not missing" should {

        "do nothing" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()

          val availability = createAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState
          )
          availability.receive(
            RemoteOutputFetch.RemoteBatchDataFetched.create(Node1, ABatchId, ABatch)
          )

          outputFetchProtocolState.localOutputMissingBatches should be(empty)
          outputFetchProtocolState.incomingBatchRequests should be(empty)
        }
      }

    "it receives OutputFetch.RemoteBatchDataFetched and " +
      "the batch is missing" should {

        "just store the batch in the local store" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()
          val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])

          outputFetchProtocolState.localOutputMissingBatches.addOne(
            ABatchId -> AMissingBatchStatusNode1And2AcksWithNode1ToTry
          )
          val availability = createAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState,
            availabilityStore = availabilityStore,
          )
          availability.receive(
            RemoteOutputFetch.RemoteBatchDataFetched.create(Node1, ABatchId, ABatch)
          )

          outputFetchProtocolState.localOutputMissingBatches should contain only ABatchId -> AMissingBatchStatusNode1And2AcksWithNode1ToTry
          outputFetchProtocolState.incomingBatchRequests should be(empty)
          verify(availabilityStore).addBatch(ABatchId, ABatch)
        }
      }

    "it receives OutputFetch.FetchedBatchStored and " +
      "the batch is not missing" should {

        "do nothing" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()

          val availability = createAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState
          )
          availability.receive(LocalOutputFetch.FetchedBatchStored(ABatchId))

          outputFetchProtocolState.localOutputMissingBatches should be(empty)
          outputFetchProtocolState.incomingBatchRequests should be(empty)
        }
      }

    "it receives OutputFetch.FetchedBatchStored but the batchId doesn't match" should {

      "not store the batch" in {
        val outputFetchProtocolState = new MainOutputFetchProtocolState()
        val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])

        val otherBatchId = WrongBatchId
        outputFetchProtocolState.localOutputMissingBatches.addOne(
          otherBatchId -> AMissingBatchStatusNode1And2AcksWithNode1ToTry
        )
        val availability = createAvailability[IgnoringUnitTestEnv](
          outputFetchProtocolState = outputFetchProtocolState,
          availabilityStore = availabilityStore,
        )
        assertLogs(
          availability.receive(
            RemoteOutputFetch.RemoteBatchDataFetched.create(Node1, otherBatchId, ABatch)
          ),
          log => {
            log.level shouldBe Level.WARN
            log.message should include("BatchId doesn't match digest")
          },
        )

        outputFetchProtocolState.localOutputMissingBatches should contain only otherBatchId -> AMissingBatchStatusNode1And2AcksWithNode1ToTry
        outputFetchProtocolState.incomingBatchRequests should be(empty)
        verifyZeroInteractions(availabilityStore)
      }
    }

    "it receives OutputFetch.FetchedBatchStored but there are more requests than allowed" should {

      "not store the batch" in {
        val outputFetchProtocolState = new MainOutputFetchProtocolState()
        val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])

        outputFetchProtocolState.localOutputMissingBatches.addOne(
          ABatchId -> AMissingBatchStatusNode1And2AcksWithNode1ToTry
        )
        val availability = createAvailability[IgnoringUnitTestEnv](
          outputFetchProtocolState = outputFetchProtocolState,
          availabilityStore = availabilityStore,
          maxRequestsInBatch = 0,
        )
        assertLogs(
          availability.receive(
            RemoteOutputFetch.RemoteBatchDataFetched.create(Node1, ABatchId, ABatch)
          ),
          log => {
            log.level shouldBe Level.WARN
            log.message should include(
              "Batch BatchId(SHA-256:f9fbd79100fb...) from 'node1' contains more requests (1) than allowed (0), skipping"
            )
          },
        )

        outputFetchProtocolState.localOutputMissingBatches should contain only ABatchId -> AMissingBatchStatusNode1And2AcksWithNode1ToTry
        outputFetchProtocolState.incomingBatchRequests should be(empty)
        verifyZeroInteractions(availabilityStore)
      }
    }

    "it receives OutputFetch.FetchedBatchStored and " +
      "the batch is missing" should {

        "just remove it from missing" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()

          outputFetchProtocolState.localOutputMissingBatches.addOne(
            ABatchId -> AMissingBatchStatusNode1And2AcksWithNode1ToTry
          )
          val availability = createAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState
          )
          availability.receive(LocalOutputFetch.FetchedBatchStored(ABatchId))

          outputFetchProtocolState.localOutputMissingBatches should be(empty)
          outputFetchProtocolState.incomingBatchRequests should be(empty)
        }
      }

    "it receives OutputFetch.FetchRemoteBatchDataTimeout and " +
      "the batch is not missing" should {

        "do nothing" in {
          val outputFetchProtocolState = new MainOutputFetchProtocolState()

          val availability = createAvailability[IgnoringUnitTestEnv](
            outputFetchProtocolState = outputFetchProtocolState
          )
          availability.receive(LocalOutputFetch.FetchRemoteBatchDataTimeout(ABatchId))

          outputFetchProtocolState.localOutputMissingBatches should be(empty)
          outputFetchProtocolState.incomingBatchRequests should be(empty)
        }
      }

    "it receives OutputFetch.FetchRemoteBatchDataTimeout, " +
      "the batch is missing and " +
      "there are nodes left to try" should {

        "update the fetch progress with the remaining nodes, " +
          "update the missing batches, " +
          "set a fetch timeout and " +
          "send OutputFetch.FetchRemoteBatchData to the current attempted node" in {
            val outputFetchProtocolState = new MainOutputFetchProtocolState()
            val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)

            outputFetchProtocolState.localOutputMissingBatches.addOne(
              ABatchId -> AMissingBatchStatusNode1And2AcksWithNode1ToTry
            )
            implicit val context
                : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
              new ProgrammableUnitTestContext
            val availability = createAvailability[ProgrammableUnitTestEnv](
              otherNodes = AMissingBatchStatusNode1And2AcksWithNode1ToTry.remainingNodesToTry.toSet,
              outputFetchProtocolState = outputFetchProtocolState,
              cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
              p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
            )
            availability.receive(LocalOutputFetch.FetchRemoteBatchDataTimeout(ABatchId))

            outputFetchProtocolState.localOutputMissingBatches should
              contain only ABatchId -> AMissingBatchStatusNode1And2AcksWithNoAttemptsLeft
            outputFetchProtocolState.incomingBatchRequests should be(empty)
            context.delayedMessages should contain(
              LocalOutputFetch.FetchRemoteBatchDataTimeout(ABatchId)
            )

            p2pNetworkOutCell.get() shouldBe None
            context.runPipedMessagesAndReceiveOnModule(availability)

            p2pNetworkOutCell.get() should contain(
              P2PNetworkOut.Multicast(
                P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
                  RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, Node0).fakeSign
                ),
                Set(Node1),
              )
            )
          }
      }

    "it receives OutputFetch.FetchRemoteBatchDataTimeout, " +
      "the batch is missing and " +
      "there are no nodes left to try" should {

        "restart from the whole proof of availability or the topology, " +
          "update the fetch progress with the remaining nodes, " +
          "update the missing batches, " +
          "set a fetch timeout and " +
          "send OutputFetch.FetchRemoteBatchData to the current attempted node" in {
            forAll(
              Table[MissingBatchStatus, Set[BftNodeId], MissingBatchStatus, BftNodeId](
                (
                  "missing batch status",
                  "other nodes",
                  "new missing batch status",
                  "expected send to",
                ),
                (
                  AMissingBatchStatusNode1And2AcksWithNoAttemptsLeft,
                  Set.from(AMissingBatchStatusNode1And2AcksWithNode1ToTry.remainingNodesToTry),
                  AMissingBatchStatusNode1And2AcksWithNode2ToTry,
                  Node1,
                ),
                // Ignore nodes from the PoA, use the current topology
                (
                  AMissingBatchStatusFromStateTransferWithNoAttemptsLeft,
                  Set(Node3),
                  AMissingBatchStatusFromStateTransferWithNoAttemptsLeft,
                  Node3,
                ),
              )
            ) { (missingBatchStatus, otherNodes, newMissingBatchStatus, expectedSendTo) =>
              val outputFetchProtocolState = new MainOutputFetchProtocolState()
              val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)
              val cryptoProvider = mock[CryptoProvider[ProgrammableUnitTestEnv]]
              val fetchRemoteBatchData =
                RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, Node0)
              when(
                cryptoProvider.signMessage(
                  fetchRemoteBatchData,
                  AuthenticatedMessageType.BftSignedAvailabilityMessage,
                )
              ) thenReturn (() => Right(fetchRemoteBatchData.fakeSign))

              outputFetchProtocolState.localOutputMissingBatches.addOne(
                ABatchId -> missingBatchStatus
              )
              implicit val context
                  : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
                new ProgrammableUnitTestContext
              val availability = createAvailability[ProgrammableUnitTestEnv](
                otherNodes = otherNodes,
                outputFetchProtocolState = outputFetchProtocolState,
                cryptoProvider = cryptoProvider,
                p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
              )
              loggerFactory.assertLoggedWarningsAndErrorsSeq(
                availability.receive(LocalOutputFetch.FetchRemoteBatchDataTimeout(ABatchId)),
                forEvery(_) { entry =>
                  entry.message should include("got fetch timeout")
                  entry.message should include("no nodes")
                  entry.message should include("restarting fetch from the beginning")
                },
              )

              outputFetchProtocolState.localOutputMissingBatches should
                contain only ABatchId -> newMissingBatchStatus
              outputFetchProtocolState.incomingBatchRequests should be(empty)
              context.delayedMessages should contain(
                LocalOutputFetch.FetchRemoteBatchDataTimeout(ABatchId)
              )

              p2pNetworkOutCell.get() shouldBe None
              context.runPipedMessagesAndReceiveOnModule(availability)

              verify(cryptoProvider).signMessage(
                fetchRemoteBatchData,
                AuthenticatedMessageType.BftSignedAvailabilityMessage,
              )

              p2pNetworkOutCell.get() should contain(
                P2PNetworkOut.Multicast(
                  P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
                    RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, Node0).fakeSign
                  ),
                  Set(expectedSendTo),
                )
              )
            }
          }
      }

    "it receives " +
      "Dissemination.StoreLocalBatch, " +
      "Dissemination.StoreRemoteBatch " +
      "and OutputFetch.StoreFetchedBatch" should {

        type Msg = Availability.Message[ProgrammableUnitTestEnv]

        "store the batch" in {
          forAll(
            Table[Msg, Msg](
              ("message", "reply"),
              (
                Availability.LocalDissemination.LocalBatchCreated(Seq(anOrderingRequest)),
                Availability.LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)),
              ),
              (
                Availability.RemoteDissemination.RemoteBatch.create(
                  ABatchId,
                  ABatch,
                  Node0,
                ),
                Availability.LocalDissemination
                  .RemoteBatchStored(ABatchId, anEpochNumber, Node0),
              ),
              (
                Availability.RemoteOutputFetch.RemoteBatchDataFetched.create(
                  Node0,
                  ABatchId,
                  ABatch,
                ),
                Availability.LocalOutputFetch.FetchedBatchStored(ABatchId),
              ),
            )
          ) { (message, reply) =>
            val outputFetchProtocolState = new MainOutputFetchProtocolState()
            outputFetchProtocolState.localOutputMissingBatches.addOne(
              ABatchId -> MissingBatchStatus(
                ABatchId,
                ProofOfAvailabilityNode1And2AcksNode1And2InTopology,
                Seq(Node1),
                mode = OrderedBlockForOutput.Mode.FromConsensus,
              )
            )
            val storage = TrieMap[BatchId, OrderingRequestBatch]()
            implicit val context
                : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
              new ProgrammableUnitTestContext
            val availability = createAvailability[ProgrammableUnitTestEnv](
              outputFetchProtocolState = outputFetchProtocolState,
              availabilityStore = new FakeAvailabilityStore[ProgrammableUnitTestEnv](storage),
            )

            availability.receive(message)

            storage shouldBe empty
            context.runPipedMessages() shouldBe Seq(reply)
            storage should contain only (ABatchId -> ABatch)
          }
        }

        "LocalBatchStored and RemoteBatchStored should be signed" in {
          forAll(
            Table[Msg, Msg](
              ("message", "reply"),
              (
                Availability.LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)),
                Availability.LocalDissemination
                  .LocalBatchesStoredSigned(
                    Seq(LocalBatchStoredSigned(ABatchId, ABatch, Right(Signature.noSignature)))
                  ),
              ),
              (
                Availability.LocalDissemination
                  .RemoteBatchStored(ABatchId, anEpochNumber, Node0),
                Availability.LocalDissemination
                  .RemoteBatchStoredSigned(ABatchId, Node0, Signature.noSignature),
              ),
            )
          ) { case (message, reply) =>
            implicit val context
                : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
              new ProgrammableUnitTestContext
            val cryptoProvider = spy(ProgrammableUnitTestEnv.noSignatureCryptoProvider)
            val availability = createAvailability[ProgrammableUnitTestEnv](
              cryptoProvider = cryptoProvider
            )

            availability.receive(message)

            context.runPipedMessages() shouldBe Seq(reply)
            verify(cryptoProvider).signHash(
              AvailabilityAck.hashFor(ABatchId, anEpochNumber, Node0)
            )
          }
        }

        "after having stored a remote batch, " +
          "update the pending requests and fetch it " +
          "when it's the only one missing" in {
            forAll(
              Table[Msg](
                "message",
                Availability.LocalDissemination
                  .RemoteBatchStoredSigned(ABatchId, Node0, Signature.noSignature),
                Availability.LocalOutputFetch.FetchedBatchStored(ABatchId),
              )
            ) { message =>
              val singleBatchMissingRequest = new BatchesRequest(
                AnOrderedBlockForOutput,
                missingBatches = mutable.SortedSet(ABatchId),
              )

              val outputFetchProtocolState = new MainOutputFetchProtocolState()
              outputFetchProtocolState.pendingBatchesRequests.addOne(singleBatchMissingRequest)
              outputFetchProtocolState.localOutputMissingBatches.addOne(
                ABatchId -> MissingBatchStatus(
                  ABatchId,
                  ProofOfAvailabilityNode1And2AcksNode1And2InTopology,
                  Seq(Node1),
                  mode = OrderedBlockForOutput.Mode.FromConsensus,
                )
              )
              implicit val context
                  : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
                new ProgrammableUnitTestContext

              val availabilityStore = spy(new FakeAvailabilityStore[ProgrammableUnitTestEnv]())
              val availability = createAvailability[ProgrammableUnitTestEnv](
                availabilityStore = availabilityStore,
                outputFetchProtocolState = outputFetchProtocolState,
                cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
              )
              val result = mock[AvailabilityStore.FetchBatchesResult]
              when(availabilityStore.fetchBatches(Seq(ABatchId))) thenReturn (() => result)

              availability.receive(message)

              outputFetchProtocolState.pendingBatchesRequests shouldBe empty
              singleBatchMissingRequest.missingBatches shouldBe empty
              context.runPipedMessages() should contain(
                LocalOutputFetch.FetchedBlockDataFromStorage(singleBatchMissingRequest, result)
              )
            }
          }

        "after having stored a remote batch, " +
          "update the pending requests and " +
          "don't fetch it when batches are still missing" in {
            forAll(
              Table[Msg](
                "message",
                Availability.LocalDissemination
                  .RemoteBatchStoredSigned(ABatchId, Node0, Signature.noSignature),
                Availability.LocalOutputFetch.FetchedBatchStored(ABatchId),
              )
            ) { message =>
              val multipleBatchMissingRequest = new BatchesRequest(
                AnotherOrderedBlockForOutput,
                missingBatches = mutable.SortedSet(ABatchId, AnotherBatchId),
              )

              val outputFetchProtocolState = new MainOutputFetchProtocolState()
              outputFetchProtocolState.pendingBatchesRequests.addOne(multipleBatchMissingRequest)
              outputFetchProtocolState.localOutputMissingBatches.addOne(
                ABatchId -> MissingBatchStatus(
                  ABatchId,
                  ProofOfAvailabilityNode1And2AcksNode1And2InTopology,
                  Seq(Node1),
                  mode = OrderedBlockForOutput.Mode.FromConsensus,
                )
              )
              implicit val context
                  : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
                new ProgrammableUnitTestContext

              val availabilityStore = spy(new FakeAvailabilityStore[ProgrammableUnitTestEnv]())
              val availability = createAvailability[ProgrammableUnitTestEnv](
                availabilityStore = availabilityStore,
                outputFetchProtocolState = outputFetchProtocolState,
                cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
              )

              availability.receive(message)

              outputFetchProtocolState.pendingBatchesRequests.size should be(1)
              outputFetchProtocolState
                .pendingBatchesRequests(0)
                .missingBatches should contain only AnotherBatchId
              context.runPipedMessages()
              multipleBatchMissingRequest.missingBatches.toSet shouldBe Set(AnotherBatchId)
              verifyZeroInteractions(availabilityStore)
            }
          }
      }

    "it receives OutputFetch.FetchedBlockDataFromStorage and there are no missing batches" should {

      "send to output" in {
        val storage = TrieMap[BatchId, OrderingRequestBatch](ABatchId -> ABatch)
        val availabilityStore =
          spy(new FakeAvailabilityStore[FakePipeToSelfCellUnitTestEnv](storage))
        val cellContextFake =
          new AtomicReference[
            Option[() => Option[Availability.Message[FakePipeToSelfCellUnitTestEnv]]]
          ](None)
        val expectedOutputCell =
          new AtomicReference[Option[Output.Message[FakePipeToSelfCellUnitTestEnv]]](None)
        implicit val context: FakePipeToSelfCellUnitTestContext[
          Availability.Message[FakePipeToSelfCellUnitTestEnv]
        ] =
          FakePipeToSelfCellUnitTestContext(cellContextFake)
        val availability = createAvailability(
          availabilityStore = availabilityStore,
          output = fakeCellModule(expectedOutputCell),
        )
        val request = new BatchesRequest(AnOrderedBlockForOutput, mutable.SortedSet(ABatchId))

        availability.receive(
          Availability.LocalOutputFetch.FetchedBlockDataFromStorage(
            request,
            AvailabilityStore.AllBatches(Seq(ABatchId -> ABatch)),
          )
        )

        expectedOutputCell.get() shouldBe Some(
          Output.BlockDataFetched(ACompleteBlock)
        )
      }
    }

    "it receives OutputFetch.FetchedBlockDataFromStorage and there are missing batches" should {

      "record the missing batches and ask other node for missing data" in {
        forAll(
          Table[OrderedBlockForOutput.Mode, BftNodeId](
            ("block mode", "expected send to"),
            (OrderedBlockForOutput.Mode.FromConsensus, Node1),
            // Ignore nodes from the PoA, use the current topology
            (OrderedBlockForOutput.Mode.StateTransfer.MiddleBlock, Node3),
            // Ignore nodes from the PoA, use the current topology
            (OrderedBlockForOutput.Mode.StateTransfer.LastBlock, Node3),
          )
        ) { (blockMode, expectedSendTo) =>
          val outputFetchProtocolState = new MainOutputFetchProtocolState()
          val expectedMessageCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)
          val cellNetwork = fakeCellModule(expectedMessageCell)
          val availabilityStore = spy(new FakeAvailabilityStore[ProgrammableUnitTestEnv])
          implicit val context
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext
          val availability = createAvailability(
            otherNodes = Set(Node3),
            availabilityStore = availabilityStore,
            outputFetchProtocolState = outputFetchProtocolState,
            cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
            p2pNetworkOut = cellNetwork,
          )

          val request = new BatchesRequest(
            OrderedBlockForOutput(
              OrderedBlock(
                ABlockMetadata,
                Seq(ProofOfAvailabilityNode1And2AcksNode1And2InTopology),
                CanonicalCommitSet(Set.empty),
              ),
              ViewNumber.First,
              isLastInEpoch = false, // Irrelevant for availability
              from = Node0,
              mode = blockMode,
            ),
            mutable.SortedSet(ABatchId),
          )
          outputFetchProtocolState.pendingBatchesRequests.addOne(request)

          availability.receive(
            Availability.LocalOutputFetch
              .FetchedBlockDataFromStorage(
                request,
                AvailabilityStore.MissingBatches(Set(ABatchId)),
              )
          )

          outputFetchProtocolState.pendingBatchesRequests.size should be(1)
          outputFetchProtocolState
            .pendingBatchesRequests(0)
            .missingBatches should contain only ABatchId

          expectedMessageCell.get() shouldBe None

          context.runPipedMessagesAndReceiveOnModule(availability)

          expectedMessageCell.get() should contain(
            P2PNetworkOut.send(
              P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
                Availability.RemoteOutputFetch.FetchRemoteBatchData
                  .create(ABatchId, from = Node0)
                  .fakeSign
              ),
              expectedSendTo,
            )
          )
        }
      }
    }

    "it receives OutputFetch.LoadBatchData and the batch is present" should {

      "reply to local availability with the batch" in {
        val storage = TrieMap[BatchId, OrderingRequestBatch](ABatchId -> ABatch)
        val cellContextFake =
          new AtomicReference[
            Option[() => Option[Availability.Message[FakePipeToSelfCellUnitTestEnv]]]
          ](None)
        val availabilityStore =
          spy(new FakeAvailabilityStore[FakePipeToSelfCellUnitTestEnv](storage))
        implicit val context: FakePipeToSelfCellUnitTestContext[
          Availability.Message[FakePipeToSelfCellUnitTestEnv]
        ] =
          FakePipeToSelfCellUnitTestContext(cellContextFake)
        val availability = createAvailability(
          availabilityStore = availabilityStore
        )

        availability.receive(
          Availability.RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, Node0)
        )

        cellContextFake.get() shouldBe defined
        cellContextFake.get().foreach { f =>
          f() shouldBe Some(
            Availability.LocalOutputFetch.AttemptedBatchDataLoadForNode(ABatchId, Some(ABatch))
          )
        }
      }
    }

    "it receives OutputFetch.LoadBatchData and the batch is missing" should {

      "reply to local availability without the batch" in {
        val cellContextFake =
          new AtomicReference[
            Option[() => Option[Availability.Message[FakePipeToSelfCellUnitTestEnv]]]
          ](None)
        val availabilityStore = spy(new FakeAvailabilityStore[FakePipeToSelfCellUnitTestEnv])
        implicit val context: FakePipeToSelfCellUnitTestContext[
          Availability.Message[FakePipeToSelfCellUnitTestEnv]
        ] =
          FakePipeToSelfCellUnitTestContext(cellContextFake)
        val availability = createAvailability(
          availabilityStore = availabilityStore
        )

        availability.receive(
          Availability.RemoteOutputFetch.FetchRemoteBatchData.create(ABatchId, Node0)
        )

        cellContextFake.get() shouldBe defined
        cellContextFake.get().foreach { f =>
          f() shouldBe
            Some(Availability.LocalOutputFetch.AttemptedBatchDataLoadForNode(ABatchId, None))
        }
      }
    }
  }
}
