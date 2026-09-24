// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.ViewPosition.MerkleSeqIndex
import com.digitalasset.canton.data.ViewPosition.MerkleSeqIndex.Direction
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Confirmation
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{BaseTest, CommandId, LfPartyId, UserId}
import org.scalatest.funspec.PathAnyFunSpec

import java.time.Duration
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.language.existentials

import MediatorVerdict.MediatorApprove
import ResponseAggregation.{ConsortiumVotingState, ViewState}

class ResponseAggregationTest extends PathAnyFunSpec with BaseTest {

  private implicit val ec: ExecutionContext = directExecutionContext

  describe(classOf[ResponseAggregation[?]].getSimpleName) {
    def b[A](i: Int): BlindedNode[A] = BlindedNode(RootHash(TestHash.digest(i)))

    val hashOps: HashOps = new SymbolicPureCrypto

    def salt(i: Int): Salt = TestSalt.generateSalt(i)

    val synchronizerId = DefaultTestIdentities.physicalSynchronizerId
    val mediator = MediatorGroupRecipient(MediatorGroupIndex.zero)
    val participantId = DefaultTestIdentities.participant1

    val alice = LfPartyId.assertFromString("alice")
    val aliceCp = Map(alice -> PositiveInt.three)
    val bob = LfPartyId.assertFromString("bob")
    val bobCp = Map(bob -> PositiveInt.two)
    val charlie = LfPartyId.assertFromString("charlie")
    val dave = LfPartyId.assertFromString("dave")
    val daveCp = Map(dave -> PositiveInt.one)
    val solo = ParticipantId("solo")
    val one = ParticipantId("one")
    val two = ParticipantId("two")
    val three = ParticipantId("three")

    val emptySubviews = TransactionSubviews.empty(testedProtocolVersion, hashOps)

    val viewCommonData2 =
      ViewCommonData.tryCreate(hashOps)(
        ViewConfirmationParameters.tryCreate(
          Set(bob, charlie),
          Seq(Quorum(bobCp, NonNegativeInt.two)),
        ),
        salt(54170),
        testedProtocolVersion,
      )
    val viewCommonData1 =
      ViewCommonData.tryCreate(hashOps)(
        ViewConfirmationParameters.tryCreate(
          Set(alice, bob),
          Seq(Quorum(aliceCp ++ bobCp, NonNegativeInt.three)),
        ),
        salt(54171),
        testedProtocolVersion,
      )
    val view2 =
      TransactionView.tryCreate(hashOps)(
        viewCommonData2,
        b(100),
        emptySubviews,
        testedProtocolVersion,
      )
    val view1Subviews = TransactionSubviews(view2 :: Nil)(testedProtocolVersion, hashOps)
    val view1 =
      TransactionView.tryCreate(hashOps)(
        viewCommonData1,
        b(8),
        view1Subviews,
        testedProtocolVersion,
      )

    val view1Position = ViewPosition(List(MerkleSeqIndex(List.empty)))
    val view2Position = ViewPosition(List(MerkleSeqIndex(List.empty), MerkleSeqIndex(List.empty)))

    val requestId = RequestId(CantonTimestamp.Epoch)

    val submitterMetadata = SubmitterMetadata(
      NonEmpty(Set, alice),
      UserId.assertFromString("kaese"),
      CommandId.assertFromString("wurst"),
      participantId,
      salt = salt(6638),
      None,
      DeduplicationPeriod.DeduplicationDuration(Duration.ZERO),
      CantonTimestamp.MaxValue,
      None,
      hashOps,
      testedProtocolVersion,
    )

    val commonMetadata = CommonMetadata
      .create(hashOps)(
        synchronizerId,
        mediator,
        salt(5417),
        new UUID(0L, 0L),
      )

    def mkResponse(
        viewPosition: ViewPosition,
        verdict: LocalVerdict,
        confirmingParties: Set[LfPartyId],
        rootHash: RootHash,
        sender: ParticipantId = solo,
    ): ConfirmationResponses =
      ConfirmationResponses.tryCreate(
        requestId,
        rootHash,
        synchronizerId,
        sender,
        NonEmpty.mk(
          Seq,
          ConfirmationResponse.tryCreate(
            Some(viewPosition),
            verdict,
            confirmingParties,
          ),
        ),
        testedProtocolVersion,
      )

    describe("correct aggregation") {
      def testReject() =
        LocalRejectError.ConsistencyRejections.LockedContracts
          .Reject(Seq())
          .toLocalReject(testedProtocolVersion)

      val fullInformeeTree =
        FullInformeeTree.tryCreate(
          GenTransactionTree.tryCreate(hashOps)(
            submitterMetadata,
            commonMetadata,
            b(2),
            MerkleSeq.fromSeq(hashOps, testedProtocolVersion)(view1 :: Nil),
          ),
          testedProtocolVersion,
        )
      val requestId = RequestId(CantonTimestamp.Epoch)
      val informeeMessage =
        InformeeMessage(fullInformeeTree, Signature.noSignature)(testedProtocolVersion)
      val rootHash = informeeMessage.rootHash
      val someOtherRootHash = RootHash(TestHash.digest(12345))

      val topologySnapshot: TopologySnapshot = mock[TopologySnapshot]
      when(
        topologySnapshot.activeParticipantsOfPartiesWithInfo(any[Seq[LfPartyId]])(anyTraceContext)
      )
        .thenAnswer { (parties: Seq[LfPartyId]) =>
          FutureUnlessShutdown.pure(
            parties
              .map(x =>
                x -> PartyInfo(
                  PositiveInt.one,
                  Map(ParticipantId("one") -> ParticipantAttributes(Confirmation)),
                )
              )
              .toMap
          )
        }

      val sut = ResponseAggregation
        .fromRequest(
          requestId,
          informeeMessage,
          requestId.unwrap.plusSeconds(300),
          requestId.unwrap.plusSeconds(600),
          topologySnapshot,
          participantResponseDeadlineTick = None,
        )
        .futureValueUS

      it("should have initially all pending confirming parties listed") {
        sut.state shouldBe Right(
          Map(
            view1Position -> ViewState(
              Map(
                alice -> ConsortiumVotingState.withDefaultValues(),
                bob -> ConsortiumVotingState.withDefaultValues(),
              ),
              Seq(Quorum(aliceCp ++ bobCp, NonNegativeInt.three)),
              Nil,
            ),
            view2Position -> ViewState(
              Map(bob -> ConsortiumVotingState.withDefaultValues()),
              Seq(Quorum(bobCp, NonNegativeInt.two)),
              Nil,
            ),
          )
        )
      }

      it("should reject responses with the wrong root hash") {
        val responseWithWrongRootHash = ConfirmationResponses.tryCreate(
          requestId,
          someOtherRootHash,
          synchronizerId,
          solo,
          NonEmpty.mk(
            Seq,
            ConfirmationResponse.tryCreate(
              Some(view1Position),
              LocalApprove(testedProtocolVersion),
              Set(alice),
            ),
          ),
          testedProtocolVersion,
        )
        val responseTs = requestId.unwrap.plusSeconds(1)
        val result = loggerFactory.assertLogs(
          sut
            .validateAndProgress(responseTs, responseWithWrongRootHash, topologySnapshot)
            .futureValueUS,
          _.shouldBeCantonError(
            MediatorError.MalformedMessage,
            _ shouldBe show"Received a confirmation response at $responseTs by $solo for request $requestId with an invalid root hash $someOtherRootHash instead of $rootHash. Discarding response...",
          ),
        )
        result shouldBe None
      }

      when(
        topologySnapshot.canConfirm(eqTo(solo), any[Set[LfPartyId]])(anyTraceContext)
      )
        .thenAnswer { (_: ParticipantId, parties: Set[LfPartyId]) =>
          FutureUnlessShutdown.pure(parties)
        }

      describe("rejection") {
        val changeTs1 = requestId.unwrap.plusSeconds(1)

        describe("by Alice with veto rights due to her weight of 3") {
          it("rejects the transaction") {
            val response1 = mkResponse(
              view1Position,
              testReject(),
              Set(alice),
              rootHash,
            )
            val rejected1 =
              sut.validateAndProgress(changeTs1, response1, topologySnapshot).futureValueUS.value

            rejected1 shouldBe ResponseAggregation[ViewPosition](
              requestId,
              informeeMessage,
              requestId.unwrap.plusSeconds(300),
              requestId.unwrap.plusSeconds(600),
              changeTs1,
              Left(
                MediatorVerdict.ParticipantReject(
                  NonEmpty(List, (Set(alice), solo, testReject()))
                )
              ),
            )(TraceContext.empty, None)
          }
        }

        describe("by a 'light-weight' party") {
          val response1 = mkResponse(view1Position, testReject(), Set(bob), rootHash)
          lazy val rejected1 = loggerFactory.suppressWarningsAndErrors {
            sut.validateAndProgress(changeTs1, response1, topologySnapshot).futureValueUS.value
          }

          it("leaves possibility of overall approval") {
            rejected1.version shouldBe changeTs1
            rejected1.state shouldBe
              Right(
                Map(
                  view1Position -> ViewState(
                    Map(
                      alice -> ConsortiumVotingState.withDefaultValues(),
                      bob -> ConsortiumVotingState.withDefaultValues(rejections =
                        List(solo -> testReject())
                      ),
                    ),
                    Seq(Quorum(aliceCp, NonNegativeInt.three)),
                    List((Set(bob), solo, testReject())),
                  ),
                  view2Position -> ViewState(
                    Map(bob -> ConsortiumVotingState.withDefaultValues()),
                    Seq(Quorum(bobCp, NonNegativeInt.two)),
                    Nil,
                  ),
                )
              )
          }

          describe("a subsequent rejection that ensure no possibility of overall approval") {
            val changeTs2 = changeTs1.plusSeconds(1)
            val response2 = mkResponse(
              view1Position,
              testReject(),
              Set(alice),
              rootHash,
            )
            lazy val rejected2 = rejected1
              .validateAndProgress(changeTs2, response2, topologySnapshot)
              .futureValueUS
              .value

            val rejection =
              MediatorVerdict.ParticipantReject(
                NonEmpty(List, (Set(alice), solo, testReject()), (Set(bob), solo, testReject()))
              )
            it("rejects the transaction") {
              rejected2 shouldBe ResponseAggregation[ViewPosition](
                requestId,
                informeeMessage,
                requestId.unwrap.plusSeconds(300),
                requestId.unwrap.plusSeconds(600),
                changeTs2,
                Left(rejection),
              )(TraceContext.empty, None)
            }

            describe("further rejection") {
              val changeTs3 = changeTs2.plusSeconds(1)
              val response3 = mkResponse(
                view1Position,
                testReject(),
                Set(bob),
                rootHash,
              )
              lazy val rejected3 =
                rejected2.validateAndProgress(changeTs3, response3, topologySnapshot).futureValueUS
              it("should not rejection after finalization") {
                rejected3 shouldBe None
              }
            }

            describe("further approval") {
              val changeTs3 = changeTs2.plusSeconds(1)
              val response3 = mkResponse(
                view1Position,
                LocalApprove(testedProtocolVersion),
                Set(alice),
                rootHash,
              )
              lazy val rejected3 =
                rejected2.validateAndProgress(changeTs3, response3, topologySnapshot).futureValueUS
              it("should not allow approval after finalization") {
                rejected3 shouldBe None
              }
            }
          }
        }
      }

      describe("approval") {
        lazy val changeTs = requestId.unwrap.plusSeconds(1)
        val response1 = mkResponse(
          view1Position,
          LocalApprove(testedProtocolVersion),
          Set(bob),
          rootHash,
        )
        lazy val result =
          sut.validateAndProgress(changeTs, response1, topologySnapshot).futureValueUS.value

        it("should update the pending confirming parties set") {
          result.version shouldBe changeTs
          result.state shouldBe
            Right(
              Map(
                view1Position -> ViewState(
                  Map(
                    alice -> ConsortiumVotingState.withDefaultValues(),
                    bob -> ConsortiumVotingState.withDefaultValues(approvals = Set(solo)),
                  ),
                  Seq(Quorum(aliceCp, NonNegativeInt.one)),
                  Nil,
                ),
                view2Position -> ViewState(
                  Map(bob -> ConsortiumVotingState.withDefaultValues()),
                  Seq(Quorum(bobCp, NonNegativeInt.two)),
                  Nil,
                ),
              )
            )
        }
        describe("if approvals meet the threshold") {
          val response2 = mkResponse(
            view1Position,
            LocalApprove(testedProtocolVersion),
            Set(alice),
            rootHash,
          )
          lazy val step2 =
            result.validateAndProgress(changeTs, response2, topologySnapshot).futureValueUS.value

          val response3 = mkResponse(
            view2Position,
            LocalApprove(testedProtocolVersion),
            Set(bob),
            rootHash,
          )
          lazy val step3 =
            step2.validateAndProgress(changeTs, response3, topologySnapshot).futureValueUS.value

          it("should get an approved verdict") {
            step3 shouldBe ResponseAggregation[ViewPosition](
              requestId,
              informeeMessage,
              requestId.unwrap.plusSeconds(300),
              requestId.unwrap.plusSeconds(600),
              changeTs,
              Left(MediatorApprove),
            )(TraceContext.empty, None)
          }

          describe("further rejection") {
            val response4 =
              mkResponse(
                view1Position,
                LocalRejectError.MalformedRejects.Payloads
                  .Reject("test4")
                  .toLocalReject(testedProtocolVersion),
                Set.empty,
                rootHash,
              )
            lazy val result =
              step3
                .validateAndProgress(requestId.unwrap.plusSeconds(2), response4, topologySnapshot)
                .futureValueUS
            it("should not allow repeated rejection") {
              result shouldBe None
            }
          }

          describe("further redundant approval") {
            val response4 = mkResponse(
              view1Position,
              LocalApprove(testedProtocolVersion),
              Set(alice),
              rootHash,
            )
            lazy val result =
              step3
                .validateAndProgress(requestId.unwrap.plusSeconds(2), response4, topologySnapshot)
                .futureValueUS
            it("should not allow repeated rejection") {
              result shouldBe None
            }
          }
        }
      }
      describe("abstain") {
        val localAbstain = LocalAbstainError.CannotPerformAllValidations
          .Abstain("Unassignment data not found")
          .toLocalAbstain(testedProtocolVersion)

        it("by a light-weight party") {
          // if bob abstains, Alice can still approve
          val response1 = mkResponse(
            view1Position,
            localAbstain,
            Set(bob),
            rootHash,
          )
          val abstain1 = sut
            .validateAndProgress(requestId.unwrap.plusSeconds(1), response1, topologySnapshot)
            .futureValueUS
            .value

          abstain1.state.value shouldBe
            Map(
              view1Position -> ViewState(
                Map(
                  alice -> ConsortiumVotingState.withDefaultValues(),
                  bob -> ConsortiumVotingState.withDefaultValues(abstains = Set(solo)),
                ),
                Seq(Quorum(aliceCp, NonNegativeInt.three)),
                List((Set(bob), solo, localAbstain)),
              ),
              view2Position -> ViewState(
                Map(bob -> ConsortiumVotingState.withDefaultValues()),
                Seq(Quorum(bobCp, NonNegativeInt.two)),
                Nil,
              ),
            )
        }
        it("by Alice with veto rights") {
          // if alice abstains, we cannot reach the threshold with bob confirmation, so the mediator should reject
          val response1 = mkResponse(
            view1Position,
            localAbstain,
            Set(alice),
            rootHash,
          )
          val t2 = requestId.unwrap.plusSeconds(2)
          val abstain = sut.validateAndProgress(t2, response1, topologySnapshot).futureValueUS.value

          abstain shouldBe ResponseAggregation[ViewPosition](
            requestId,
            informeeMessage,
            requestId.unwrap.plusSeconds(300),
            requestId.unwrap.plusSeconds(600),
            t2,
            Left(
              MediatorVerdict.ParticipantReject(
                NonEmpty(List, (Set(alice), solo, localAbstain))
              )
            ),
          )(TraceContext.empty, None)
        }
      }
    }

    describe("response Malformed") {

      val viewCommonData1 = ViewCommonData.tryCreate(hashOps)(
        ViewConfirmationParameters.tryCreate(
          Set(alice, bob, charlie),
          Seq(Quorum(aliceCp ++ bobCp, NonNegativeInt.three)),
        ),
        salt(54170),
        testedProtocolVersion,
      )
      val viewCommonData2 = ViewCommonData.tryCreate(hashOps)(
        ViewConfirmationParameters.tryCreate(
          Set(alice, bob, dave),
          Seq(Quorum(aliceCp ++ bobCp ++ daveCp, NonNegativeInt.three)),
        ),
        salt(54171),
        testedProtocolVersion,
      )
      val view2 =
        TransactionView.tryCreate(hashOps)(
          viewCommonData2,
          b(100),
          emptySubviews,
          testedProtocolVersion,
        )
      val view1 =
        TransactionView.tryCreate(hashOps)(
          viewCommonData1,
          b(8),
          emptySubviews,
          testedProtocolVersion,
        )

      val informeeMessage = InformeeMessage(
        FullInformeeTree.tryCreate(
          GenTransactionTree.tryCreate(hashOps)(
            submitterMetadata,
            commonMetadata,
            b(2),
            MerkleSeq.fromSeq(hashOps, testedProtocolVersion)(view1 :: view2 :: Nil),
          ),
          testedProtocolVersion,
        ),
        Signature.noSignature,
      )(testedProtocolVersion)

      val view1Position = ViewPosition(List(MerkleSeqIndex(List(Direction.Left))))
      val view2Position = ViewPosition(List(MerkleSeqIndex(List(Direction.Right))))

      val topologySnapshot: TopologySnapshot = mock[TopologySnapshot]
      when(
        topologySnapshot.activeParticipantsOfPartiesWithInfo(any[Seq[LfPartyId]])(anyTraceContext)
      )
        .thenAnswer { (parties: Seq[LfPartyId]) =>
          FutureUnlessShutdown.pure(
            parties
              .map(x =>
                x -> PartyInfo(
                  PositiveInt.one,
                  Map(ParticipantId("one") -> ParticipantAttributes(Confirmation)),
                )
              )
              .toMap
          )
        }

      when(
        topologySnapshot.canConfirm(any[ParticipantId], any[Set[LfPartyId]])(
          anyTraceContext
        )
      )
        .thenAnswer { (participantId: ParticipantId, parties: Set[LfPartyId]) =>
          if (participantId != solo)
            FutureUnlessShutdown.failed(
              new IllegalArgumentException(s"unexpected participant: $participantId")
            )
          FutureUnlessShutdown.pure(parties.flatMap {
            case `bob` => Set(bob)
            case `dave` => Set(dave)
            case `alice` => Set.empty
            case otherwise => throw new IllegalArgumentException(s"unexpected party: $otherwise")
          })
        }

      val sut = ResponseAggregation
        .fromRequest(
          requestId,
          informeeMessage,
          requestId.unwrap.plusSeconds(300),
          requestId.unwrap.plusSeconds(600),
          topologySnapshot,
          participantResponseDeadlineTick = None,
        )
        .futureValueUS
      lazy val changeTs = requestId.unwrap.plusSeconds(1)

      def testReject(reason: String) =
        LocalRejectError.MalformedRejects.Payloads
          .Reject(reason)
          .toLocalReject(testedProtocolVersion)

      describe("for a single view") {
        it("should update the pending confirming parties set for all hosted parties") {
          val response = ConfirmationResponses.tryCreate(
            requestId,
            informeeMessage.rootHash,
            synchronizerId,
            solo,
            NonEmpty.mk(
              Seq,
              ConfirmationResponse.tryCreate(
                Some(view1Position),
                testReject("malformed view"),
                Set.empty,
              ),
            ),
            testedProtocolVersion,
          )
          val result =
            sut.validateAndProgress(changeTs, response, topologySnapshot).futureValueUS.value

          result.version shouldBe changeTs
          result.state.value shouldBe Map(
            view1Position -> ViewState(
              Map(
                alice -> ConsortiumVotingState.withDefaultValues(),
                bob -> ConsortiumVotingState.withDefaultValues(rejections =
                  List(solo -> testReject("malformed view"))
                ),
              ),
              Seq(Quorum(aliceCp, NonNegativeInt.three)),
              List((Set(bob), solo, testReject("malformed view"))),
            ),
            view2Position -> ViewState(
              Map(
                alice -> ConsortiumVotingState.withDefaultValues(),
                bob -> ConsortiumVotingState.withDefaultValues(),
                dave -> ConsortiumVotingState.withDefaultValues(),
              ),
              Seq(Quorum(aliceCp ++ bobCp ++ daveCp, NonNegativeInt.three)),
              Nil,
            ),
          )
        }
      }

      describe("without a view hash") {
        it("should update the pending confirming parties for all hosted parties in all views") {
          val rejectMsg = "malformed request"
          val response = ConfirmationResponses.tryCreate(
            requestId,
            informeeMessage.rootHash,
            synchronizerId,
            solo,
            NonEmpty.mk(
              Seq,
              ConfirmationResponse.tryCreate(
                None,
                testReject(rejectMsg),
                Set.empty,
              ),
            ),
            testedProtocolVersion,
          )
          val result =
            sut.validateAndProgress(changeTs, response, topologySnapshot).futureValueUS.value
          result.version shouldBe changeTs
          result.state shouldBe Right(
            Map(
              view1Position -> ViewState(
                Map(
                  alice -> ConsortiumVotingState.withDefaultValues(),
                  bob -> ConsortiumVotingState.withDefaultValues(rejections =
                    List(solo -> testReject(rejectMsg))
                  ),
                ),
                Seq(Quorum(aliceCp, NonNegativeInt.three)),
                List((Set(bob), solo, testReject(rejectMsg))),
              ),
              view2Position -> ViewState(
                Map(
                  alice -> ConsortiumVotingState.withDefaultValues(),
                  bob -> ConsortiumVotingState.withDefaultValues(rejections =
                    List(solo -> testReject(rejectMsg))
                  ),
                  dave -> ConsortiumVotingState.withDefaultValues(rejections =
                    List(solo -> testReject(rejectMsg))
                  ),
                ),
                Seq(Quorum(aliceCp, NonNegativeInt.three)),
                List((Set(bob, dave), solo, testReject(rejectMsg))),
              ),
            )
          )
        }
      }
    }

    describe("consortium state") {
      val reject = LocalRejectError.MalformedRejects.Payloads
        .Reject("reason")
        .toLocalReject(testedProtocolVersion)
      it("should work for threshold = 1") {
        ConsortiumVotingState.withDefaultValues(approvals = Set(solo)).isApproved shouldBe true
        ConsortiumVotingState.withDefaultValues(approvals = Set(solo)).isRejected shouldBe false
        ConsortiumVotingState
          .withDefaultValues(rejections = List(solo -> reject))
          .isApproved shouldBe false
        ConsortiumVotingState
          .withDefaultValues(rejections = List(solo -> reject))
          .isRejected shouldBe true
        ConsortiumVotingState.withDefaultValues(abstains = Set(solo)).isApproved shouldBe false
        ConsortiumVotingState.withDefaultValues(abstains = Set(solo)).isRejected shouldBe true
      }

      it("should work for threshold >= 2") {
        ConsortiumVotingState
          .withDefaultValues(PositiveInt.two, approvals = Set(one))
          .isApproved shouldBe false
        ConsortiumVotingState
          .withDefaultValues(PositiveInt.two, approvals = Set(one))
          .isRejected shouldBe false
        ConsortiumVotingState
          .withDefaultValues(PositiveInt.two, approvals = Set(one, two))
          .isApproved shouldBe true
        ConsortiumVotingState
          .withDefaultValues(
            PositiveInt.two,
            Some(PositiveInt.three),
            approvals = Set(one, two),
            rejections = List(three -> reject),
          )
          .isApproved shouldBe true
        ConsortiumVotingState
          .withDefaultValues(
            PositiveInt.tryCreate(4),
            Some(PositiveInt.tryCreate(5)),
            rejections = List(one -> reject, two -> reject),
          )
          .isRejected shouldBe true
        ConsortiumVotingState
          .withDefaultValues(
            PositiveInt.two,
            approvals = Set(one),
            rejections = List(two -> reject, three -> reject),
          )
          .isApproved shouldBe false
        ConsortiumVotingState
          .withDefaultValues(
            PositiveInt.two,
            Some(PositiveInt.three),
            approvals = Set(one),
            rejections = List(two -> reject, three -> reject),
          )
          .isRejected shouldBe true
        ConsortiumVotingState
          .withDefaultValues(
            PositiveInt.three,
            approvals = Set(one),
            rejections = List(two -> reject, three -> reject),
          )
          .isApproved shouldBe false
        ConsortiumVotingState
          .withDefaultValues(
            PositiveInt.three,
            approvals = Set(one),
            rejections = List(two -> reject, three -> reject),
          )
          .isRejected shouldBe true
        ConsortiumVotingState
          .withDefaultValues(PositiveInt.three, approvals = Set(one, two, three))
          .isApproved shouldBe true
        ConsortiumVotingState
          .withDefaultValues(PositiveInt.three, rejections = List(one -> reject))
          .isRejected shouldBe true
        ConsortiumVotingState
          .withDefaultValues(
            PositiveInt.three,
            rejections = List(one -> reject, two -> reject, three -> reject),
          )
          .isRejected shouldBe true

        ConsortiumVotingState
          .withDefaultValues(
            PositiveInt.two,
            numberOfHostingParticipants = Some(PositiveInt.three),
            abstains = Set(one),
          )
          .isRejected shouldBe false
        ConsortiumVotingState
          .withDefaultValues(
            PositiveInt.two,
            numberOfHostingParticipants = Some(PositiveInt.three),
            abstains = Set(one),
          )
          .isApproved shouldBe false

        ConsortiumVotingState
          .withDefaultValues(
            PositiveInt.two,
            numberOfHostingParticipants = Some(PositiveInt.three),
            approvals = Set(one, two),
            abstains = Set(three),
          )
          .isApproved shouldBe true

        ConsortiumVotingState
          .withDefaultValues(
            PositiveInt.two,
            numberOfHostingParticipants = Some(PositiveInt.three),
            rejections = List(one -> reject, two -> reject),
            abstains = Set(three),
          )
          .isRejected shouldBe true

        ConsortiumVotingState
          .withDefaultValues(
            PositiveInt.two,
            numberOfHostingParticipants = Some(PositiveInt.three),
            rejections = List(one -> reject),
            abstains = Set(three),
          )
          .isRejected shouldBe true

        ConsortiumVotingState
          .withDefaultValues(PositiveInt.three, abstains = Set(one))
          .isRejected shouldBe true
      }
      it("should not allow a participant to respond with different verdicts") {
        val localApprove = LocalApprove(testedProtocolVersion)
        val localAbstain = LocalAbstainError.CannotPerformAllValidations
          .Abstain("Unassignment data not found")
          .toLocalAbstain(testedProtocolVersion)
        val localReject = LocalRejectError.ConsistencyRejections.LockedContracts
          .Reject(Seq())
          .toLocalReject(testedProtocolVersion)

        val state = ConsortiumVotingState.withDefaultValues(
          PositiveInt.two,
          numberOfHostingParticipants = Some(PositiveInt.three),
          approvals = Set(one),
          rejections = List(two -> localReject),
          abstains = Set(three),
        )

        state.update(localApprove, one) shouldBe state
        state.update(localApprove, two) shouldBe state
        state.update(localApprove, three) shouldBe state

        state.update(localAbstain, one) shouldBe state
        state.update(localAbstain, two) shouldBe state
        state.update(localAbstain, three) shouldBe state

        state.update(localReject, one) shouldBe state
        state.update(localReject, two) shouldBe state
        state.update(localReject, three) shouldBe state
      }
    }

    describe("consortium voting") {
      val testReject =
        LocalRejectError.ConsistencyRejections.LockedContracts
          .Reject(Seq())
          .toLocalReject(testedProtocolVersion)

      val fullInformeeTree =
        FullInformeeTree.tryCreate(
          GenTransactionTree.tryCreate(hashOps)(
            submitterMetadata,
            commonMetadata,
            b(2),
            MerkleSeq.fromSeq(hashOps, testedProtocolVersion)(view1 :: Nil),
          ),
          testedProtocolVersion,
        )
      val requestId = RequestId(CantonTimestamp.Epoch)
      val informeeMessage =
        InformeeMessage(fullInformeeTree, Signature.noSignature)(testedProtocolVersion)
      val rootHash = informeeMessage.rootHash

      val topologySnapshot: TopologySnapshot = mock[TopologySnapshot]
      when(
        topologySnapshot.activeParticipantsOfPartiesWithInfo(any[Seq[LfPartyId]])(anyTraceContext)
      )
        .thenAnswer { (parties: Seq[LfPartyId]) =>
          FutureUnlessShutdown.pure(
            Map(
              alice -> PartyInfo(
                PositiveInt.two,
                Map(
                  ParticipantId("one") -> ParticipantAttributes(Confirmation),
                  ParticipantId("two") -> ParticipantAttributes(Confirmation),
                  ParticipantId("tree") -> ParticipantAttributes(Confirmation),
                ),
              ),
              bob -> PartyInfo(
                PositiveInt.three,
                Map(
                  ParticipantId("one") -> ParticipantAttributes(Confirmation),
                  ParticipantId("two") -> ParticipantAttributes(Confirmation),
                  ParticipantId("three") -> ParticipantAttributes(Confirmation),
                  ParticipantId("four") -> ParticipantAttributes(Confirmation),
                  ParticipantId("five") -> ParticipantAttributes(Confirmation),
                ),
              ),
            ).view.filterKeys(parties.contains).toMap
          )
        }

      val sut = ResponseAggregation
        .fromRequest(
          requestId,
          informeeMessage,
          requestId.unwrap.plusSeconds(300),
          requestId.unwrap.plusSeconds(600),
          topologySnapshot,
          participantResponseDeadlineTick = None,
        )
        .futureValueUS

      it("should correctly initialize the state") {
        sut.state shouldBe Right(
          Map(
            view1Position -> ViewState(
              Map(
                alice -> ConsortiumVotingState
                  .withDefaultValues(PositiveInt.two, Some(PositiveInt.tryCreate(3))),
                bob -> ConsortiumVotingState
                  .withDefaultValues(PositiveInt.three, Some(PositiveInt.tryCreate(5))),
              ),
              Seq(Quorum(aliceCp ++ bobCp, NonNegativeInt.three)),
              Nil,
            ),
            view2Position -> ViewState(
              Map(
                bob -> ConsortiumVotingState
                  .withDefaultValues(PositiveInt.three, Some(PositiveInt.tryCreate(5)))
              ),
              Seq(Quorum(bobCp, NonNegativeInt.two)),
              Nil,
            ),
          )
        )
      }

      when(
        topologySnapshot.canConfirm(any[ParticipantId], any[Set[LfPartyId]])(
          anyTraceContext
        )
      )
        .thenAnswer { (_: ParticipantId, parties: Set[LfPartyId]) =>
          FutureUnlessShutdown.pure(parties)
        }

      describe("should prevent response stuffing") {
        describe("for reject by Bob with 3 votes from the same participant") {

          val changeTs1 = requestId.unwrap.plusSeconds(1)
          val changeTs2 = requestId.unwrap.plusSeconds(2)
          val changeTs3 = requestId.unwrap.plusSeconds(3)

          val response1a = mkResponse(
            view1Position,
            testReject,
            Set(bob),
            rootHash,
            one,
          )
          val response1b = mkResponse(
            view1Position,
            testReject,
            Set(bob),
            rootHash,
            one,
          )
          val response1c = mkResponse(
            view1Position,
            testReject,
            Set(bob),
            rootHash,
            one,
          )
          lazy val rejected1 = loggerFactory.suppressWarningsAndErrors {
            (for {
              p1 <- sut.validateAndProgress(changeTs1, response1a, topologySnapshot).futureValueUS
              p2 <- p1.validateAndProgress(changeTs2, response1b, topologySnapshot).futureValueUS
              p3 <- p2.validateAndProgress(changeTs3, response1c, topologySnapshot).futureValueUS
            } yield p3).value
          }

          it("should count Bob's vote only once") {
            rejected1.version shouldBe changeTs3
            rejected1.state shouldBe
              Right(
                Map(
                  view1Position -> ViewState(
                    Map(
                      alice -> ConsortiumVotingState
                        .withDefaultValues(PositiveInt.two, Some(PositiveInt.tryCreate(3))),
                      bob -> ConsortiumVotingState.withDefaultValues(
                        PositiveInt.three,
                        Some(PositiveInt.tryCreate(5)),
                        rejections = List(one -> testReject),
                      ),
                    ),
                    Seq(Quorum(aliceCp ++ bobCp, NonNegativeInt.three)),
                    Nil,
                  ),
                  view2Position -> ViewState(
                    Map(
                      bob -> ConsortiumVotingState
                        .withDefaultValues(PositiveInt.three, Some(PositiveInt.tryCreate(5)))
                    ),
                    Seq(Quorum(bobCp, NonNegativeInt.two)),
                    Nil,
                  ),
                )
              )
          }
        }

        describe("for accept by Bob with 3 votes from the same participant") {
          lazy val changeTs1 = requestId.unwrap.plusSeconds(1)
          lazy val changeTs2 = requestId.unwrap.plusSeconds(2)
          lazy val changeTs3 = requestId.unwrap.plusSeconds(3)
          val response1a = mkResponse(
            view1Position,
            LocalApprove(testedProtocolVersion),
            Set(bob),
            rootHash,
            one,
          )
          val response1b = mkResponse(
            view1Position,
            LocalApprove(testedProtocolVersion),
            Set(bob),
            rootHash,
            one,
          )
          val response1c = mkResponse(
            view1Position,
            LocalApprove(testedProtocolVersion),
            Set(bob),
            rootHash,
            one,
          )
          lazy val result =
            (for {
              p1 <- sut.validateAndProgress(changeTs1, response1a, topologySnapshot).futureValueUS
              p2 <- p1.validateAndProgress(changeTs2, response1b, topologySnapshot).futureValueUS
              p3 <- p2.validateAndProgress(changeTs3, response1c, topologySnapshot).futureValueUS
            } yield p3).value

          it("should count Bob's vote only once") {
            result.version shouldBe changeTs3
            result.state shouldBe
              Right(
                Map(
                  view1Position -> ViewState(
                    Map(
                      alice -> ConsortiumVotingState
                        .withDefaultValues(PositiveInt.two, Some(PositiveInt.tryCreate(3))),
                      bob -> ConsortiumVotingState.withDefaultValues(
                        PositiveInt.three,
                        Some(PositiveInt.tryCreate(5)),
                        approvals = Set(one),
                      ),
                    ),
                    Seq(Quorum(aliceCp ++ bobCp, NonNegativeInt.three)),
                    Nil,
                  ),
                  view2Position -> ViewState(
                    Map(
                      bob -> ConsortiumVotingState
                        .withDefaultValues(PositiveInt.three, Some(PositiveInt.tryCreate(5)))
                    ),
                    Seq(Quorum(bobCp, NonNegativeInt.two)),
                    Nil,
                  ),
                )
              )
          }
        }
      }

      describe("rejection") {
        val changeTs1 = requestId.unwrap.plusSeconds(1)
        val changeTs2 = requestId.unwrap.plusSeconds(2)
        val changeTs3 = requestId.unwrap.plusSeconds(3)

        describe("by Alice with 2 votes") {
          it("rejects the transaction") {
            val response1a =
              mkResponse(
                view1Position,
                testReject,
                Set(alice),
                rootHash,
                one,
              )
            val response1b =
              mkResponse(
                view1Position,
                testReject,
                Set(alice),
                rootHash,
                two,
              )
            val rejected1a =
              sut.validateAndProgress(changeTs1, response1a, topologySnapshot).futureValueUS.value

            val rejected1b = rejected1a
              .validateAndProgress(changeTs2, response1b, topologySnapshot)
              .futureValueUS
              .value

            rejected1a.state shouldBe Right(
              Map(
                view1Position -> ViewState(
                  Map(
                    alice -> ConsortiumVotingState.withDefaultValues(
                      PositiveInt.two,
                      Some(PositiveInt.tryCreate(3)),
                      rejections = List(one -> testReject),
                    ),
                    bob -> ConsortiumVotingState
                      .withDefaultValues(PositiveInt.three, Some(PositiveInt.tryCreate(5))),
                  ),
                  Seq(Quorum(aliceCp ++ bobCp, NonNegativeInt.three)),
                  Nil,
                ),
                view2Position -> ViewState(
                  Map(
                    bob -> ConsortiumVotingState
                      .withDefaultValues(PositiveInt.three, Some(PositiveInt.tryCreate(5)))
                  ),
                  Seq(Quorum(bobCp, NonNegativeInt.two)),
                  Nil,
                ),
              )
            )

            rejected1b shouldBe ResponseAggregation[ViewPosition](
              requestId,
              informeeMessage,
              requestId.unwrap.plusSeconds(300),
              requestId.unwrap.plusSeconds(600),
              changeTs2,
              Left(
                MediatorVerdict.ParticipantReject(
                  NonEmpty(List, (Set(alice), two, testReject))
                )
              ),
            )(TraceContext.empty, None)
          }
        }

        describe("by Bob with 3 votes") {
          val response1a = mkResponse(
            view1Position,
            testReject,
            Set(bob),
            rootHash,
            one,
          )
          val response1b = mkResponse(
            view1Position,
            testReject,
            Set(bob),
            rootHash,
            two,
          )
          val response1c = mkResponse(
            view1Position,
            testReject,
            Set(bob),
            rootHash,
            three,
          )
          lazy val rejected1 = loggerFactory.suppressWarningsAndErrors {
            (for {
              p1 <- sut.validateAndProgress(changeTs1, response1a, topologySnapshot).futureValueUS
              p2 <- p1.validateAndProgress(changeTs2, response1b, topologySnapshot).futureValueUS
              p3 <- p2.validateAndProgress(changeTs3, response1c, topologySnapshot).futureValueUS
            } yield p3).value
          }

          it("not rejected due to Alice's heavier weight") {
            rejected1.version shouldBe changeTs3
            rejected1.state shouldBe
              Right(
                Map(
                  view1Position -> ViewState(
                    Map(
                      alice -> ConsortiumVotingState
                        .withDefaultValues(PositiveInt.two, Some(PositiveInt.tryCreate(3))),
                      bob -> ConsortiumVotingState.withDefaultValues(
                        PositiveInt.three,
                        Some(PositiveInt.tryCreate(5)),
                        rejections = List(three -> testReject, two -> testReject, one -> testReject),
                      ),
                    ),
                    Seq(Quorum(aliceCp, NonNegativeInt.three)),
                    List((Set(bob), three, testReject)),
                  ),
                  view2Position -> ViewState(
                    Map(
                      bob -> ConsortiumVotingState
                        .withDefaultValues(PositiveInt.three, Some(PositiveInt.tryCreate(5)))
                    ),
                    Seq(Quorum(bobCp, NonNegativeInt.two)),
                    Nil,
                  ),
                )
              )
          }

          describe("rejected fully with Alice's 2 votes") {
            val changeTs4 = changeTs1.plusSeconds(4)
            val changeTs5 = changeTs1.plusSeconds(5)
            val response2a =
              mkResponse(
                view1Position,
                testReject,
                Set(alice),
                rootHash,
                one,
              )
            val response2b =
              mkResponse(
                view1Position,
                testReject,
                Set(alice),
                rootHash,
                two,
              )
            lazy val rejected2 =
              (for {
                p1 <- rejected1
                  .validateAndProgress(changeTs4, response2a, topologySnapshot)
                  .futureValueUS
                p2 <- p1
                  .validateAndProgress(changeTs5, response2b, topologySnapshot)
                  .futureValueUS
              } yield p2).value
            val rejection =
              MediatorVerdict.ParticipantReject(
                NonEmpty(List, (Set(alice), two, testReject), (Set(bob), three, testReject))
              )
            it("rejects the transaction") {
              rejected2 shouldBe ResponseAggregation[ViewPosition](
                requestId,
                informeeMessage,
                requestId.unwrap.plusSeconds(300),
                requestId.unwrap.plusSeconds(600),
                changeTs5,
                Left(rejection),
              )(TraceContext.empty, None)
            }

            describe("further rejection") {
              val changeTs6 = changeTs5.plusSeconds(1)
              val response3 = mkResponse(
                view1Position,
                testReject,
                Set(bob),
                rootHash,
              )
              lazy val rejected3 =
                rejected2.validateAndProgress(changeTs6, response3, topologySnapshot).futureValueUS
              it("should not rejection after finalization") {
                rejected3 shouldBe None
              }
            }

            describe("further approval") {
              val changeTs6 = changeTs5.plusSeconds(1)
              val response3 = mkResponse(
                view1Position,
                LocalApprove(testedProtocolVersion),
                Set(alice),
                rootHash,
              )
              lazy val rejected3 =
                rejected2.validateAndProgress(changeTs6, response3, topologySnapshot).futureValueUS
              it("should not allow approval after finalization") {
                rejected3 shouldBe None
              }
            }
          }
        }
      }

      describe("correct rejection reason when receiving abstains and rejections") {
        lazy val changeTs1 = requestId.unwrap.plusSeconds(1)
        lazy val changeTs2 = requestId.unwrap.plusSeconds(2)
        lazy val changeTs3 = requestId.unwrap.plusSeconds(3)
        val reject1 = LocalRejectError.ConsistencyRejections.LockedContracts
          .Reject(Seq())
          .toLocalReject(testedProtocolVersion)
        val reject2 = LocalRejectError.ConsistencyRejections.InactiveContracts
          .Reject(Seq())
          .toLocalReject(testedProtocolVersion)
        val abstain1 = LocalAbstainError.CannotPerformAllValidations
          .Abstain("abstain 1")
          .toLocalAbstain(testedProtocolVersion)
        val abstain2 = LocalAbstainError.CannotPerformAllValidations
          .Abstain("abstain 2")
          .toLocalAbstain(testedProtocolVersion)

        val Seq(response1a, response1b, response1c) =
          Seq(reject1 -> one, reject2 -> two, abstain1 -> three).map { case (reject, participant) =>
            mkResponse(
              view1Position,
              reject,
              Set(bob),
              rootHash,
              participant,
            )
          }: @unchecked
        lazy val result =
          (for {
            p1 <- sut.validateAndProgress(changeTs1, response1a, topologySnapshot).futureValueUS
            p2 <- p1.validateAndProgress(changeTs2, response1b, topologySnapshot).futureValueUS
            p3 <- p2.validateAndProgress(changeTs3, response1c, topologySnapshot).futureValueUS
          } yield p3).value

        it("should report the last reject for bob and not the abstain") {
          result.state.value shouldBe
            Map(
              view1Position -> ViewState(
                Map(
                  alice -> ConsortiumVotingState
                    .withDefaultValues(PositiveInt.two, Some(PositiveInt.tryCreate(3))),
                  bob -> ConsortiumVotingState.withDefaultValues(
                    PositiveInt.three,
                    Some(PositiveInt.tryCreate(5)),
                    rejections = List(two -> reject2, one -> reject1),
                    abstains = Set(three),
                  ),
                ),
                Seq(Quorum(aliceCp, NonNegativeInt.three)),
                // bob's last rejection is reported instead of abstain
                List((Set(bob), two, reject2)),
              ),
              view2Position -> ViewState(
                Map(
                  bob -> ConsortiumVotingState
                    .withDefaultValues(PositiveInt.three, Some(PositiveInt.tryCreate(5)))
                ),
                Seq(Quorum(bobCp, NonNegativeInt.two)),
                Nil,
              ),
            )
        }
        it("should report the last abstain if there is no better rejection reason") {
          val Seq(response2a, response2b) =
            Seq(abstain1 -> one, abstain2 -> two).map { case (reject, participant) =>
              mkResponse(
                view1Position,
                reject,
                Set(alice),
                rootHash,
                participant,
              )
            }: @unchecked

          val result2 =
            (for {
              // starting from the previous result
              p1 <- result
                .validateAndProgress(changeTs3, response2a, topologySnapshot)
                .futureValueUS
              p2 <- p1.validateAndProgress(changeTs3, response2b, topologySnapshot).futureValueUS
            } yield p2).value

          result2.state shouldBe
            Left(
              MediatorVerdict.ParticipantReject(
                NonEmpty(List, (Set(alice), two, abstain2), (Set(bob), two, reject2))
              )
            )
        }

        it(
          "report correctly the best rejection reason for each party when receiving response with multiple confirming parties"
        ) {
          val response1a = mkResponse(
            view1Position,
            abstain1,
            Set(bob, alice),
            rootHash,
            one,
          )
          val response1b = mkResponse(
            view1Position,
            reject1,
            Set(bob),
            rootHash,
            two,
          )
          val response1c = mkResponse(
            view1Position,
            abstain2,
            Set(bob, alice),
            rootHash,
            three,
          )
          val result =
            (for {
              p1 <- sut.validateAndProgress(changeTs1, response1a, topologySnapshot).futureValueUS
              p2 <- p1.validateAndProgress(changeTs2, response1b, topologySnapshot).futureValueUS
              p3 <- p2.validateAndProgress(changeTs3, response1c, topologySnapshot).futureValueUS
            } yield p3).value

          result.state shouldBe
            Left(
              MediatorVerdict.ParticipantReject(
                NonEmptyUtil.fromUnsafe(
                  List((Set(alice), three, abstain2), (Set(bob), two, reject1))
                )
              )
            )
        }

        it("report the common abstain when there is no better rejection reason") {
          val response1a = mkResponse(
            view1Position,
            abstain1,
            Set(bob, alice),
            rootHash,
            one,
          )
          val response1b = mkResponse(
            view1Position,
            abstain1,
            Set(bob),
            rootHash,
            two,
          )
          val response1c = mkResponse(
            view1Position,
            abstain2,
            Set(bob, alice),
            rootHash,
            three,
          )
          lazy val result =
            (for {
              p1 <- sut.validateAndProgress(changeTs1, response1a, topologySnapshot).futureValueUS
              p2 <- p1.validateAndProgress(changeTs2, response1b, topologySnapshot).futureValueUS
              p3 <- p2.validateAndProgress(changeTs3, response1c, topologySnapshot).futureValueUS
            } yield p3).value

          result.state shouldBe
            Left(
              MediatorVerdict.ParticipantReject(
                NonEmptyUtil.fromUnsafe(List((Set(alice, bob), three, abstain2)))
              )
            )
        }
      }

      describe("approval") {
        lazy val changeTs1 = requestId.unwrap.plusSeconds(1)
        lazy val changeTs2 = requestId.unwrap.plusSeconds(2)
        lazy val changeTs3 = requestId.unwrap.plusSeconds(3)
        val response1a = mkResponse(
          view1Position,
          LocalApprove(testedProtocolVersion),
          Set(bob),
          rootHash,
          one,
        )
        val response1b = mkResponse(
          view1Position,
          LocalApprove(testedProtocolVersion),
          Set(bob),
          rootHash,
          two,
        )
        val response1c = mkResponse(
          view1Position,
          LocalApprove(testedProtocolVersion),
          Set(bob),
          rootHash,
          three,
        )
        lazy val result =
          (for {
            p1 <- sut.validateAndProgress(changeTs1, response1a, topologySnapshot).futureValueUS
            p2 <- p1.validateAndProgress(changeTs2, response1b, topologySnapshot).futureValueUS
            p3 <- p2.validateAndProgress(changeTs3, response1c, topologySnapshot).futureValueUS
          } yield p3).value
        it("should update the pending confirming parties set") {
          result.version shouldBe changeTs3
          result.state shouldBe
            Right(
              Map(
                view1Position -> ViewState(
                  Map(
                    alice -> ConsortiumVotingState
                      .withDefaultValues(PositiveInt.two, Some(PositiveInt.tryCreate(3))),
                    bob -> ConsortiumVotingState.withDefaultValues(
                      PositiveInt.three,
                      Some(PositiveInt.tryCreate(5)),
                      approvals = Set(one, two, three),
                    ),
                  ),
                  Seq(Quorum(aliceCp, NonNegativeInt.one)),
                  Nil,
                ),
                view2Position -> ViewState(
                  Map(
                    bob -> ConsortiumVotingState
                      .withDefaultValues(PositiveInt.three, Some(PositiveInt.tryCreate(5)))
                  ),
                  Seq(Quorum(bobCp, NonNegativeInt.two)),
                  Nil,
                ),
              )
            )
        }
        describe("if approvals meet the threshold") {
          val response2a = mkResponse(
            view1Position,
            LocalApprove(testedProtocolVersion),
            Set(alice),
            rootHash,
            one,
          )
          val response2b = mkResponse(
            view1Position,
            LocalApprove(testedProtocolVersion),
            Set(alice),
            rootHash,
            two,
          )
          lazy val step2 =
            (for {
              p1 <- result
                .validateAndProgress(changeTs2, response2a, topologySnapshot)
                .futureValueUS
              p2 <- p1.validateAndProgress(changeTs2, response2b, topologySnapshot).futureValueUS
            } yield p2).value
          val response3a = mkResponse(
            view2Position,
            LocalApprove(testedProtocolVersion),
            Set(bob),
            rootHash,
            one,
          )
          val response3b = mkResponse(
            view2Position,
            LocalApprove(testedProtocolVersion),
            Set(bob),
            rootHash,
            two,
          )
          val response3c = mkResponse(
            view2Position,
            LocalApprove(testedProtocolVersion),
            Set(bob),
            rootHash,
            three,
          )
          lazy val step3 = (for {
            p1 <- step2
              .validateAndProgress(changeTs2, response3a, topologySnapshot)
              .futureValueUS
            p2 <- p1.validateAndProgress(changeTs2, response3b, topologySnapshot).futureValueUS
            p3 <- p2.validateAndProgress(changeTs2, response3c, topologySnapshot).futureValueUS
          } yield p3).value

          it("should get an approved verdict") {
            step3 shouldBe ResponseAggregation[ViewPosition](
              requestId,
              informeeMessage,
              requestId.unwrap.plusSeconds(300),
              requestId.unwrap.plusSeconds(600),
              changeTs2,
              Left(MediatorApprove),
            )(TraceContext.empty, None)
          }

          describe("further rejection") {
            val response4 =
              mkResponse(
                view1Position,
                LocalRejectError.MalformedRejects.Payloads
                  .Reject("test4")
                  .toLocalReject(testedProtocolVersion),
                Set.empty,
                rootHash,
              )
            lazy val result =
              step3
                .validateAndProgress(requestId.unwrap.plusSeconds(2), response4, topologySnapshot)
                .futureValueUS
            it("should not allow repeated rejection") {
              result shouldBe None
            }
          }

          describe("further redundant approval") {
            val response4 = mkResponse(
              view1Position,
              LocalApprove(testedProtocolVersion),
              Set(alice),
              rootHash,
            )
            lazy val result =
              step3
                .validateAndProgress(requestId.unwrap.plusSeconds(2), response4, topologySnapshot)
                .futureValueUS
            it("should not allow repeated rejection") {
              result shouldBe None
            }
          }
        }
      }
    }
  }
}
