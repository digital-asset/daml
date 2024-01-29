// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{HashOps, Salt, TestHash, TestSalt}
import com.digitalasset.canton.data.ViewPosition.MerkleSeqIndex
import com.digitalasset.canton.data.ViewPosition.MerkleSeqIndex.Direction
import com.digitalasset.canton.data.*
import com.digitalasset.canton.domain.mediator.MediatorVerdict.MediatorApprove
import com.digitalasset.canton.domain.mediator.ResponseAggregation.{
  ConsortiumVotingState,
  ViewState,
}
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.TrustLevel
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, LfPartyId}
import org.scalatest.funspec.PathAnyFunSpec

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.language.existentials

class ResponseAggregationTestV5 extends PathAnyFunSpec with BaseTest {

  private implicit val ec: ExecutionContext = directExecutionContext
  private lazy val localVerdictProtocolVersion =
    LocalVerdict.protocolVersionRepresentativeFor(testedProtocolVersion)

  if (testedProtocolVersion >= ProtocolVersion.v5) {
    describe(classOf[ResponseAggregation[?]].getSimpleName) {
      def b[A](i: Int): BlindedNode[A] = BlindedNode(RootHash(TestHash.digest(i)))

      val hashOps: HashOps = new SymbolicPureCrypto

      def salt(i: Int): Salt = TestSalt.generateSalt(i)

      val domainId = DefaultTestIdentities.domainId
      val mediator = MediatorRef(DefaultTestIdentities.mediator)

      val alice = ConfirmingParty(
        LfPartyId.assertFromString("alice"),
        PositiveInt.tryCreate(3),
        TrustLevel.Ordinary,
      )
      val aliceVip = ConfirmingParty(
        LfPartyId.assertFromString("alice"),
        PositiveInt.tryCreate(3),
        TrustLevel.Vip,
      )
      val bob = ConfirmingParty(
        LfPartyId.assertFromString("bob"),
        PositiveInt.tryCreate(2),
        TrustLevel.Ordinary,
      )
      val bobVip =
        ConfirmingParty(LfPartyId.assertFromString("bob"), PositiveInt.tryCreate(2), TrustLevel.Vip)
      val charlie = PlainInformee(LfPartyId.assertFromString("charlie"))
      val dave =
        ConfirmingParty(LfPartyId.assertFromString("dave"), PositiveInt.one, TrustLevel.Ordinary)
      val solo = ParticipantId("solo")
      val uno = ParticipantId("uno")
      val duo = ParticipantId("duo")
      val tre = ParticipantId("tre")

      val emptySubviews = TransactionSubviews.empty(testedProtocolVersion, hashOps)

      val viewCommonData2 =
        ViewCommonData.create(hashOps)(
          Set(bob, charlie),
          NonNegativeInt.tryCreate(2),
          salt(54170),
          testedProtocolVersion,
        )
      val viewCommonData1 =
        ViewCommonData.create(hashOps)(
          Set(alice, bob),
          NonNegativeInt.tryCreate(3),
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

      val viewVip =
        TransactionView.tryCreate(hashOps)(
          ViewCommonData.create(hashOps)(
            Set(aliceVip, bobVip, charlie),
            NonNegativeInt.tryCreate(4),
            salt(54171),
            testedProtocolVersion,
          ),
          b(9),
          emptySubviews,
          testedProtocolVersion,
        )

      val view1Position = ViewPosition(List(MerkleSeqIndex(List.empty)))
      val view2Position = ViewPosition(List(MerkleSeqIndex(List.empty), MerkleSeqIndex(List.empty)))
      val viewVipPosition = ViewPosition(List(MerkleSeqIndex(List.empty)))

      val requestId = RequestId(CantonTimestamp.Epoch)

      val commonMetadataSignatory = CommonMetadata(hashOps, testedProtocolVersion)(
        ConfirmationPolicy.Signatory,
        domainId,
        mediator,
        salt(5417),
        new UUID(0L, 0L),
      )

      def mkResponse(
          viewHash: ViewHash,
          viewPosition: ViewPosition,
          verdict: LocalVerdict,
          confirmingParties: Set[LfPartyId],
          rootHashO: Option[RootHash],
          sender: ParticipantId = solo,
      ): MediatorResponse =
        MediatorResponse.tryCreate(
          requestId,
          sender,
          Some(viewHash),
          Some(viewPosition),
          verdict,
          rootHashO,
          confirmingParties,
          domainId,
          testedProtocolVersion,
        )

      describe("under the Signatory policy") {
        def testReject() =
          LocalReject.ConsistencyRejections.LockedContracts.Reject(Seq())(
            localVerdictProtocolVersion
          )

        val fullInformeeTree =
          FullInformeeTree.tryCreate(
            GenTransactionTree.tryCreate(hashOps)(
              b(0),
              commonMetadataSignatory,
              b(2),
              MerkleSeq.fromSeq(hashOps, testedProtocolVersion)(view1 :: Nil),
            ),
            testedProtocolVersion,
          )
        val requestId = RequestId(CantonTimestamp.Epoch)
        val informeeMessage = InformeeMessage(fullInformeeTree)(testedProtocolVersion)
        val rootHash = informeeMessage.rootHash
        val someOtherRootHash = Some(RootHash(TestHash.digest(12345)))

        val topologySnapshot: TopologySnapshot = mock[TopologySnapshot]
        when(topologySnapshot.consortiumThresholds(any[Set[LfPartyId]]))
          .thenAnswer((parties: Set[LfPartyId]) =>
            Future.successful(parties.map(x => x -> PositiveInt.one).toMap)
          )

        val sut = ResponseAggregation
          .fromRequest(
            requestId,
            informeeMessage,
            testedProtocolVersion,
            topologySnapshot,
          )
          .futureValue

        it("should have initially all pending confirming parties listed") {
          sut.state shouldBe Right(
            Map(
              view1Position -> ViewState(
                Set(alice, bob),
                Map(
                  alice.party -> ConsortiumVotingState(),
                  bob.party -> ConsortiumVotingState(),
                ),
                3,
                Nil,
              ),
              view2Position -> ViewState(
                Set(bob),
                Map(bob.party -> ConsortiumVotingState()),
                2,
                Nil,
              ),
            )
          )
        }

        it("should reject responses with the wrong root hash") {
          val responseWithWrongRootHash = MediatorResponse.tryCreate(
            requestId,
            solo,
            Some(view1.viewHash),
            Some(view1Position),
            LocalApprove(testedProtocolVersion),
            someOtherRootHash,
            Set(alice.party),
            domainId,
            testedProtocolVersion,
          )
          val responseTs = requestId.unwrap.plusSeconds(1)
          val result = loggerFactory.assertLogs(
            sut
              .validateAndProgress(responseTs, responseWithWrongRootHash, topologySnapshot)
              .futureValue,
            _.shouldBeCantonError(
              MediatorError.MalformedMessage,
              _ shouldBe show"Received a mediator response at $responseTs by $solo for request $requestId with an invalid root hash ${someOtherRootHash.showValue} instead of ${rootHash.showValue}. Discarding response...",
            ),
          )
          result shouldBe None
        }

        when(topologySnapshot.canConfirm(eqTo(solo), any[LfPartyId], any[TrustLevel]))
          .thenReturn(Future.successful(true))

        describe("rejection") {
          val changeTs1 = requestId.unwrap.plusSeconds(1)

          describe("by Alice with veto rights due to her weight of 3") {
            it("rejects the transaction") {
              val response1 = mkResponse(
                view1.viewHash,
                view1Position,
                testReject(),
                Set(alice.party),
                rootHash,
              )
              val rejected1 =
                valueOrFail(
                  sut.validateAndProgress(changeTs1, response1, topologySnapshot).futureValue
                )(
                  "Alice's rejection"
                )

              rejected1 shouldBe ResponseAggregation[ViewPosition](
                requestId,
                informeeMessage,
                changeTs1,
                Left(
                  MediatorVerdict.ParticipantReject(
                    NonEmpty(List, Set(alice.party) -> testReject())
                  )
                ),
              )(TraceContext.empty)
            }
          }

          describe("by a 'light-weight' party") {
            val response1 =
              mkResponse(view1.viewHash, view1Position, testReject(), Set(bob.party), rootHash)
            lazy val rejected1 = loggerFactory.suppressWarningsAndErrors {
              valueOrFail(
                sut.validateAndProgress(changeTs1, response1, topologySnapshot).futureValue
              )(
                "Bob's rejection"
              )
            }

            it("leaves possibility of overall approval") {
              rejected1.version shouldBe changeTs1
              rejected1.state shouldBe
                Right(
                  Map(
                    view1Position -> ViewState(
                      Set(alice),
                      Map(
                        alice.party -> ConsortiumVotingState(),
                        bob.party -> ConsortiumVotingState(rejections = Set(solo)),
                      ),
                      3,
                      List(Set(bob.party) -> testReject()),
                    ),
                    view2Position -> ViewState(
                      Set(bob),
                      Map(bob.party -> ConsortiumVotingState()),
                      2,
                      Nil,
                    ),
                  )
                )
            }

            describe("a subsequent rejection that ensure no possibility of overall approval") {
              val changeTs2 = changeTs1.plusSeconds(1)
              val response2 = mkResponse(
                view1.viewHash,
                view1Position,
                testReject(),
                Set(alice.party),
                rootHash,
              )
              lazy val rejected2 =
                valueOrFail(
                  rejected1.validateAndProgress(changeTs2, response2, topologySnapshot).futureValue
                )("Alice's second rejection")
              val rejection =
                MediatorVerdict.ParticipantReject(
                  NonEmpty(List, Set(alice.party) -> testReject(), Set(bob.party) -> testReject())
                )
              it("rejects the transaction") {
                rejected2 shouldBe ResponseAggregation[ViewPosition](
                  requestId,
                  informeeMessage,
                  changeTs2,
                  Left(rejection),
                )(TraceContext.empty)
              }

              describe("further rejection") {
                val changeTs3 = changeTs2.plusSeconds(1)
                val response3 = mkResponse(
                  view1.viewHash,
                  view1Position,
                  testReject(),
                  Set(bob.party),
                  rootHash,
                )
                lazy val rejected3 =
                  rejected2.validateAndProgress(changeTs3, response3, topologySnapshot).futureValue
                it("should not rejection after finalization") {
                  rejected3 shouldBe None
                }
              }

              describe("further approval") {
                val changeTs3 = changeTs2.plusSeconds(1)
                val response3 = mkResponse(
                  view1.viewHash,
                  view1Position,
                  LocalApprove(testedProtocolVersion),
                  Set(alice.party),
                  rootHash,
                )
                lazy val rejected3 =
                  rejected2.validateAndProgress(changeTs3, response3, topologySnapshot).futureValue
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
            view1.viewHash,
            view1Position,
            LocalApprove(testedProtocolVersion),
            Set(bob.party),
            rootHash,
          )
          lazy val result =
            valueOrFail(sut.validateAndProgress(changeTs, response1, topologySnapshot).futureValue)(
              "Bob's approval"
            )
          it("should update the pending confirming parties set") {
            result.version shouldBe changeTs
            result.state shouldBe
              Right(
                Map(
                  view1Position -> ViewState(
                    Set(alice),
                    Map(
                      alice.party -> ConsortiumVotingState(),
                      bob.party -> ConsortiumVotingState(approvals = Set(solo)),
                    ),
                    1,
                    Nil,
                  ),
                  view2Position -> ViewState(
                    Set(bob),
                    Map(bob.party -> ConsortiumVotingState()),
                    2,
                    Nil,
                  ),
                )
              )
          }
          describe("if approvals meet the threshold") {
            val response2 = mkResponse(
              view1.viewHash,
              view1Position,
              LocalApprove(testedProtocolVersion),
              Set(alice.party),
              rootHash,
            )
            lazy val step2 =
              valueOrFail(
                result.validateAndProgress(changeTs, response2, topologySnapshot).futureValue
              )(
                "Alice's approval"
              )
            val response3 = mkResponse(
              view2.viewHash,
              view2Position,
              LocalApprove(testedProtocolVersion),
              Set(bob.party),
              rootHash,
            )
            lazy val step3 =
              valueOrFail(
                step2.validateAndProgress(changeTs, response3, topologySnapshot).futureValue
              )(
                "Bob's approval for view 2"
              )
            it("should get an approved verdict") {
              step3 shouldBe ResponseAggregation[ViewPosition](
                requestId,
                informeeMessage,
                changeTs,
                Left(MediatorApprove),
              )(TraceContext.empty)
            }

            describe("further rejection") {
              val response4 =
                mkResponse(
                  view1.viewHash,
                  view1Position,
                  LocalReject.MalformedRejects.Payloads
                    .Reject("test4")(localVerdictProtocolVersion),
                  Set.empty,
                  rootHash,
                )
              lazy val result = loggerFactory.assertLogs(
                step3
                  .validateAndProgress(requestId.unwrap.plusSeconds(2), response4, topologySnapshot)
                  .futureValue,
                _.shouldBeCantonErrorCode(LocalReject.MalformedRejects.Payloads),
              )
              it("should not allow repeated rejection") {
                result shouldBe None
              }
            }

            describe("further redundant approval") {
              val response4 = mkResponse(
                view1.viewHash,
                view1Position,
                LocalApprove(testedProtocolVersion),
                Set(alice.party),
                rootHash,
              )
              lazy val result =
                step3
                  .validateAndProgress(requestId.unwrap.plusSeconds(2), response4, topologySnapshot)
                  .futureValue
              it("should not allow repeated rejection") {
                result shouldBe None
              }
            }
          }
        }
      }

      describe("response Malformed") {

        val viewCommonData1 = ViewCommonData.create(hashOps)(
          Set(alice, bob, charlie),
          NonNegativeInt.tryCreate(3),
          salt(54170),
          testedProtocolVersion,
        )
        val viewCommonData2 = ViewCommonData.create(hashOps)(
          Set(alice, bob, dave),
          NonNegativeInt.tryCreate(3),
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
              b(0),
              commonMetadataSignatory,
              b(2),
              MerkleSeq.fromSeq(hashOps, testedProtocolVersion)(view1 :: view2 :: Nil),
            ),
            testedProtocolVersion,
          )
        )(testedProtocolVersion)

        val view1Position = ViewPosition(List(MerkleSeqIndex(List(Direction.Left))))
        val view2Position = ViewPosition(List(MerkleSeqIndex(List(Direction.Right))))

        val topologySnapshot: TopologySnapshot = mock[TopologySnapshot]
        when(topologySnapshot.consortiumThresholds(any[Set[LfPartyId]]))
          .thenAnswer((parties: Set[LfPartyId]) =>
            Future.successful(parties.map(x => x -> PositiveInt.one).toMap)
          )
        when(topologySnapshot.canConfirm(eqTo(solo), eqTo(bob.party), any[TrustLevel]))
          .thenReturn(Future.successful(true))
        when(topologySnapshot.canConfirm(eqTo(solo), eqTo(dave.party), any[TrustLevel]))
          .thenReturn(Future.successful(true))
        when(topologySnapshot.canConfirm(eqTo(solo), eqTo(alice.party), any[TrustLevel]))
          .thenReturn(Future.successful(false))

        val sut = ResponseAggregation
          .fromRequest(
            requestId,
            informeeMessage,
            testedProtocolVersion,
            topologySnapshot,
          )
          .futureValue
        lazy val changeTs = requestId.unwrap.plusSeconds(1)

        def testReject(reason: String) =
          LocalReject.MalformedRejects.Payloads.Reject(reason)(localVerdictProtocolVersion)

        describe("for a single view") {
          it("should update the pending confirming parties set for all hosted parties") {
            val response = mkResponse(
              view1.viewHash,
              view1Position,
              testReject("malformed view"),
              Set.empty,
              None,
            )
            val result = loggerFactory.assertLogs(
              valueOrFail(
                sut.validateAndProgress(changeTs, response, topologySnapshot).futureValue
              )(
                "Malformed response for a view hash"
              ),
              _.shouldBeCantonError(
                LocalReject.MalformedRejects.Payloads,
                _ shouldBe "Rejected transaction due to malformed payload within views malformed view",
              ),
            )

            result.version shouldBe changeTs
            result.state shouldBe Right(
              Map(
                view1Position -> ViewState(
                  Set(alice),
                  Map(
                    alice.party -> ConsortiumVotingState(),
                    bob.party -> ConsortiumVotingState(rejections = Set(solo)),
                  ),
                  3,
                  List(Set(bob.party) -> testReject("malformed view")),
                ),
                view2Position -> ViewState(
                  Set(alice, bob, dave),
                  Map(
                    alice.party -> ConsortiumVotingState(),
                    bob.party -> ConsortiumVotingState(),
                    dave.party -> ConsortiumVotingState(),
                  ),
                  3,
                  Nil,
                ),
              )
            )
          }
        }

        describe("without a view hash") {
          it("should update the pending confirming parties for all hosted parties in all views") {
            val rejectMsg = "malformed request"
            val response =
              MediatorResponse.tryCreate(
                requestId,
                solo,
                None,
                None,
                testReject(rejectMsg),
                None,
                Set.empty,
                domainId,
                testedProtocolVersion,
              )
            val result = loggerFactory.assertLogs(
              valueOrFail(
                sut.validateAndProgress(changeTs, response, topologySnapshot).futureValue
              )(
                "Malformed response without view hash"
              ),
              _.shouldBeCantonError(
                LocalReject.MalformedRejects.Payloads,
                _ shouldBe s"Rejected transaction due to malformed payload within views $rejectMsg",
                _ should (contain("reportedBy" -> s"$solo") and contain(
                  "requestId" -> requestId.toString
                )),
              ),
            )
            result.version shouldBe changeTs
            result.state shouldBe Right(
              Map(
                view1Position -> ViewState(
                  Set(alice),
                  Map(
                    alice.party -> ConsortiumVotingState(),
                    bob.party -> ConsortiumVotingState(rejections = Set(solo)),
                  ),
                  3,
                  List(Set(bob.party) -> testReject(rejectMsg)),
                ),
                view2Position -> ViewState(
                  Set(alice),
                  Map(
                    alice.party -> ConsortiumVotingState(),
                    bob.party -> ConsortiumVotingState(rejections = Set(solo)),
                    dave.party -> ConsortiumVotingState(rejections = Set(solo)),
                  ),
                  3,
                  List(Set(bob.party, dave.party) -> testReject(rejectMsg)),
                ),
              )
            )
          }
        }
      }

      describe("under the VIP policy") {
        val commonMetadata = CommonMetadata(hashOps, testedProtocolVersion)(
          ConfirmationPolicy.Vip,
          domainId,
          mediator,
          salt(5417),
          new UUID(0L, 0L),
        )

        val fullInformeeTree = FullInformeeTree.tryCreate(
          GenTransactionTree.tryCreate(hashOps)(
            b(0),
            commonMetadata,
            b(2),
            MerkleSeq.fromSeq(hashOps, testedProtocolVersion)(viewVip :: Nil),
          ),
          testedProtocolVersion,
        )
        val informeeMessage = InformeeMessage(fullInformeeTree)(testedProtocolVersion)
        val rootHash = informeeMessage.rootHash
        val nonVip = ParticipantId("notAVip")

        val topologySnapshotVip: TopologySnapshot = mock[TopologySnapshot]
        when(topologySnapshotVip.consortiumThresholds(any[Set[LfPartyId]]))
          .thenAnswer((parties: Set[LfPartyId]) =>
            Future.successful(parties.map(x => x -> PositiveInt.one).toMap)
          )
        when(topologySnapshotVip.canConfirm(eqTo(solo), eqTo(alice.party), eqTo(TrustLevel.Vip)))
          .thenReturn(Future.successful(true))
        when(topologySnapshotVip.canConfirm(eqTo(solo), eqTo(bob.party), eqTo(TrustLevel.Vip)))
          .thenReturn(Future.successful(false))
        when(topologySnapshotVip.canConfirm(eqTo(solo), eqTo(bob.party), eqTo(TrustLevel.Ordinary)))
          .thenReturn(Future.successful(true))
        when(topologySnapshotVip.canConfirm(eqTo(nonVip), any[LfPartyId], eqTo(TrustLevel.Vip)))
          .thenReturn(Future.successful(false))
        when(
          topologySnapshotVip.canConfirm(eqTo(nonVip), any[LfPartyId], eqTo(TrustLevel.Ordinary))
        )
          .thenReturn(Future.successful(true))

        val sut = ResponseAggregation
          .fromRequest(
            requestId,
            informeeMessage,
            testedProtocolVersion,
            topologySnapshotVip,
          )
          .futureValue
        val initialState =
          Map(
            viewVipPosition -> ViewState(
              pendingConfirmingParties = Set(aliceVip, bobVip),
              consortiumVoting = Map(
                alice.party -> ConsortiumVotingState(),
                bob.party -> ConsortiumVotingState(),
              ),
              distanceToThreshold = viewVip.viewCommonData.tryUnwrap.threshold.unwrap,
              rejections = Nil,
            )
          )

        it("should have all pending confirming parties listed") {
          sut.state shouldBe Right(initialState)
        }

        it("should reject non-VIP responses") {
          val response =
            mkResponse(
              viewVip.viewHash,
              viewVipPosition,
              LocalApprove(testedProtocolVersion),
              Set(alice.party, bob.party),
              rootHash,
            )

          val responseTs = requestId.unwrap.plusSeconds(1)
          loggerFactory.assertLogs(
            sut
              .validateAndProgress(responseTs, response, topologySnapshotVip)
              .futureValue shouldBe None,
            _.shouldBeCantonError(
              MediatorError.MalformedMessage,
              _ shouldBe show"Received an unauthorized mediator response at $responseTs by $solo for request $requestId on behalf of Set(bob). Discarding response...",
            ),
          )
        }

        it("should ignore malformed non-VIP responses") {
          val rejectMsg = "malformed request"
          val reject =
            LocalReject.MalformedRejects.Payloads.Reject(rejectMsg)(localVerdictProtocolVersion)
          val response =
            MediatorResponse.tryCreate(
              requestId,
              nonVip,
              None,
              None,
              reject,
              None,
              Set.empty,
              domainId,
              testedProtocolVersion,
            )
          val result = loggerFactory.assertLogs(
            valueOrFail(
              sut
                .validateAndProgress(requestId.unwrap.plusSeconds(1), response, topologySnapshotVip)
                .futureValue
            )(s"$nonVip responds Malformed"),
            _.shouldBeCantonError(
              LocalReject.MalformedRejects.Payloads,
              _ shouldBe s"Rejected transaction due to malformed payload within views $rejectMsg",
              _ should (contain("reportedBy" -> s"$nonVip") and contain(
                "requestId" -> requestId.toString
              )),
            ),
          )
          result.state shouldBe Right(initialState)
        }

        it("should accept VIP responses") {
          val changeTs = requestId.unwrap.plusSeconds(1)
          val response = mkResponse(
            viewVip.viewHash,
            viewVipPosition,
            LocalApprove(testedProtocolVersion),
            Set(alice.party),
            rootHash,
          )
          val result = valueOrFail(
            sut.validateAndProgress(changeTs, response, topologySnapshotVip).futureValue
          )("solo confirms with VIP trust level for alice")

          result.version shouldBe changeTs
          result.state shouldBe
            Right(
              Map(
                viewVipPosition -> ViewState(
                  Set(bobVip),
                  Map(
                    alice.party -> ConsortiumVotingState(approvals = Set(solo)),
                    bob.party -> ConsortiumVotingState(),
                  ),
                  1,
                  Nil,
                )
              )
            )
        }
      }

      describe("consortium state") {
        it("should work for threshold = 1") {
          ConsortiumVotingState(approvals = Set(solo)).isApproved shouldBe (true)
          ConsortiumVotingState(approvals = Set(solo)).isRejected shouldBe (false)
          ConsortiumVotingState(rejections = Set(solo)).isApproved shouldBe (false)
          ConsortiumVotingState(rejections = Set(solo)).isRejected shouldBe (true)
        }

        it("should work for threshold >= 2") {
          ConsortiumVotingState(
            PositiveInt.tryCreate(2),
            approvals = Set(uno),
          ).isApproved shouldBe (false)
          ConsortiumVotingState(
            PositiveInt.tryCreate(2),
            approvals = Set(uno),
          ).isRejected shouldBe (false)
          ConsortiumVotingState(
            PositiveInt.tryCreate(2),
            approvals = Set(uno, duo),
          ).isApproved shouldBe (true)
          ConsortiumVotingState(
            PositiveInt.tryCreate(2),
            approvals = Set(uno, duo),
            rejections = Set(tre),
          ).isApproved shouldBe (true)
          ConsortiumVotingState(
            PositiveInt.tryCreate(2),
            approvals = Set(uno),
            rejections = Set(duo, tre),
          ).isApproved shouldBe (false)
          ConsortiumVotingState(
            PositiveInt.tryCreate(2),
            approvals = Set(uno),
            rejections = Set(duo, tre),
          ).isRejected shouldBe (true)
          ConsortiumVotingState(
            PositiveInt.tryCreate(3),
            approvals = Set(uno),
            rejections = Set(duo, tre),
          ).isApproved shouldBe (false)
          ConsortiumVotingState(
            PositiveInt.tryCreate(3),
            approvals = Set(uno),
            rejections = Set(duo, tre),
          ).isRejected shouldBe (false)
          ConsortiumVotingState(
            PositiveInt.tryCreate(3),
            approvals = Set(uno, duo, tre),
          ).isApproved shouldBe (true)
          ConsortiumVotingState(
            PositiveInt.tryCreate(3),
            rejections = Set(uno, duo, tre),
          ).isRejected shouldBe (true)
        }
      }

      describe("consortium voting") {
        def testReject() =
          LocalReject.ConsistencyRejections.LockedContracts.Reject(Seq())(
            localVerdictProtocolVersion
          )

        val fullInformeeTree =
          FullInformeeTree.tryCreate(
            GenTransactionTree.tryCreate(hashOps)(
              b(0),
              commonMetadataSignatory,
              b(2),
              MerkleSeq.fromSeq(hashOps, testedProtocolVersion)(view1 :: Nil),
            ),
            testedProtocolVersion,
          )
        val requestId = RequestId(CantonTimestamp.Epoch)
        val informeeMessage = InformeeMessage(fullInformeeTree)(testedProtocolVersion)
        val rootHash = informeeMessage.rootHash

        val topologySnapshot: TopologySnapshot = mock[TopologySnapshot]
        when(topologySnapshot.consortiumThresholds(any[Set[LfPartyId]]))
          .thenAnswer((parties: Set[LfPartyId]) =>
            Future.successful(
              Map(
                alice.party -> PositiveInt.tryCreate(2),
                bob.party -> PositiveInt.tryCreate(3),
              ).view.filterKeys(parties.contains).toMap
            )
          )

        val sut = ResponseAggregation
          .fromRequest(
            requestId,
            informeeMessage,
            testedProtocolVersion,
            topologySnapshot,
          )
          .futureValue

        it("should correctly initialize the state") {
          sut.state shouldBe Right(
            Map(
              view1Position -> ViewState(
                Set(alice, bob),
                Map(
                  alice.party -> ConsortiumVotingState(PositiveInt.tryCreate(2)),
                  bob.party -> ConsortiumVotingState(PositiveInt.tryCreate(3)),
                ),
                3,
                Nil,
              ),
              view2Position -> ViewState(
                Set(bob),
                Map(bob.party -> ConsortiumVotingState(PositiveInt.tryCreate(3))),
                2,
                Nil,
              ),
            )
          )
        }

        when(topologySnapshot.canConfirm(any[ParticipantId], any[LfPartyId], any[TrustLevel]))
          .thenReturn(Future.successful(true))

        describe("should prevent response stuffing") {
          describe("for reject by Bob with 3 votes from the same participant") {

            val changeTs1 = requestId.unwrap.plusSeconds(1)
            val changeTs2 = requestId.unwrap.plusSeconds(2)
            val changeTs3 = requestId.unwrap.plusSeconds(3)

            val response1a = mkResponse(
              view1.viewHash,
              view1Position,
              testReject(),
              Set(bob.party),
              rootHash,
              uno,
            )
            val response1b = mkResponse(
              view1.viewHash,
              view1Position,
              testReject(),
              Set(bob.party),
              rootHash,
              uno,
            )
            val response1c = mkResponse(
              view1.viewHash,
              view1Position,
              testReject(),
              Set(bob.party),
              rootHash,
              uno,
            )
            lazy val rejected1 = loggerFactory.suppressWarningsAndErrors {
              valueOrFail(
                for {
                  p1 <- sut.validateAndProgress(changeTs1, response1a, topologySnapshot).futureValue
                  p2 <- p1.validateAndProgress(changeTs2, response1b, topologySnapshot).futureValue
                  p3 <- p2.validateAndProgress(changeTs3, response1c, topologySnapshot).futureValue
                } yield p3
              )(
                "Bob's rejections"
              )
            }

            it("should count Bob's vote only once") {
              rejected1.version shouldBe changeTs3
              rejected1.state shouldBe
                Right(
                  Map(
                    view1Position -> ViewState(
                      Set(alice, bob),
                      Map(
                        alice.party -> ConsortiumVotingState(PositiveInt.tryCreate(2)),
                        bob.party -> ConsortiumVotingState(
                          PositiveInt.tryCreate(3),
                          rejections = Set(uno),
                        ),
                      ),
                      3,
                      Nil,
                    ),
                    view2Position -> ViewState(
                      Set(bob),
                      Map(bob.party -> ConsortiumVotingState(PositiveInt.tryCreate(3))),
                      2,
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
              view1.viewHash,
              view1Position,
              LocalApprove(testedProtocolVersion),
              Set(bob.party),
              rootHash,
              uno,
            )
            val response1b = mkResponse(
              view1.viewHash,
              view1Position,
              LocalApprove(testedProtocolVersion),
              Set(bob.party),
              rootHash,
              uno,
            )
            val response1c = mkResponse(
              view1.viewHash,
              view1Position,
              LocalApprove(testedProtocolVersion),
              Set(bob.party),
              rootHash,
              uno,
            )
            lazy val result =
              valueOrFail(
                for {
                  p1 <- sut.validateAndProgress(changeTs1, response1a, topologySnapshot).futureValue
                  p2 <- p1.validateAndProgress(changeTs2, response1b, topologySnapshot).futureValue
                  p3 <- p2.validateAndProgress(changeTs3, response1c, topologySnapshot).futureValue
                } yield p3
              )(
                "Bob's approval"
              )
            it("should count Bob's vote only once") {
              result.version shouldBe changeTs3
              result.state shouldBe
                Right(
                  Map(
                    view1Position -> ViewState(
                      Set(alice, bob),
                      Map(
                        alice.party -> ConsortiumVotingState(PositiveInt.tryCreate(2)),
                        bob.party -> ConsortiumVotingState(
                          PositiveInt.tryCreate(3),
                          approvals = Set(uno),
                        ),
                      ),
                      3,
                      Nil,
                    ),
                    view2Position -> ViewState(
                      Set(bob),
                      Map(bob.party -> ConsortiumVotingState(PositiveInt.tryCreate(3))),
                      2,
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
                  view1.viewHash,
                  view1Position,
                  testReject(),
                  Set(alice.party),
                  rootHash,
                  uno,
                )
              val response1b =
                mkResponse(
                  view1.viewHash,
                  view1Position,
                  testReject(),
                  Set(alice.party),
                  rootHash,
                  duo,
                )
              val rejected1a =
                valueOrFail(
                  sut.validateAndProgress(changeTs1, response1a, topologySnapshot).futureValue
                )(
                  "Alice's rejection (uno)"
                )
              val rejected1b =
                valueOrFail(
                  rejected1a
                    .validateAndProgress(changeTs2, response1b, topologySnapshot)
                    .futureValue
                )(
                  "Alice's rejection (duo)"
                )

              rejected1a.state shouldBe Right(
                Map(
                  view1Position -> ViewState(
                    Set(alice, bob),
                    Map(
                      alice.party -> ConsortiumVotingState(
                        PositiveInt.tryCreate(2),
                        rejections = Set(uno),
                      ),
                      bob.party -> ConsortiumVotingState(PositiveInt.tryCreate(3)),
                    ),
                    3,
                    Nil,
                  ),
                  view2Position -> ViewState(
                    Set(bob),
                    Map(bob.party -> ConsortiumVotingState(PositiveInt.tryCreate(3))),
                    2,
                    Nil,
                  ),
                )
              )

              rejected1b shouldBe ResponseAggregation[ViewPosition](
                requestId,
                informeeMessage,
                changeTs2,
                Left(
                  MediatorVerdict.ParticipantReject(
                    NonEmpty(List, Set(alice.party) -> testReject())
                  )
                ),
              )(TraceContext.empty)
            }
          }

          describe("by Bob with 3 votes") {
            val response1a = mkResponse(
              view1.viewHash,
              view1Position,
              testReject(),
              Set(bob.party),
              rootHash,
              uno,
            )
            val response1b = mkResponse(
              view1.viewHash,
              view1Position,
              testReject(),
              Set(bob.party),
              rootHash,
              duo,
            )
            val response1c = mkResponse(
              view1.viewHash,
              view1Position,
              testReject(),
              Set(bob.party),
              rootHash,
              tre,
            )
            lazy val rejected1 = loggerFactory.suppressWarningsAndErrors {
              valueOrFail(
                for {
                  p1 <- sut.validateAndProgress(changeTs1, response1a, topologySnapshot).futureValue
                  p2 <- p1.validateAndProgress(changeTs2, response1b, topologySnapshot).futureValue
                  p3 <- p2.validateAndProgress(changeTs3, response1c, topologySnapshot).futureValue
                } yield p3
              )(
                "Bob's rejections"
              )
            }

            it("not rejected due to Alice's heavier weight") {
              rejected1.version shouldBe changeTs3
              rejected1.state shouldBe
                Right(
                  Map(
                    view1Position -> ViewState(
                      Set(alice),
                      Map(
                        alice.party -> ConsortiumVotingState(PositiveInt.tryCreate(2)),
                        bob.party -> ConsortiumVotingState(
                          PositiveInt.tryCreate(3),
                          rejections = Set(uno, duo, tre),
                        ),
                      ),
                      3,
                      List(Set(bob.party) -> testReject()),
                    ),
                    view2Position -> ViewState(
                      Set(bob),
                      Map(bob.party -> ConsortiumVotingState(PositiveInt.tryCreate(3))),
                      2,
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
                  view1.viewHash,
                  view1Position,
                  testReject(),
                  Set(alice.party),
                  rootHash,
                  uno,
                )
              val response2b =
                mkResponse(
                  view1.viewHash,
                  view1Position,
                  testReject(),
                  Set(alice.party),
                  rootHash,
                  duo,
                )
              lazy val rejected2 =
                valueOrFail(
                  for {
                    p1 <- rejected1
                      .validateAndProgress(changeTs4, response2a, topologySnapshot)
                      .futureValue
                    p2 <- p1
                      .validateAndProgress(changeTs5, response2b, topologySnapshot)
                      .futureValue
                  } yield p2
                )("Alice's second rejection")
              val rejection =
                MediatorVerdict.ParticipantReject(
                  NonEmpty(List, Set(alice.party) -> testReject(), Set(bob.party) -> testReject())
                )
              it("rejects the transaction") {
                rejected2 shouldBe ResponseAggregation[ViewPosition](
                  requestId,
                  informeeMessage,
                  changeTs5,
                  Left(rejection),
                )(TraceContext.empty)
              }

              describe("further rejection") {
                val changeTs6 = changeTs5.plusSeconds(1)
                val response3 = mkResponse(
                  view1.viewHash,
                  view1Position,
                  testReject(),
                  Set(bob.party),
                  rootHash,
                )
                lazy val rejected3 =
                  rejected2.validateAndProgress(changeTs6, response3, topologySnapshot).futureValue
                it("should not rejection after finalization") {
                  rejected3 shouldBe None
                }
              }

              describe("further approval") {
                val changeTs6 = changeTs5.plusSeconds(1)
                val response3 = mkResponse(
                  view1.viewHash,
                  view1Position,
                  LocalApprove(testedProtocolVersion),
                  Set(alice.party),
                  rootHash,
                )
                lazy val rejected3 =
                  rejected2.validateAndProgress(changeTs6, response3, topologySnapshot).futureValue
                it("should not allow approval after finalization") {
                  rejected3 shouldBe None
                }
              }
            }
          }
        }

        describe("approval") {
          lazy val changeTs1 = requestId.unwrap.plusSeconds(1)
          lazy val changeTs2 = requestId.unwrap.plusSeconds(2)
          lazy val changeTs3 = requestId.unwrap.plusSeconds(3)
          val response1a = mkResponse(
            view1.viewHash,
            view1Position,
            LocalApprove(testedProtocolVersion),
            Set(bob.party),
            rootHash,
            uno,
          )
          val response1b = mkResponse(
            view1.viewHash,
            view1Position,
            LocalApprove(testedProtocolVersion),
            Set(bob.party),
            rootHash,
            duo,
          )
          val response1c = mkResponse(
            view1.viewHash,
            view1Position,
            LocalApprove(testedProtocolVersion),
            Set(bob.party),
            rootHash,
            tre,
          )
          lazy val result =
            valueOrFail(
              for {
                p1 <- sut.validateAndProgress(changeTs1, response1a, topologySnapshot).futureValue
                p2 <- p1.validateAndProgress(changeTs2, response1b, topologySnapshot).futureValue
                p3 <- p2.validateAndProgress(changeTs3, response1c, topologySnapshot).futureValue
              } yield p3
            )(
              "Bob's approval"
            )
          it("should update the pending confirming parties set") {
            result.version shouldBe changeTs3
            result.state shouldBe
              Right(
                Map(
                  view1Position -> ViewState(
                    Set(alice),
                    Map(
                      alice.party -> ConsortiumVotingState(PositiveInt.tryCreate(2)),
                      bob.party -> ConsortiumVotingState(
                        PositiveInt.tryCreate(3),
                        approvals = Set(uno, duo, tre),
                      ),
                    ),
                    1,
                    Nil,
                  ),
                  view2Position -> ViewState(
                    Set(bob),
                    Map(bob.party -> ConsortiumVotingState(PositiveInt.tryCreate(3))),
                    2,
                    Nil,
                  ),
                )
              )
          }
          describe("if approvals meet the threshold") {
            val response2a = mkResponse(
              view1.viewHash,
              view1Position,
              LocalApprove(testedProtocolVersion),
              Set(alice.party),
              rootHash,
              uno,
            )
            val response2b = mkResponse(
              view1.viewHash,
              view1Position,
              LocalApprove(testedProtocolVersion),
              Set(alice.party),
              rootHash,
              duo,
            )
            lazy val step2 =
              valueOrFail(
                for {
                  p1 <- result
                    .validateAndProgress(changeTs2, response2a, topologySnapshot)
                    .futureValue
                  p2 <- p1.validateAndProgress(changeTs2, response2b, topologySnapshot).futureValue
                } yield p2
              )(
                "Alice's approval"
              )
            val response3a = mkResponse(
              view2.viewHash,
              view2Position,
              LocalApprove(testedProtocolVersion),
              Set(bob.party),
              rootHash,
              uno,
            )
            val response3b = mkResponse(
              view2.viewHash,
              view2Position,
              LocalApprove(testedProtocolVersion),
              Set(bob.party),
              rootHash,
              duo,
            )
            val response3c = mkResponse(
              view2.viewHash,
              view2Position,
              LocalApprove(testedProtocolVersion),
              Set(bob.party),
              rootHash,
              tre,
            )
            lazy val step3 =
              valueOrFail(
                for {
                  p1 <- step2
                    .validateAndProgress(changeTs2, response3a, topologySnapshot)
                    .futureValue
                  p2 <- p1.validateAndProgress(changeTs2, response3b, topologySnapshot).futureValue
                  p3 <- p2.validateAndProgress(changeTs2, response3c, topologySnapshot).futureValue
                } yield p3
              )(
                "Bob's approval for view 2"
              )
            it("should get an approved verdict") {
              step3 shouldBe ResponseAggregation[ViewPosition](
                requestId,
                informeeMessage,
                changeTs2,
                Left(MediatorApprove),
              )(TraceContext.empty)
            }

            describe("further rejection") {
              val response4 =
                mkResponse(
                  view1.viewHash,
                  view1Position,
                  LocalReject.MalformedRejects.Payloads
                    .Reject("test4")(localVerdictProtocolVersion),
                  Set.empty,
                  rootHash,
                )
              lazy val result = loggerFactory.assertLogs(
                step3
                  .validateAndProgress(requestId.unwrap.plusSeconds(2), response4, topologySnapshot)
                  .futureValue,
                _.shouldBeCantonErrorCode(LocalReject.MalformedRejects.Payloads),
              )
              it("should not allow repeated rejection") {
                result shouldBe None
              }
            }

            describe("further redundant approval") {
              val response4 = mkResponse(
                view1.viewHash,
                view1Position,
                LocalApprove(testedProtocolVersion),
                Set(alice.party),
                rootHash,
              )
              lazy val result =
                step3
                  .validateAndProgress(requestId.unwrap.plusSeconds(2), response4, topologySnapshot)
                  .futureValue
              it("should not allow repeated rejection") {
                result shouldBe None
              }
            }
          }
        }
      }
    }
  }
}
