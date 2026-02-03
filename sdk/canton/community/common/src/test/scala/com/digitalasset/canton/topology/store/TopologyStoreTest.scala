// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{BatchAggregatorConfig, TopologyConfig}
import com.digitalasset.canton.crypto.topology.TopologyStateHash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.SuppressionRule.LevelAndAbove
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  InitialTopologySnapshotValidator,
  SequencedTime,
}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.{
  GenericStoredTopologyTransactions,
  PositiveStoredTopologyTransactions,
}
import com.digitalasset.canton.topology.store.TopologyStore.{EffectiveStateChange, StateKeyFetch}
import com.digitalasset.canton.topology.store.db.DbTopologyStore
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.topology.transaction.{TopologyMapping, *}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{FailOnShutdown, HasActorSystem}
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level

trait TopologyStoreTest
    extends AsyncWordSpec
    with TopologyStoreTestBase
    with FailOnShutdown
    with HasActorSystem {

  implicit def closeContext: CloseContext

  val testData = new TopologyStoreTestData(testedProtocolVersion, loggerFactory, executionContext)
  import testData.*

  // TODO(#14066): Test coverage is rudimentary - enough to convince ourselves that queries basically seem to work.
  //  Increase coverage.
  def topologyStore(
      mk: (PhysicalSynchronizerId, String) => TopologyStore[TopologyStoreId.SynchronizerStore]
  ): Unit = {

    val bootstrapTransactions = StoredTopologyTransactions(
      Seq[
        (
            CantonTimestamp,
            (GenericSignedTopologyTransaction, Option[CantonTimestamp], Option[String]),
        )
      ](
        ts1 -> (nsd_p1, None, None),
        ts1 -> (nsd_p2, None, None),
        ts1 -> (nsd_p3, None, None),
        ts1 -> (dnd_p1p2, None, None),
        ts1 -> (dop_synchronizer1_proposal, ts2.some, None),
        ts2 -> (dop_synchronizer1, None, None),
        ts2 -> (otk_p1, None, None),
        ts3 -> (p1_permission_daSynchronizer, ts3.some, None),
        ts3 -> (p1_permission_daSynchronizer_removal, None, None),
        ts3 -> (nsd_seq, None, None),
        ts3 -> (dtc_p1_synchronizer1, None, None),
        ts3 -> (ptp_fred_p1_proposal, ts5.some, None),
        ts4 -> (dnd_p1seq, None, None),
        ts4 -> (otk_p3_proposal, None, None),
        ts4 -> (otk_p2, None, None),
        ts5 -> (ptp_fred_p1, None, None),
        ts5 -> (dtc_p2_synchronizer1, ts6.some, None),
        ts6 -> (dtc_p2_synchronizer1_update, None, None),
        ts6 -> (mds_med1_synchronizer1_invalid, ts6.some,
        // mapping checks run before auth checks, so this will fail with the missing otk check
        s"Members $med1Id are missing a valid owner to key mapping.".some),
      ).map { case (from, (tx, until, rejection)) =>
        StoredTopologyTransaction(
          SequencedTime(from),
          EffectiveTime(from),
          until.map(EffectiveTime(_)),
          tx,
          rejection.map(String300.tryCreate(_)),
        )
      }
    )

    "topology store" should {

      "clear all data without affecting other stores" in {
        val store1 = mk(synchronizer1_p1p2_physicalSynchronizerId, "case1a")
        val store2 = mk(da_p1p2_physicalSynchronizerId, "case1b")

        for {
          _ <- update(store1, ts1, add = Seq(nsd_p1))
          _ <- store1.updateDispatchingWatermark(ts1)

          _ <- update(store2, ts1, add = Seq(nsd_p2))
          _ <- store2.updateDispatchingWatermark(ts1)

          _ <- store1.deleteAllData()

          watermarkStore1 <- store1.currentDispatchingWatermark
          maxTimestampStore1 <- store1.maxTimestamp(
            SequencedTime.MaxValue,
            includeRejected = true,
          )

          watermarkStore2 <- store2.currentDispatchingWatermark
          maxTimestampStore2 <- store2.maxTimestamp(
            SequencedTime.MaxValue,
            includeRejected = true,
          )

        } yield {
          maxTimestampStore1 shouldBe empty
          watermarkStore1 shouldBe empty

          maxTimestampStore2 shouldBe Some((SequencedTime(ts1), EffectiveTime(ts1)))
          watermarkStore2 shouldBe Some(ts1)
        }
      }

      "properly evolve party participant hosting" in {
        val store = mk(synchronizer1_p1p2_physicalSynchronizerId, "case2")
        def ptpFred(
            participants: HostingParticipant*
        ) =
          makeSignedTx(
            PartyToParticipant.tryCreate(
              partyId = `fred::p2Namepsace`,
              threshold = PositiveInt.one,
              participants = participants,
            )
          )(p1Key)
        val ptp1 = ptpFred(
          HostingParticipant(p1Id, ParticipantPermission.Submission)
        )
        val ptp2 = ptpFred(
          HostingParticipant(p1Id, ParticipantPermission.Submission),
          HostingParticipant(p2Id, ParticipantPermission.Confirmation, onboarding = true),
        )
        val ptp3 = ptpFred(
          HostingParticipant(p1Id, ParticipantPermission.Submission),
          HostingParticipant(p2Id, ParticipantPermission.Confirmation, onboarding = false),
        )
        for {
          _ <- update(store, ts1, add = Seq(ptp1))
          _ <- update(
            store,
            ts2,
            add = Seq(ptp2),
            removals = Map(ptp1.mapping.uniqueKey -> (None, Set(ptp1.transaction.hash))),
          )
          _ <- update(
            store,
            ts3,
            add = Seq(ptp3),
            removals = Map(ptp2.mapping.uniqueKey -> (None, Set(ptp2.transaction.hash))),
          )
          snapshot1 <- inspect(store, TimeQuery.Snapshot(ts1.immediateSuccessor))
          snapshot2 <- inspect(store, TimeQuery.Snapshot(ts2.immediateSuccessor))
          snapshot3 <- inspect(store, TimeQuery.Snapshot(ts3.immediateSuccessor))
        } yield {
          expectTransactions(snapshot1, Seq(ptp1))
          expectTransactions(snapshot2, Seq(ptp2))
          expectTransactions(snapshot3, Seq(ptp3))
        }
      }

      "properly store duplicate records" when {
        "all are sent in one batch" in {
          val store = mk(synchronizer1_p1p2_physicalSynchronizerId, "case3")
          for {
            _ <- store.update(
              SequencedTime(ts1),
              EffectiveTime(ts1),
              removals = Map(),
              additions = Seq(
                ValidatedTopologyTransaction(dop_synchronizer1_proposal, expireImmediately = true),
                ValidatedTopologyTransaction(
                  dop_synchronizer1.copy(signatures =
                    NonEmpty.from(dop_synchronizer1.signatures.drop(1)).value
                  ),
                  expireImmediately = true,
                ),
                ValidatedTopologyTransaction(
                  dop_synchronizer1
                ),
              ),
            )
            snapshot1 <- inspect(store, TimeQuery.Snapshot(ts1.immediateSuccessor))
          } yield {
            snapshot1.result.loneElement.transaction shouldBe dop_synchronizer1
            succeed
          }
        }

        "txs are sent one by one" in {
          val store = mk(synchronizer1_p1p2_physicalSynchronizerId, "case3")
          val txs = Seq(
            (ts1, ValidatedTopologyTransaction(dop_synchronizer1_proposal)),
            (
              ts2,
              ValidatedTopologyTransaction(
                dop_synchronizer1.copy(
                  signatures = NonEmpty.from(dop_synchronizer1.signatures.drop(1)).value,
                  isProposal = true,
                )
              ),
            ),
            (
              ts3,
              ValidatedTopologyTransaction(
                dop_synchronizer1
              ),
            ),
          )
          for {
            _ <- MonadUtil.sequentialTraverse_(txs) { case (ts, tx) =>
              store.update(
                SequencedTime(ts),
                EffectiveTime(ts),
                removals = Map(
                  dop_synchronizer1.mapping.uniqueKey -> (Some(dop_synchronizer1.serial), Set.empty)
                ),
                additions = Seq(tx),
              )
            }
            snapshot1 <- inspect(store, TimeQuery.Snapshot(ts1.immediateSuccessor))
            snapshot1p <- inspect(
              store,
              TimeQuery.Snapshot(ts1.immediateSuccessor),
              proposals = true,
            )
            snapshot3 <- inspect(store, TimeQuery.Snapshot(ts3.immediateSuccessor))
          } yield {
            snapshot1.result shouldBe empty
            snapshot1p.result should have length (1)
            snapshot3.result.loneElement.transaction shouldBe dop_synchronizer1
            succeed
          }
        }

      }

      "deal with authorized transactions" when {
        "handle simple operations" in {
          val store = mk(synchronizer1_p1p2_physicalSynchronizerId, "case4")

          for {
            _ <- update(store, ts1, add = Seq(nsd_p1, dop_synchronizer1_proposal))
            _ <- update(store, ts2, add = Seq(otk_p1))
            _ <- update(store, ts5, add = Seq(dtc_p2_synchronizer1))
            // in the following updates to the store, we add the same onboarding transaction twice
            // for each type of node (participant, mediator, sequencer), to test that
            // findFirstTrustCertificateForParticipant (and respectively for mediator and sequencer)
            // really finds the onboarding transaction with the lowest serial and lowest effective time.
            _ <- update(
              store,
              ts6,
              add = Seq(mds_med1_synchronizer1, dtc_p2_synchronizer1),
              removals = Map(
                dtc_p2_synchronizer1.mapping.uniqueKey -> (dtc_p2_synchronizer1.serial.some, Set.empty)
              ),
            )
            _ <- update(
              store,
              ts7,
              add = Seq(mds_med1_synchronizer1, sds_seq1_synchronizer1),
              removals = Map(
                mds_med1_synchronizer1.mapping.uniqueKey -> (mds_med1_synchronizer1.serial.some, Set.empty)
              ),
            )
            _ <- update(
              store,
              ts8,
              add = Seq(sds_seq1_synchronizer1),
              removals = Map(
                sds_seq1_synchronizer1.mapping.uniqueKey -> (sds_seq1_synchronizer1.serial.some, Set.empty)
              ),
            )

            maxTs <- store.maxTimestamp(SequencedTime.MaxValue, includeRejected = true)
            retrievedTx <- store.findStored(CantonTimestamp.MaxValue, nsd_p1)
            txProtocolVersion <- store.findStoredForVersion(
              CantonTimestamp.MaxValue,
              nsd_p1.transaction,
              ProtocolVersion.v34,
            )

            proposalTransactions <- inspect(
              store,
              TimeQuery.Range(ts1.some, ts4.some),
              proposals = true,
            )
            proposalTransactionsFiltered <- inspect(
              store,
              TimeQuery.Range(ts1.some, ts4.some),
              proposals = true,
              types = Seq(
                SynchronizerParametersState.code,
                PartyToParticipant.code,
              ), // to test the types filter
            )
            proposalTransactionsFiltered2 <- inspect(
              store,
              TimeQuery.Range(ts1.some, ts4.some),
              proposals = true,
              types = Seq(PartyToParticipant.code),
            )

            positiveProposals <- findPositiveTransactions(store, ts6, isProposal = true)

            txByMappingHash <- store.findTransactionsForMapping(
              EffectiveTime(ts2.immediateSuccessor), // increase since exclusive
              NonEmpty(Set, otk_p1.mapping.uniqueKey),
            )

            _ <- store.updateDispatchingWatermark(ts1)
            tsWatermark <- store.currentDispatchingWatermark

            _ <- update(
              store,
              ts4,
              removals = Map(nsd_p1.mapping.uniqueKey -> (nsd_p1.serial.some, Set.empty)),
            )
            removedByMappingHash <- store.findStored(CantonTimestamp.MaxValue, nsd_p1)
            _ <- update(
              store,
              ts4,
              removals = Map(otk_p1.mapping.uniqueKey -> (None, Set(otk_p1.hash))),
            )
            removedByTxHash <- store.findStored(CantonTimestamp.MaxValue, otk_p1)

            mdsTx <- store.findFirstMediatorStateForMediator(
              mds_med1_synchronizer1.mapping.active.headOption.getOrElse(fail())
            )

            dtsTx <- store.findFirstTrustCertificateForParticipant(
              dtc_p2_synchronizer1.mapping.participantId
            )
            sdsTx <- store.findFirstSequencerStateForSequencer(
              sds_seq1_synchronizer1.mapping.active.headOption.getOrElse(fail())
            )
          } yield {
            assert(maxTs.contains((SequencedTime(ts8), EffectiveTime(ts8))))
            retrievedTx.map(_.transaction) shouldBe Some(nsd_p1)
            txProtocolVersion.map(_.transaction) shouldBe Some(nsd_p1)

            expectTransactions(
              proposalTransactions,
              Seq(
                dop_synchronizer1_proposal
              ), // only proposal transaction, TimeQuery.Range is inclusive on both sides
            )
            expectTransactions(
              proposalTransactionsFiltered,
              Seq(
                dop_synchronizer1_proposal
              ),
            )
            expectTransactions(
              proposalTransactionsFiltered2,
              Nil, // no proposal transaction of type PartyToParticipant in the range
            )
            expectTransactions(positiveProposals, Seq(dop_synchronizer1_proposal))

            txByMappingHash shouldBe Seq(otk_p1)

            tsWatermark shouldBe Some(ts1)

            removedByMappingHash.flatMap(_.validUntil) shouldBe Some(EffectiveTime(ts4))
            removedByTxHash.flatMap(_.validUntil) shouldBe Some(EffectiveTime(ts4))

            dtsTx.value.transaction shouldBe dtc_p2_synchronizer1
            dtsTx.value.validFrom.value shouldBe ts5

            mdsTx.value.transaction shouldBe mds_med1_synchronizer1
            mdsTx.value.validFrom.value shouldBe ts6

            sdsTx.value.transaction shouldBe sds_seq1_synchronizer1
            sdsTx.value.validFrom.value shouldBe ts7
          }
        }
        "able to filter with inspect" in {
          val store = mk(synchronizer1_p1p2_physicalSynchronizerId, "case5")

          for {
            _ <- update(store, ts2, add = Seq(otk_p1))
            _ <- update(store, ts5, add = Seq(dtc_p2_synchronizer1))
            _ <- update(store, ts6, add = Seq(mds_med1_synchronizer1))

            proposalTransactions <- inspect(
              store,
              TimeQuery.HeadState,
            )
            proposalTransactionsFiltered <- inspect(
              store,
              TimeQuery.HeadState,
              types = Seq(
                SynchronizerTrustCertificate.code,
                OwnerToKeyMapping.code,
              ), // to test the types filter
            )
            proposalTransactionsFiltered2 <- inspect(
              store,
              TimeQuery.HeadState,
              types = Seq(PartyToParticipant.code),
            )
          } yield {
            expectTransactions(
              proposalTransactions,
              Seq(
                otk_p1,
                dtc_p2_synchronizer1,
                mds_med1_synchronizer1,
              ),
            )
            expectTransactions(
              proposalTransactionsFiltered,
              Seq(
                otk_p1,
                dtc_p2_synchronizer1,
              ),
            )
            expectTransactions(
              proposalTransactionsFiltered2,
              Nil, // no proposal transaction of type PartyToParticipant in the range
            )
          }
        }

        "able to inspect" in {
          val store = mk(synchronizer1_p1p2_physicalSynchronizerId, "case6")
          for {
            _ <- new InitialTopologySnapshotValidator(
              pureCrypto = testData.factory.syncCryptoClient.crypto.pureCrypto,
              store = store,
              BatchAggregatorConfig.defaultsForTesting,
              TopologyConfig.forTesting.copy(validateInitialTopologySnapshot = true),
              Some(defaultStaticSynchronizerParameters),
              timeouts,
              loggerFactory = loggerFactory.appendUnnamedKey("TestName", "case6"),
            ).validateAndApplyInitialTopologySnapshot(bootstrapTransactions)
              .valueOrFail("topology bootstrap")
            headStateTransactions <- inspect(store, TimeQuery.HeadState)
            rangeBetweenTs2AndTs3Transactions <- inspect(
              store,
              TimeQuery.Range(ts2.some, ts3.some),
            )
            snapshotAtTs3Transactions <- inspect(
              store,
              TimeQuery.Snapshot(ts3),
            )
            decentralizedNamespaceTransactions <- inspect(
              store,
              TimeQuery.Range(ts1.some, ts4.some),
              types = Seq(DecentralizedNamespaceDefinition.code),
            )
            removalTransactions <- inspect(
              store,
              timeQuery = TimeQuery.Range(ts1.some, ts4.some),
              op = TopologyChangeOp.Remove.some,
            )
            idP1Transactions <- inspect(
              store,
              timeQuery = TimeQuery.Range(ts1.some, ts4.some),
              idFilter = Some(p1Id.identifier.unwrap),
            )
            idNamespaceTransactions <- inspect(
              store,
              timeQuery = TimeQuery.Range(ts1.some, ts4.some),
              namespaceFilter = Some(dns_p1seq.filterString),
            )
            allParties <- inspectKnownParties(store, ts6)
            onlyFred <- inspectKnownParties(
              store,
              ts6,
              filterParty = `fred::p2Namepsace`.filterString,
            )
            fredFullySpecified <- inspectKnownParties(
              store,
              ts6,
              filterParty = `fred::p2Namepsace`.uid.toProtoPrimitive,
              filterParticipant = p1Id.uid.toProtoPrimitive,
            )
            onlyParticipant2ViaParticipantFilter <- inspectKnownParties(
              store,
              ts6,
              filterParticipant = "participant2",
            )
            onlyParticipant2ViaPartyFilter <- inspectKnownParties(
              store,
              ts6,
              filterParty = "participant2",
            )
            onlyParticipant3 <- inspectKnownParties(store, ts6, filterParticipant = "participant3")
            neitherParty <- inspectKnownParties(store, ts6, "fred::canton", "participant3")
          } yield {
            expectTransactions(
              headStateTransactions,
              Seq(
                nsd_p1,
                nsd_p2,
                nsd_p3,
                dnd_p1p2,
                dop_synchronizer1,
                otk_p1,
                p1_permission_daSynchronizer_removal,
                nsd_seq,
                dtc_p1_synchronizer1,
                dnd_p1seq,
                otk_p2,
                ptp_fred_p1,
                dtc_p2_synchronizer1_update,
              ),
            )
            expectTransactions(
              rangeBetweenTs2AndTs3Transactions,
              Seq(
                dop_synchronizer1,
                otk_p1,
                p1_permission_daSynchronizer,
                p1_permission_daSynchronizer_removal,
                nsd_seq,
                dtc_p1_synchronizer1,
              ),
            )
            expectTransactions(
              snapshotAtTs3Transactions,
              Seq(
                nsd_p1,
                nsd_p2,
                nsd_p3,
                dnd_p1p2,
                dop_synchronizer1,
                otk_p1,
              ), // tx2 include as until is inclusive, tx3 missing as from exclusive
            )
            expectTransactions(decentralizedNamespaceTransactions, Seq(dnd_p1p2, dnd_p1seq))
            expectTransactions(removalTransactions, Seq(p1_permission_daSynchronizer_removal))
            expectTransactions(
              idP1Transactions,
              Seq(
                otk_p1,
                p1_permission_daSynchronizer,
                p1_permission_daSynchronizer_removal,
                dtc_p1_synchronizer1,
              ),
            )
            expectTransactions(idNamespaceTransactions, Seq(dnd_p1seq))

            allParties shouldBe Set(
              dtc_p1_synchronizer1.mapping.participantId.adminParty,
              ptp_fred_p1.mapping.partyId,
              dtc_p2_synchronizer1.mapping.participantId.adminParty,
              // p3 cannot appear here as OTKP3 is only a proposal
            )
            onlyFred shouldBe Set(ptp_fred_p1.mapping.partyId)
            fredFullySpecified shouldBe Set(ptp_fred_p1.mapping.partyId)
            onlyParticipant2ViaParticipantFilter shouldBe Set(
              dtc_p2_synchronizer1.mapping.participantId.adminParty
            )
            onlyParticipant2ViaPartyFilter shouldBe Set(
              dtc_p2_synchronizer1.mapping.participantId.adminParty
            )
            onlyParticipant3 shouldBe Set() // p3 cannot appear as OTK3 is only a proposal
            neitherParty shouldBe Set.empty
          }
        }

        "handle rejected transactions" in {
          val store = mk(synchronizer1_p1p2_physicalSynchronizerId, "case7")

          val bootstrapTransactions = StoredTopologyTransactions(
            Seq[
              (
                  CantonTimestamp,
                  (GenericSignedTopologyTransaction, Option[CantonTimestamp], Option[String300]),
              )
            ](
              ts1 -> (nsd_p1, None, None),
              ts1 -> (otk_p1, None, None),
              ts2 -> (nsd_seq_invalid, Some(ts2), Some(
                String300.tryCreate(s"No delegation found for keys ${p1Key.fingerprint}")
              )),
            ).map { case (from, (tx, until, rejectionReason)) =>
              StoredTopologyTransaction(
                SequencedTime(from),
                EffectiveTime(from),
                until.map(EffectiveTime(_)),
                tx,
                rejectionReason,
              )
            }
          )

          for {
            _ <- new InitialTopologySnapshotValidator(
              factory.syncCryptoClient.crypto.pureCrypto,
              store,
              BatchAggregatorConfig.defaultsForTesting,
              TopologyConfig.forTesting.copy(validateInitialTopologySnapshot = true),
              Some(defaultStaticSynchronizerParameters),
              timeouts,
              loggerFactory,
            ).validateAndApplyInitialTopologySnapshot(bootstrapTransactions)
              .valueOrFail("topology bootstrap")

            headStateTransactions <- inspect(store, TimeQuery.HeadState)
          } yield {
            expectTransactions(
              headStateTransactions,
              Seq(nsd_p1, otk_p1), // tx3_NSD was rejected
            )
          }
        }

        "able to findEssentialStateAtSequencedTime" in {
          val store = mk(synchronizer1_p1p2_physicalSynchronizerId, "case8")
          for {
            _ <- update(store, ts2, add = Seq(otk_p1))
            _ <- update(store, ts5, add = Seq(dtc_p2_synchronizer1))
            _ <- update(store, ts6, add = Seq(mds_med1_synchronizer1))

            transactionsAtTs6 <- FutureUnlessShutdown.outcomeF(
              store
                .findEssentialStateAtSequencedTime(
                  asOfInclusive = SequencedTime(ts6),
                  includeRejected = true,
                )
                .runWith(Sink.seq)
            )
          } yield {
            expectTransactions(
              StoredTopologyTransactions(transactionsAtTs6),
              Seq(
                otk_p1,
                dtc_p2_synchronizer1,
                mds_med1_synchronizer1,
              ),
            )
          }
        }

        "able to correctly hash in findEssentialStateHashAtSequencedTime" in {
          val store = mk(synchronizer1_p1p2_physicalSynchronizerId, "case8.1")

          for {
            _ <- update(
              store,
              SignedTopologyTransaction.InitialTopologySequencingTime,
              add = Seq(otk_p1, dtc_p2_synchronizer1),
            )
            _ <- update(store, ts1, add = Seq(mds_med1_synchronizer1))
            initialHash <- store
              .findEssentialStateHashAtSequencedTime(
                asOfInclusive =
                  SequencedTime(SignedTopologyTransaction.InitialTopologySequencingTime)
              )
            ts1Hash <-
              loggerFactory.assertLogsSeq(LevelAndAbove(Level.DEBUG))(
                store
                  .findEssentialStateHashAtSequencedTime(
                    asOfInclusive = SequencedTime(ts1)
                  ),
                logs =>
                  store match {
                    case _: DbTopologyStore[?] =>
                      // Only the DB store caches the genesis hash
                      logs.exists(
                        _.message.contains(
                          "Reusing existing genesis topology state hash computation"
                        )
                      ) shouldBe true
                    case _ => succeed
                  },
              )
          } yield {
            val expectedInitialHash =
              TopologyStateHash.build().add(otk_p1).add(dtc_p2_synchronizer1).finish().hash
            val expectedTs1Hash = TopologyStateHash
              .build()
              .add(otk_p1)
              .add(dtc_p2_synchronizer1)
              .add(mds_med1_synchronizer1)
              .finish()
              .hash
            initialHash shouldBe expectedInitialHash
            ts1Hash shouldBe expectedTs1Hash
          }
        }

        "able to find latest vetted packages changes in order" in {
          val store = mk(da_vp123_physicalSynchronizerId, "case9a")

          def toParticipantIds(
              vps: StoredTopologyTransactions[TopologyChangeOp, VettedPackages]
          ): Seq[ParticipantId] =
            vps.result.map(_.mapping.participantId)

          def isNotSortedNaively(
              vps: StoredTopologyTransactions[TopologyChangeOp, VettedPackages]
          ): Assertion = {
            val naiveKeys = toParticipantIds(vps).map(id => id.uid.toProtoPrimitive)
            val keys = toParticipantIds(vps).map(id =>
              id.uid.identifier.toProtoPrimitive
                -> id.uid.namespace.toProtoPrimitive
            )
            naiveKeys.sorted should not equal keys.sorted
          }

          def isSorted(
              vps: StoredTopologyTransactions[TopologyChangeOp, VettedPackages]
          ): Assertion = {
            val keys = toParticipantIds(vps).map(id =>
              id.uid.identifier.toProtoPrimitive
                -> id.uid.namespace.toProtoPrimitive
            )
            keys.sorted should equal(keys)
          }

          def findLatestPagedVettingChanges(
              store: TopologyStore[TopologyStoreId.SynchronizerStore],
              participantsFilter: Option[NonEmpty[Set[ParticipantId]]],
              participantStartExclusive: Option[ParticipantId],
              pageLimit: Int,
          ): FutureUnlessShutdown[
            StoredTopologyTransactions[TopologyChangeOp.Replace, VettedPackages]
          ] =
            store
              .findPositiveTransactions(
                asOf = CantonTimestamp.MaxValue,
                asOfInclusive = true,
                isProposal = false,
                types = Seq(VettedPackages.code),
                filterUid = participantsFilter.map(_.toSeq.map(_.uid)),
                filterNamespace = None,
                pagination = Some((participantStartExclusive.map(_.uid), pageLimit)),
              )
              .map(_.collectOfMapping[VettedPackages])

          for {
            _ <- update(
              store,
              ts1,
              add = Seq(vp_vp3_synchronizer1, vp_vp2_synchronizer1, vp_vp1_synchronizer1),
            )

            vettedPackagesAll <- findLatestPagedVettingChanges(
              store = store,
              participantsFilter = None,
              participantStartExclusive = None,
              pageLimit = 1000,
            )

            vettedPackagesOnly2 <- findLatestPagedVettingChanges(
              store = store,
              participantsFilter = None,
              participantStartExclusive = None,
              pageLimit = 2,
            )

            vettedPackagesBounded <- findLatestPagedVettingChanges(
              store = store,
              participantsFilter = None,
              participantStartExclusive = Some(vp3Id),
              pageLimit = 1000,
            )

            vettedPackagesUserSpecified <- findLatestPagedVettingChanges(
              store = store,
              participantsFilter = Some(NonEmpty(Set, vp2Id, vp3Id)),
              participantStartExclusive = None,
              pageLimit = 1000,
            )

            vettedPackagesUserSpecifiedBounded <- findLatestPagedVettingChanges(
              store = store,
              participantsFilter = Some(NonEmpty(Set, vp2Id, vp3Id)),
              participantStartExclusive = Some(vp3Id),
              pageLimit = 1000,
            )
          } yield {
            vettedPackagesAll.result should have length 3
            isSorted(vettedPackagesAll)
            isNotSortedNaively(vettedPackagesAll)

            vettedPackagesOnly2.result should have length 2
            isSorted(vettedPackagesOnly2)
            toParticipantIds(vettedPackagesOnly2) should equal(Seq(vp3Id, vp2Id))

            vettedPackagesBounded.result should have length 2
            isSorted(vettedPackagesBounded)
            toParticipantIds(vettedPackagesBounded) should equal(Seq(vp2Id, vp1Id))

            vettedPackagesUserSpecified.result should have length 2
            isSorted(vettedPackagesUserSpecified)
            toParticipantIds(vettedPackagesUserSpecified) should equal(Seq(vp3Id, vp2Id))

            vettedPackagesUserSpecifiedBounded.result should have length 1
            toParticipantIds(vettedPackagesUserSpecifiedBounded) should equal(Seq(vp2Id))
          }

        }

        "able to find positive transactions" in {
          val store = mk(synchronizer1_p1p2_physicalSynchronizerId, "case9")

          for {
            _ <- new InitialTopologySnapshotValidator(
              factory.syncCryptoClient.crypto.pureCrypto,
              store,
              BatchAggregatorConfig.defaultsForTesting,
              TopologyConfig.forTesting.copy(validateInitialTopologySnapshot = true),
              Some(defaultStaticSynchronizerParameters),
              timeouts,
              loggerFactory,
              cleanupTopologySnapshot = false,
            ).validateAndApplyInitialTopologySnapshot(bootstrapTransactions)
              .valueOrFail("topology bootstrap")

            positiveTransactions <- findPositiveTransactions(store, ts6)
            positiveTransactionsExclusive <- findPositiveTransactions(
              store,
              ts5,
            )
            positiveTransactionsInclusive <- findPositiveTransactions(
              store,
              ts5,
              asOfInclusive = true,
            )
            selectiveMappingTransactions <- findPositiveTransactions(
              store,
              ts6,
              types = Seq(
                DecentralizedNamespaceDefinition.code,
                OwnerToKeyMapping.code,
                PartyToParticipant.code,
              ),
            )
            uidFilterTransactions <- findPositiveTransactions(
              store,
              ts6,
              filterUid = Some(
                NonEmpty(
                  Seq,
                  ptp_fred_p1.mapping.partyId.uid,
                  dtc_p2_synchronizer1.mapping.participantId.uid,
                )
              ),
            )
            namespaceFilterTransactions <- findPositiveTransactions(
              store,
              ts6,
              filterNamespace = Some(
                NonEmpty(Seq, dns_p1seq, p2Namespace)
              ),
            )

            essentialStateTransactions <- FutureUnlessShutdown.outcomeF(
              store
                .findEssentialStateAtSequencedTime(
                  SequencedTime(ts6),
                  includeRejected = false,
                )
                .runWith(Sink.seq)
            )

            essentialStateTransactionsWithRejections <- FutureUnlessShutdown.outcomeF(
              store
                .findEssentialStateAtSequencedTime(
                  SequencedTime(ts6),
                  includeRejected = true,
                )
                .runWith(Sink.seq)
            )

            upcomingTransactions <- store.findUpcomingEffectiveChanges(asOfInclusive = ts4)

            dispatchingTransactionsAfter <- store.findDispatchingTransactionsAfter(
              timestampExclusive = ts1,
              limit = None,
            )

            onboardingTransactionUnlessShutdown <- store
              .findParticipantOnboardingTransactions(
                p2Id,
                synchronizer1_p1p2_synchronizerId,
              )
          } yield {
            expectTransactions(
              positiveTransactions,
              Seq(
                nsd_p1,
                nsd_p2,
                nsd_p3,
                dnd_p1p2,
                dop_synchronizer1,
                otk_p1,
                nsd_seq,
                dtc_p1_synchronizer1,
                dnd_p1seq,
                otk_p2,
                ptp_fred_p1,
                dtc_p2_synchronizer1,
              ),
            )
            expectTransactions(
              positiveTransactionsExclusive,
              Seq(
                nsd_p1,
                nsd_p2,
                nsd_p3,
                dnd_p1p2,
                dop_synchronizer1,
                otk_p1,
                nsd_seq,
                dtc_p1_synchronizer1,
                dnd_p1seq,
                otk_p2,
              ),
            )
            expectTransactions(
              positiveTransactionsInclusive,
              positiveTransactions.result.map(_.transaction),
            )
            expectTransactions(
              selectiveMappingTransactions,
              Seq(dnd_p1p2, otk_p1, dnd_p1seq, otk_p2, ptp_fred_p1),
            )
            expectTransactions(
              uidFilterTransactions,
              Seq(otk_p2, ptp_fred_p1, dtc_p2_synchronizer1),
            )
            expectTransactions(
              namespaceFilterTransactions,
              Seq(nsd_p2, dnd_p1seq, otk_p2, ptp_fred_p1, dtc_p2_synchronizer1),
            )

            // Essential state currently encompasses all transactions at the specified time
            expectTransactions(
              StoredTopologyTransactions(essentialStateTransactions),
              bootstrapTransactions.result
                .filter(tx => tx.validFrom.value <= ts6 && tx.rejectionReason.isEmpty)
                .map(_.transaction),
            )

            // Essential state with rejection currently encompasses all transactions at the specified time
            expectTransactions(
              StoredTopologyTransactions(essentialStateTransactionsWithRejections),
              bootstrapTransactions.result.map(_.transaction),
            )

            upcomingTransactions shouldBe bootstrapTransactions.result.collect {
              case tx if tx.validFrom.value >= ts4 =>
                TopologyStore.Change.Other(tx.sequenced, tx.validFrom)
            }.distinct

            expectTransactions(
              dispatchingTransactionsAfter,
              Seq(
                dop_synchronizer1,
                otk_p1,
                p1_permission_daSynchronizer,
                p1_permission_daSynchronizer_removal,
                nsd_seq,
                dtc_p1_synchronizer1,
                dnd_p1seq,
                otk_p3_proposal,
                otk_p2,
                ptp_fred_p1,
                dtc_p2_synchronizer1,
                dtc_p2_synchronizer1_update,
              ),
            )

            onboardingTransactionUnlessShutdown shouldBe Seq(
              nsd_p2,
              otk_p2,
              dtc_p2_synchronizer1,
              dtc_p2_synchronizer1_update,
            )

          }
        }

        "correctly store rejected and accepted topology transactions with the same unique key within a batch" in {
          val store = mk(synchronizer1_p1p2_physicalSynchronizerId, "case10")

          // * create two transactions with the same unique key but different content.
          // * use the signatures of the transaction to accept for the transaction to reject.
          // * put the rejected transaction before accepted one in the batch to be stored.
          // => if the DB unique key is not specific enough (ie doesn't cover the content), then
          //    the accepted transaction will not be stored correctly.

          val good_otk = makeSignedTx(
            OwnerToKeyMapping.tryCreate(p1Id, NonEmpty(Seq, factory.SigningKeys.key1))
          )(p1Key, factory.SigningKeys.key1)

          val bad_otkTx = makeSignedTx(
            OwnerToKeyMapping.tryCreate(p1Id, NonEmpty(Seq, factory.EncryptionKeys.key2))
          )(p1Key, factory.SigningKeys.key2)
          val bad_otk = bad_otkTx
            .copy(signatures =
              good_otk.signatures.map(sig =>
                SingleTransactionSignature(bad_otkTx.hash, sig.signature)
              )
            )

          for {
            _ <- store.update(
              SequencedTime(ts1),
              EffectiveTime(ts1),
              removals = Map.empty,
              additions = Seq(
                ValidatedTopologyTransaction(
                  bad_otk,
                  Some(
                    TopologyTransactionRejection.RequiredMapping.InvalidTopologyMapping(
                      "bad signature"
                    )
                  ),
                ),
                ValidatedTopologyTransaction(good_otk),
              ),
            )
            txsAtTs2 <- findPositiveTransactions(
              store,
              asOf = ts2,
              types = Seq(Code.OwnerToKeyMapping),
            )
          } yield txsAtTs2.result.loneElement.transaction shouldBe good_otk
        }

        "return the right transactions on state key requests" in {
          val store = mk(synchronizer1_p1p2_physicalSynchronizerId, "case10")

          def validate(s: GenericStoredTopologyTransactions)(
              expected: Set[
                (GenericSignedTopologyTransaction, CantonTimestamp, Option[CantonTimestamp])
              ]
          ): Assertion = {
            val have = s.result
              .map(r => (r.mapping, r.validFrom.value, r.validUntil.map(_.value)))
              .toSet

            val converted = expected.map { case (a, b, c) => (a.mapping, b, c) }
            val missing = converted -- have
            val excess = have -- converted

            if (missing.nonEmpty || excess.nonEmpty) {
              logger.warn(
                s"Missing:\n  ${missing.mkString("\n  ")}\nExcess:\n  ${excess.mkString("\n  ")}"
              )
            }
            missing shouldBe empty
            excess shouldBe empty

          }

          val et = EffectiveTime(ts1)

          for {
            _ <- update(store, ts1, add = Seq(nsd_p1, nsd_p2, otk_p1))
            _ <- update(store, ts2, add = Seq(otk_p2))
            _ <- update(
              store,
              ts3,
              add = Seq(),
              removals = Map(
                nsd_p2.mapping.uniqueKey -> (Some(nsd_p2.serial), Set.empty),
                otk_p2.mapping.uniqueKey -> (Some(otk_p2.serial), Set.empty),
              ),
            )
            _ <- update(store, ts4, add = Seq(dtc_p2_synchronizer1))
            _ <- update(store, ts5, add = Seq(mds_med1_synchronizer1))
            allNsF <- store.fetchAllDescending(
              Seq(
                StateKeyFetch(nsd_p1.mapping.code, nsd_p1.mapping.namespace, None, et),
                StateKeyFetch(nsd_p2.mapping.code, nsd_p2.mapping.namespace, None, et),
              )
            )
            onlyNs1 <- store.fetchAllDescending(
              Seq(
                StateKeyFetch(nsd_p1.mapping.code, nsd_p1.mapping.namespace, None, et)
              )
            )
            mix <- store.fetchAllDescending(
              Seq(
                StateKeyFetch(nsd_p1.mapping.code, nsd_p1.mapping.namespace, None, et),
                StateKeyFetch(
                  otk_p2.mapping.code,
                  otk_p2.mapping.namespace,
                  otk_p2.mapping.maybeUid.map(_.identifier),
                  et,
                ),
              )
            )
            onlyUid <- store.fetchAllDescending(
              Seq(
                StateKeyFetch(
                  otk_p2.mapping.code,
                  otk_p2.mapping.namespace,
                  otk_p2.mapping.maybeUid.map(_.identifier),
                  et,
                ),
                StateKeyFetch(
                  otk_p1.mapping.code,
                  otk_p1.mapping.namespace,
                  otk_p1.mapping.maybeUid.map(_.identifier),
                  et,
                ),
              )
            )
          } yield {
            validate(onlyUid)(Set((otk_p2, ts2, ts3.some), (otk_p1, ts1, None)))
            validate(mix)(Set((otk_p2, ts2, ts3.some), (nsd_p1, ts1, None)))
            validate(onlyNs1)(Set((nsd_p1, ts1, None)))
            validate(allNsF)(Set((nsd_p1, ts1, None), (nsd_p2, ts1, ts3.some)))

          }
        }

      }

      "compute correctly effective state changes" when {
        def assertResult(
            actual: Seq[EffectiveStateChange],
            expected: Seq[EffectiveStateChange],
        ): Assertion = {
          type PosTxSet = Set[StoredTopologyTransaction[TopologyChangeOp.Replace, TopologyMapping]]
          val emptyPositive: PositiveStoredTopologyTransactions =
            StoredTopologyTransactions(Nil)
          def makeComparable(
              effectiveStateChange: EffectiveStateChange
          ): (EffectiveStateChange, PosTxSet, PosTxSet) =
            (
              effectiveStateChange.copy(
                before = emptyPositive, // clear for comparison
                after = emptyPositive, // clear for comparison
              ),
              effectiveStateChange.before.result.toSet,
              effectiveStateChange.after.result.toSet,
            )
          actual.map(makeComparable).toSet shouldBe expected.map(makeComparable).toSet
        }

        "store is evolving in different ways" in {
          val store = mk(synchronizer1_p1p2_physicalSynchronizerId, "case11")

          for {
            // store is empty
            atResultEmpty <- store.findEffectiveStateChanges(
              fromEffectiveInclusive = ts1,
              onlyAtEffective = true,
            )
            fromResultEmpty <- store.findEffectiveStateChanges(
              fromEffectiveInclusive = ts1,
              onlyAtEffective = false,
            )
            _ = {
              atResultEmpty shouldBe List()
              fromResultEmpty shouldBe List()
            }

            // added a party mapping
            partyToParticipant1 = makeSignedTx(
              PartyToParticipant
                .create(
                  partyId = party1,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = p1Id,
                      permission = ParticipantPermission.Submission,
                    )
                  ),
                  partySigningKeysWithThreshold = None,
                )
                .value
            )(p1Key)
            proposedPartyToParticipant = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party3,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = p1Id,
                      permission = ParticipantPermission.Submission,
                    )
                  ),
                  partySigningKeysWithThreshold = None,
                )
                .value,
              isProposal = true,
            )(p1Key)
            rejectedPartyToParticipant = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party2,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = p1Id,
                      permission = ParticipantPermission.Submission,
                    )
                  ),
                  partySigningKeysWithThreshold = None,
                )
                .value
            )(p1Key)
            _ <- store.update(
              SequencedTime(ts1),
              EffectiveTime(ts2),
              removals = Map.empty,
              additions = Seq(
                ValidatedTopologyTransaction(
                  transaction = partyToParticipant1,
                  rejectionReason = None,
                ),
                ValidatedTopologyTransaction(
                  transaction = rejectedPartyToParticipant,
                  rejectionReason =
                    Some(TopologyTransactionRejection.RequiredMapping.InvalidTopologyMapping("sad")),
                ),
                ValidatedTopologyTransaction(
                  transaction = proposedPartyToParticipant,
                  rejectionReason = None,
                ),
              ),
            )
            _ <- for {
              atTs1Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts1,
                onlyAtEffective = true,
              )

              atTs2ResultWithoutMappingFilter <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts2,
                onlyAtEffective = true,
              )
              atTs2ResultWithMappingFilter <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts2,
                onlyAtEffective = true,
                filterTypes = Some(Seq(TopologyMapping.Code.PartyToParticipant)),
              )
              // no OTK
              atTs2ResultWithMappingEmptyFilter <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts2,
                onlyAtEffective = true,
                filterTypes = Some(Seq(TopologyMapping.Code.OwnerToKeyMapping)),
              )
              atTs3Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts3,
                onlyAtEffective = true,
              )
              fromTs1Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts1,
                onlyAtEffective = false,
              )
              fromTs2Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts2,
                onlyAtEffective = false,
              )
              fromTs3Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts3,
                onlyAtEffective = false,
              )
            } yield {
              val resultTs2 = EffectiveStateChange(
                effectiveTime = EffectiveTime(ts2),
                sequencedTime = SequencedTime(ts1),
                before = StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping](
                  Seq.empty
                ),
                after = StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping](
                  Seq(
                    StoredTopologyTransaction(
                      sequenced = SequencedTime(ts1),
                      validFrom = EffectiveTime(ts2),
                      validUntil = None,
                      transaction = partyToParticipant1,
                      rejectionReason = None,
                    )
                  )
                ),
              )
              assertResult(atTs1Result, Seq.empty)

              assertResult(atTs2ResultWithoutMappingFilter, Seq(resultTs2))
              assertResult(atTs2ResultWithMappingFilter, Seq(resultTs2))
              assertResult(atTs2ResultWithMappingEmptyFilter, Seq())

              assertResult(atTs3Result, Seq.empty)
              assertResult(fromTs1Result, Seq(resultTs2))
              assertResult(fromTs2Result, Seq(resultTs2))
              assertResult(fromTs3Result, Seq.empty)
              ()
            }

            // changed a party mapping, and adding mapping for a different party
            partyToParticipant2transient1 = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party1,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = p1Id,
                      permission = ParticipantPermission.Confirmation,
                    ),
                    HostingParticipant(
                      participantId = p2Id,
                      permission = ParticipantPermission.Submission,
                    ),
                  ),
                  partySigningKeysWithThreshold = None,
                )
                .value,
              serial = PositiveInt.two,
            )(p1Key, p2Key)
            partyToParticipant2transient2 = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party1,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = p1Id,
                      permission = ParticipantPermission.Observation,
                    ),
                    HostingParticipant(
                      participantId = p2Id,
                      permission = ParticipantPermission.Submission,
                    ),
                  ),
                  partySigningKeysWithThreshold = None,
                )
                .value,
              serial = PositiveInt.three,
            )(p1Key, p2Key)
            partyToParticipant2transient3 = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party1,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = p1Id,
                      permission = ParticipantPermission.Observation,
                    ),
                    HostingParticipant(
                      participantId = p2Id,
                      permission = ParticipantPermission.Submission,
                    ),
                  ),
                  partySigningKeysWithThreshold = None,
                )
                .value,
              op = TopologyChangeOp.Remove,
              serial = PositiveInt.tryCreate(4),
            )(p1Key)
            partyToParticipant2 = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party1,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = p1Id,
                      permission = ParticipantPermission.Submission,
                    ),
                    HostingParticipant(
                      participantId = p2Id,
                      permission = ParticipantPermission.Submission,
                    ),
                  ),
                  partySigningKeysWithThreshold = None,
                )
                .value,
              serial = PositiveInt.tryCreate(5),
            )(p1Key, p2Key)
            party2ToParticipant = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party2,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = p1Id,
                      permission = ParticipantPermission.Confirmation,
                    ),
                    HostingParticipant(
                      participantId = p2Id,
                      permission = ParticipantPermission.Observation,
                    ),
                  ),
                  partySigningKeysWithThreshold = None,
                )
                .value,
              serial = PositiveInt.one,
            )(p1Key, p2Key)
            _ <- store.update(
              SequencedTime(ts2),
              EffectiveTime(ts3),
              removals = Map(
                partyToParticipant2.mapping.uniqueKey -> (Some(
                  partyToParticipant2.serial
                ), Set.empty)
              ),
              additions = Seq(
                ValidatedTopologyTransaction(
                  transaction = partyToParticipant2transient1,
                  rejectionReason = None,
                  expireImmediately = true,
                ),
                ValidatedTopologyTransaction(
                  transaction = partyToParticipant2transient2,
                  rejectionReason = None,
                  expireImmediately = true,
                ),
                ValidatedTopologyTransaction(
                  transaction = partyToParticipant2transient3,
                  rejectionReason = None,
                  expireImmediately = true,
                ),
                ValidatedTopologyTransaction(
                  transaction = partyToParticipant2,
                  rejectionReason = None,
                ),
                ValidatedTopologyTransaction(
                  transaction = party2ToParticipant,
                  rejectionReason = None,
                ),
              ),
            )
            resultTs2 = EffectiveStateChange(
              effectiveTime = EffectiveTime(ts2),
              sequencedTime = SequencedTime(ts1),
              before = StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping](
                Seq.empty
              ),
              after = StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping](
                Seq(
                  StoredTopologyTransaction(
                    sequenced = SequencedTime(ts1),
                    validFrom = EffectiveTime(ts2),
                    validUntil = Some(EffectiveTime(ts3)),
                    transaction = partyToParticipant1,
                    rejectionReason = None,
                  )
                )
              ),
            )
            _ <- for {
              atTs2Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts2,
                onlyAtEffective = true,
              )
              atTs3Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts3,
                onlyAtEffective = true,
              )
              atTs4Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts4,
                onlyAtEffective = true,
              )
              fromTs2Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts2,
                onlyAtEffective = false,
              )
              fromTs3Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts3,
                onlyAtEffective = false,
              )
              fromTs4Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts4,
                onlyAtEffective = false,
              )
            } yield {
              val resultTs3 = EffectiveStateChange(
                effectiveTime = EffectiveTime(ts3),
                sequencedTime = SequencedTime(ts2),
                before = StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping](
                  Seq(
                    StoredTopologyTransaction(
                      sequenced = SequencedTime(ts1),
                      validFrom = EffectiveTime(ts2),
                      validUntil = Some(EffectiveTime(ts3)),
                      transaction = partyToParticipant1,
                      rejectionReason = None,
                    )
                  )
                ),
                after = StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping](
                  Seq(
                    StoredTopologyTransaction(
                      sequenced = SequencedTime(ts2),
                      validFrom = EffectiveTime(ts3),
                      validUntil = None,
                      transaction = partyToParticipant2,
                      rejectionReason = None,
                    ),
                    StoredTopologyTransaction(
                      sequenced = SequencedTime(ts2),
                      validFrom = EffectiveTime(ts3),
                      validUntil = None,
                      transaction = party2ToParticipant,
                      rejectionReason = None,
                    ),
                  )
                ),
              )
              assertResult(atTs2Result, Seq(resultTs2))
              assertResult(atTs3Result, Seq(resultTs3))
              assertResult(atTs4Result, Seq.empty)
              assertResult(fromTs2Result, Seq(resultTs2, resultTs3))
              assertResult(fromTs3Result, Seq(resultTs3))
              assertResult(fromTs4Result, Seq.empty)
              ()
            }

            // remove a party mapping
            partyToParticipant3 = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party1,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = p1Id,
                      permission = ParticipantPermission.Submission,
                    ),
                    HostingParticipant(
                      participantId = p2Id,
                      permission = ParticipantPermission.Submission,
                    ),
                  ),
                  partySigningKeysWithThreshold = None,
                )
                .value,
              op = TopologyChangeOp.Remove,
              serial = PositiveInt.tryCreate(6),
            )(p1Key)
            _ <- store.update(
              SequencedTime(ts3),
              EffectiveTime(ts4),
              removals = Map(
                partyToParticipant3.mapping.uniqueKey -> (Some(
                  partyToParticipant3.serial
                ), Set.empty)
              ),
              additions = Seq(
                ValidatedTopologyTransaction(
                  transaction = partyToParticipant3,
                  rejectionReason = None,
                )
              ),
            )
            resultTs3 = EffectiveStateChange(
              effectiveTime = EffectiveTime(ts3),
              sequencedTime = SequencedTime(ts2),
              before = StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping](
                Seq(
                  StoredTopologyTransaction(
                    sequenced = SequencedTime(ts1),
                    validFrom = EffectiveTime(ts2),
                    validUntil = Some(EffectiveTime(ts3)),
                    transaction = partyToParticipant1,
                    rejectionReason = None,
                  )
                )
              ),
              after = StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping](
                Seq(
                  StoredTopologyTransaction(
                    sequenced = SequencedTime(ts2),
                    validFrom = EffectiveTime(ts3),
                    validUntil = Some(EffectiveTime(ts4)),
                    transaction = partyToParticipant2,
                    rejectionReason = None,
                  ),
                  StoredTopologyTransaction(
                    sequenced = SequencedTime(ts2),
                    validFrom = EffectiveTime(ts3),
                    validUntil = None,
                    transaction = party2ToParticipant,
                    rejectionReason = None,
                  ),
                )
              ),
            )
            resultTs4 = EffectiveStateChange(
              effectiveTime = EffectiveTime(ts4),
              sequencedTime = SequencedTime(ts3),
              before = StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping](
                Seq(
                  StoredTopologyTransaction(
                    sequenced = SequencedTime(ts2),
                    validFrom = EffectiveTime(ts3),
                    validUntil = Some(EffectiveTime(ts4)),
                    transaction = partyToParticipant2,
                    rejectionReason = None,
                  )
                )
              ),
              after = StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping](
                Seq.empty
              ),
            )
            _ <- for {
              atTs3Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts3,
                onlyAtEffective = true,
              )
              atTs4Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts4,
                onlyAtEffective = true,
              )
              atTs5Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts5,
                onlyAtEffective = true,
              )
              fromTs3Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts3,
                onlyAtEffective = false,
              )
              fromTs4Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts4,
                onlyAtEffective = false,
              )
              fromTs5Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts5,
                onlyAtEffective = false,
              )
            } yield {
              assertResult(atTs3Result, Seq(resultTs3))
              assertResult(atTs4Result, Seq(resultTs4))
              assertResult(atTs5Result, Seq.empty)
              assertResult(fromTs3Result, Seq(resultTs3, resultTs4))
              assertResult(fromTs4Result, Seq(resultTs4))
              assertResult(fromTs5Result, Seq.empty)
              ()
            }

            // add remove twice, both transient
            partyToParticipant4transient1 = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party1,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = p1Id,
                      permission = ParticipantPermission.Observation,
                    )
                  ),
                  partySigningKeysWithThreshold = None,
                )
                .value,
              op = TopologyChangeOp.Replace,
              serial = PositiveInt.tryCreate(7),
            )(p1Key)
            partyToParticipant4transient2 = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party1,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = p1Id,
                      permission = ParticipantPermission.Observation,
                    )
                  ),
                  partySigningKeysWithThreshold = None,
                )
                .value,
              op = TopologyChangeOp.Remove,
              serial = PositiveInt.tryCreate(8),
            )(p1Key)
            partyToParticipant4transient3 = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party1,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = p1Id,
                      permission = ParticipantPermission.Confirmation,
                    )
                  ),
                  partySigningKeysWithThreshold = None,
                )
                .value,
              op = TopologyChangeOp.Replace,
              serial = PositiveInt.tryCreate(9),
            )(p1Key)
            partyToParticipant4 = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party1,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = p1Id,
                      permission = ParticipantPermission.Observation,
                    )
                  ),
                  partySigningKeysWithThreshold = None,
                )
                .value,
              op = TopologyChangeOp.Remove,
              serial = PositiveInt.tryCreate(10),
            )(p1Key)
            _ <- store.update(
              SequencedTime(ts4),
              EffectiveTime(ts5),
              removals = Map(
                partyToParticipant4.mapping.uniqueKey -> (Some(
                  partyToParticipant4.serial
                ), Set.empty)
              ),
              additions = Seq(
                ValidatedTopologyTransaction(
                  transaction = partyToParticipant4transient1,
                  rejectionReason = None,
                  expireImmediately = true,
                ),
                ValidatedTopologyTransaction(
                  transaction = partyToParticipant4transient2,
                  rejectionReason = None,
                  expireImmediately = true,
                ),
                ValidatedTopologyTransaction(
                  transaction = partyToParticipant4transient3,
                  rejectionReason = None,
                  expireImmediately = true,
                ),
                ValidatedTopologyTransaction(
                  transaction = partyToParticipant4,
                  rejectionReason = None,
                ),
              ),
            )
            _ <- for {
              atTs4Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts4,
                onlyAtEffective = true,
              )
              atTs5Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts5,
                onlyAtEffective = true,
              )
              atTs6Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts6,
                onlyAtEffective = true,
              )
              fromTs4Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts4,
                onlyAtEffective = false,
              )
              fromTs5Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts5,
                onlyAtEffective = false,
              )
              fromTs6Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts6,
                onlyAtEffective = false,
              )
            } yield {
              assertResult(atTs4Result, Seq(resultTs4))
              assertResult(atTs5Result, Seq.empty)
              assertResult(atTs6Result, Seq.empty)
              assertResult(fromTs4Result, Seq(resultTs4))
              assertResult(fromTs5Result, Seq.empty)
              assertResult(fromTs6Result, Seq.empty)
              ()
            }

            // add mapping again
            partyToParticipant5 = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party1,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = p3Id,
                      permission = ParticipantPermission.Submission,
                    )
                  ),
                  partySigningKeysWithThreshold = None,
                )
                .value,
              op = TopologyChangeOp.Replace,
              serial = PositiveInt.tryCreate(11),
            )(p1Key, p3Key)
            _ <- store.update(
              SequencedTime(ts5),
              EffectiveTime(ts6),
              removals = Map(
                partyToParticipant5.mapping.uniqueKey -> (Some(
                  partyToParticipant5.serial
                ), Set.empty)
              ),
              additions = Seq(
                ValidatedTopologyTransaction(
                  transaction = partyToParticipant5,
                  rejectionReason = None,
                )
              ),
            )
            // now testing all the results
            resultTs6 = EffectiveStateChange(
              effectiveTime = EffectiveTime(ts6),
              sequencedTime = SequencedTime(ts5),
              before = StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping](
                Seq.empty
              ),
              after = StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping](
                Seq(
                  StoredTopologyTransaction(
                    sequenced = SequencedTime(ts5),
                    validFrom = EffectiveTime(ts6),
                    validUntil = None,
                    transaction = partyToParticipant5,
                    rejectionReason = None,
                  )
                )
              ),
            )
            (testTimestamps, testExpectedAtResults) = List(
              ts1 -> None,
              ts2 -> Some(resultTs2),
              ts3 -> Some(resultTs3),
              ts4 -> Some(resultTs4),
              ts5 -> None,
              ts6 -> Some(resultTs6),
              ts7 -> None,
            ).unzip
            _ <- for {
              atResults <- MonadUtil.sequentialTraverse(testTimestamps)(
                store.findEffectiveStateChanges(
                  _,
                  onlyAtEffective = true,
                )
              )
              fromResults <- MonadUtil.sequentialTraverse(testTimestamps)(
                store.findEffectiveStateChanges(
                  _,
                  onlyAtEffective = false,
                )
              )
            } yield {
              testTimestamps.zip(testExpectedAtResults).zip(atResults).foreach {
                case ((ts, expected), actual) =>
                  withClue(s"at $ts") {
                    assertResult(actual, expected.toList)
                  }
              }
              testTimestamps
                .zip(testExpectedAtResults)
                .zip(fromResults)
                .reverse
                .foldLeft(Seq.empty[EffectiveStateChange]) {
                  case (accEffectiveChanges, ((ts, expected), actual)) =>
                    withClue(s"from $ts") {
                      val acc = accEffectiveChanges ++ expected.toList
                      assertResult(actual, acc)
                      acc
                    }
                }
                .toSet shouldBe testExpectedAtResults.flatten.toSet
              ()
            }
          } yield succeed
        }
      }

      "copy the topology state from a predecessor store" in {
        val sourceStore = mk(synchronizer1_p1p2_physicalSynchronizerId, "case12")
        val successor = synchronizer1_p1p2_physicalSynchronizerId.copy(serial =
          synchronizer1_p1p2_physicalSynchronizerId.serial.increment.toNonNegative
        )
        val targetStore = mk(successor, "case12")

        val storeWithUnrelatedLSId = mk(da_vp123_physicalSynchronizerId, "case12")

        for {
          // flip source and target to trigger the not predecessor error
          notPredecessor <- loggerFactory.assertLoggedWarningsAndErrorsSeq(
            sourceStore.copyFromPredecessorSynchronizerStore(targetStore).failed,
            _.loneElement.throwable.value.getMessage should include(
              "is not a predecessor of the target synchronizer"
            ),
          )
          // attempt to copy from a non-matching LSId
          unexpectedLSId <- loggerFactory.assertLoggedWarningsAndErrorsSeq(
            targetStore
              .copyFromPredecessorSynchronizerStore(storeWithUnrelatedLSId)
              .failed,
            _.loneElement.throwable.value.getMessage should include(
              "unexpected logical synchronizer id"
            ),
          )

          _ <- new InitialTopologySnapshotValidator(
            pureCrypto = testData.factory.syncCryptoClient.crypto.pureCrypto,
            store = sourceStore,
            topologyCacheAggregatorConfig = BatchAggregatorConfig.defaultsForTesting,
            topologyConfig = TopologyConfig.forTesting,
            staticSynchronizerParameters = Some(defaultStaticSynchronizerParameters),
            timeouts,
            loggerFactory = loggerFactory.appendUnnamedKey("TestName", "case12"),
            cleanupTopologySnapshot = true,
          ).validateAndApplyInitialTopologySnapshot(bootstrapTransactions)
            .valueOrFail("topology bootstrap")

          targetDataBeforeCopy <- targetStore.dumpStoreContent()
          _ = targetDataBeforeCopy.result shouldBe empty

          _ <- targetStore.copyFromPredecessorSynchronizerStore(sourceStore)
          sourceData <- sourceStore.dumpStoreContent()
          targetData <- targetStore.dumpStoreContent()

        } yield {
          notPredecessor.getMessage should include(
            "is not a predecessor of the target synchronizer"
          )
          unexpectedLSId.getMessage should include("unexpected logical synchronizer id")

          val actual = targetData.result
          val expected = sourceData.result.view
            .filter(_.rejectionReason.isEmpty)
            .filter((stored => !stored.transaction.isProposal || stored.validUntil.isEmpty))
            .toSeq

          actual should contain theSameElementsInOrderAs expected
        }
      }

      "removals" should {

        "work by mapping and tx hash" in {
          val store = mk(synchronizer1_p1p2_physicalSynchronizerId, "caseRm1")

          for {
            _ <- update(store, ts1, add = Seq(nsd_p1, nsd_p2, otk_p1, otk_p2, otk_p2_proposal))
            _ <- update(
              store,
              ts2,
              add = Seq(nsd_p3),
              removals = Map(
                otk_p2.mapping.uniqueKey -> (Some(otk_p2.serial), Set(otk_p2.hash)),
                nsd_p2.mapping.uniqueKey -> (None, Set(nsd_p2.hash)),
              ),
            )
            _ <- update(
              store,
              ts3,
              add = Seq(),
              removals = Map(
                otk_p2.mapping.uniqueKey -> (None, Set(otk_p2.hash))
              ),
            )
            _ <- update(
              store,
              ts4,
              add = Seq(),
              removals = Map(
                otk_p2_proposal.mapping.uniqueKey -> (None, Set(otk_p2_proposal.hash))
              ),
            )
            snapshot1 <- inspect(store, TimeQuery.Snapshot(ts1.immediateSuccessor))
            snapshot2 <- inspect(store, TimeQuery.Snapshot(ts2.immediateSuccessor))
            snapshot2p <- inspect(
              store,
              TimeQuery.Snapshot(ts2.immediateSuccessor),
              proposals = true,
            )
            snapshot3p <- inspect(
              store,
              TimeQuery.Snapshot(ts3.immediateSuccessor),
              proposals = true,
            )
            snapshot4p <- inspect(
              store,
              TimeQuery.Snapshot(ts4.immediateSuccessor),
              proposals = true,
            )
          } yield {
            expectTransactions(snapshot1, Seq(nsd_p1, nsd_p2, otk_p1, otk_p2))
            expectTransactions(snapshot2, Seq(nsd_p1, otk_p1, nsd_p3))
            expectTransactions(snapshot2p, Seq(otk_p2_proposal))
            expectTransactions(snapshot3p, Seq(otk_p2_proposal))
            expectTransactions(snapshot4p, Seq())
          }

        }

      }

    }
  }
}
