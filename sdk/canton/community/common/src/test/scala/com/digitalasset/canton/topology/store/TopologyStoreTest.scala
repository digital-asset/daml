// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.FailOnShutdown
import com.digitalasset.canton.config.CantonRequireTypes.{String255, String300}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  InitialTopologySnapshotValidator,
  SequencedTime,
}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.PositiveStoredTopologyTransactions
import com.digitalasset.canton.topology.store.TopologyStore.EffectiveStateChange
import com.digitalasset.canton.topology.store.TopologyTransactionRejection.InvalidTopologyMapping
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  ParticipantId,
  PartyId,
  SynchronizerId,
}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

trait TopologyStoreTest extends AsyncWordSpec with TopologyStoreTestBase with FailOnShutdown {

  val testData = new TopologyStoreTestData(testedProtocolVersion, loggerFactory, executionContext)
  import testData.*

  private lazy val submissionId = String255.tryCreate("submissionId")
  private lazy val submissionId2 = String255.tryCreate("submissionId2")
  private lazy val submissionId3 = String255.tryCreate("submissionId3")

  protected def partyMetadataStore(mk: () => PartyMetadataStore): Unit = {
    import DefaultTestIdentities.*
    "inserting new succeeds" in {
      val store = mk()
      for {
        _ <- insertOrUpdatePartyMetadata(store)(
          party1,
          Some(participant1),
          CantonTimestamp.Epoch,
          submissionId,
        )
        fetch <- store.metadataForParties(Seq(party1))
      } yield {
        fetch shouldBe NonEmpty(
          Seq,
          Some(PartyMetadata(party1, Some(participant1))(CantonTimestamp.Epoch, submissionId)),
        )
      }
    }

    "updating existing succeeds" in {
      val store = mk()
      for {
        _ <- insertOrUpdatePartyMetadata(store)(
          party1,
          None,
          CantonTimestamp.Epoch,
          submissionId,
        )
        _ <- insertOrUpdatePartyMetadata(store)(
          party2,
          None,
          CantonTimestamp.Epoch,
          submissionId,
        )
        _ <- insertOrUpdatePartyMetadata(store)(
          party1,
          Some(participant1),
          CantonTimestamp.Epoch,
          submissionId,
        )
        _ <- insertOrUpdatePartyMetadata(store)(
          party2,
          Some(participant3),
          CantonTimestamp.Epoch,
          submissionId,
        )
        metadata <- store.metadataForParties(Seq(party1, party2))
      } yield {
        metadata shouldBe NonEmpty(
          Seq,
          Some(
            PartyMetadata(party1, Some(participant1))(
              CantonTimestamp.Epoch,
              String255.empty,
            )
          ),
          Some(
            PartyMetadata(party2, Some(participant3))(
              CantonTimestamp.Epoch,
              String255.empty,
            )
          ),
        )
      }
    }

    "updating existing succeeds via batch" in {
      val store = mk()
      for {
        _ <- store.insertOrUpdatePartyMetadata(
          Seq(
            PartyMetadata(party1, None)(CantonTimestamp.Epoch, submissionId),
            PartyMetadata(party2, None)(CantonTimestamp.Epoch, submissionId),
            PartyMetadata(party1, Some(participant1))(CantonTimestamp.Epoch, submissionId),
            PartyMetadata(party2, Some(participant3))(CantonTimestamp.Epoch, submissionId),
          )
        )
        metadata <- store.metadataForParties(Seq(party1, party3, party2))
      } yield {
        metadata shouldBe NonEmpty(
          Seq,
          Some(
            PartyMetadata(party1, Some(participant1))(CantonTimestamp.Epoch, String255.empty)
          ),
          None, // checking that unknown party appears in the matching slot
          Some(
            PartyMetadata(party2, Some(participant3))(CantonTimestamp.Epoch, String255.empty)
          ),
        )
      }
    }

    "deal with delayed notifications" in {
      val store = mk()
      val rec1 =
        PartyMetadata(party1, Some(participant1))(CantonTimestamp.Epoch, submissionId)
      val rec2 =
        PartyMetadata(party2, Some(participant3))(CantonTimestamp.Epoch, submissionId2)
      val rec3 =
        PartyMetadata(party2, Some(participant1))(
          CantonTimestamp.Epoch.immediateSuccessor,
          submissionId3,
        )
      val rec4 =
        PartyMetadata(party3, Some(participant2))(CantonTimestamp.Epoch, submissionId3)
      for {
        _ <- store.insertOrUpdatePartyMetadata(Seq(rec1, rec2, rec3, rec4))
        _ <- store.markNotified(rec2.effectiveTimestamp, Seq(rec2.partyId, rec4.partyId))
        notNotified <- store.fetchNotNotified().map(_.toSet)
      } yield {
        notNotified shouldBe Set(rec1, rec3)
      }
    }

  }

  private def insertOrUpdatePartyMetadata(store: PartyMetadataStore)(
      partyId: PartyId,
      participantId: Option[ParticipantId],
      effectiveTimestamp: CantonTimestamp,
      submissionId: String255,
  ) =
    store.insertOrUpdatePartyMetadata(
      Seq(PartyMetadata(partyId, participantId)(effectiveTimestamp, submissionId))
    )

  // TODO(#14066): Test coverage is rudimentary - enough to convince ourselves that queries basically seem to work.
  //  Increase coverage.
  def topologyStore(
      mk: SynchronizerId => TopologyStore[TopologyStoreId.SynchronizerStore]
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
        ts4 -> (otk_p2_proposal, None, None),
        ts5 -> (ptp_fred_p1, None, None),
        ts5 -> (dtc_p2_synchronizer1, ts6.some, None),
        ts6 -> (dtc_p2_synchronizer1_update, None, None),
        ts6 -> (mds_med1_synchronizer1_invalid, ts6.some, s"No delegation found for keys ${seqKey.fingerprint}".some),
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
        val store1 = mk(synchronizer1_p1p2_synchronizerId)
        val store2 = mk(da_p1p2_synchronizerId)

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

      "deal with authorized transactions" when {
        "handle simple operations" in {
          val store = mk(synchronizer1_p1p2_synchronizerId)

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
              removeMapping =
                Map(dtc_p2_synchronizer1.mapping.uniqueKey -> dtc_p2_synchronizer1.serial),
            )
            _ <- update(
              store,
              ts7,
              add = Seq(mds_med1_synchronizer1, sds_seq1_synchronizer1),
              removeMapping =
                Map(mds_med1_synchronizer1.mapping.uniqueKey -> mds_med1_synchronizer1.serial),
            )
            _ <- update(
              store,
              ts8,
              add = Seq(sds_seq1_synchronizer1),
              removeMapping =
                Map(sds_seq1_synchronizer1.mapping.uniqueKey -> sds_seq1_synchronizer1.serial),
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

            txByTxHash <- store.findProposalsByTxHash(
              EffectiveTime(ts1.immediateSuccessor), // increase since exclusive
              NonEmpty(Set, dop_synchronizer1_proposal.hash),
            )
            txByMappingHash <- store.findTransactionsForMapping(
              EffectiveTime(ts2.immediateSuccessor), // increase since exclusive
              NonEmpty(Set, otk_p1.mapping.uniqueKey),
            )

            _ <- store.updateDispatchingWatermark(ts1)
            tsWatermark <- store.currentDispatchingWatermark

            _ <- update(
              store,
              ts4,
              removeMapping = Map(nsd_p1.mapping.uniqueKey -> nsd_p1.serial),
            )
            removedByMappingHash <- store.findStored(CantonTimestamp.MaxValue, nsd_p1)
            _ <- update(store, ts4, removeTxs = Set(otk_p1.hash))
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

            txByTxHash shouldBe Seq(dop_synchronizer1_proposal)
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
          val store = mk(synchronizer1_p1p2_synchronizerId)

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
          val store = mk(synchronizer1_p1p2_synchronizerId)

          for {
            _ <- new InitialTopologySnapshotValidator(
              protocolVersion = testedProtocolVersion,
              pureCrypto = testData.factory.syncCryptoClient.crypto.pureCrypto,
              store = store,
              timeouts = timeouts,
              loggerFactory = loggerFactory,
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
            onlyParticipant2 <- inspectKnownParties(store, ts6, filterParticipant = "participant2")
            neitherParty <- inspectKnownParties(store, ts6, "fred::canton", "participant2")
          } yield {
            expectTransactions(
              headStateTransactions,
              Seq(
                nsd_p1,
                nsd_p2,
                dnd_p1p2,
                dop_synchronizer1,
                otk_p1,
                p1_permission_daSynchronizer_removal,
                nsd_seq,
                dtc_p1_synchronizer1,
                dnd_p1seq,
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
            )
            onlyFred shouldBe Set(ptp_fred_p1.mapping.partyId)
            fredFullySpecified shouldBe Set(ptp_fred_p1.mapping.partyId)
            onlyParticipant2 shouldBe Set(dtc_p2_synchronizer1.mapping.participantId.adminParty)
            neitherParty shouldBe Set.empty
          }
        }

        "handle rejected transactions" in {
          val store = mk(synchronizer1_p1p2_synchronizerId)

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
              testedProtocolVersion,
              factory.syncCryptoClient.crypto.pureCrypto,
              store,
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
          val store = mk(synchronizer1_p1p2_synchronizerId)
          for {
            _ <- update(store, ts2, add = Seq(otk_p1))
            _ <- update(store, ts5, add = Seq(dtc_p2_synchronizer1))
            _ <- update(store, ts6, add = Seq(mds_med1_synchronizer1))

            transactionsAtTs6 <- store.findEssentialStateAtSequencedTime(
              asOfInclusive = SequencedTime(ts6),
              includeRejected = true,
            )
          } yield {
            expectTransactions(
              transactionsAtTs6,
              Seq(
                otk_p1,
                dtc_p2_synchronizer1,
                mds_med1_synchronizer1,
              ),
            )
          }
        }

        "able to find positive transactions" in {
          val store = mk(synchronizer1_p1p2_synchronizerId)

          for {
            _ <- new InitialTopologySnapshotValidator(
              testedProtocolVersion,
              factory.syncCryptoClient.crypto.pureCrypto,
              store,
              timeouts,
              loggerFactory,
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
                Seq(
                  ptp_fred_p1.mapping.partyId.uid,
                  dtc_p2_synchronizer1.mapping.participantId.uid,
                )
              ),
            )
            namespaceFilterTransactions <- findPositiveTransactions(
              store,
              ts6,
              filterNamespace = Some(
                Seq(dns_p1seq, p2Namespace)
              ),
            )

            essentialStateTransactions <- store.findEssentialStateAtSequencedTime(
              SequencedTime(ts6),
              includeRejected = false,
            )

            essentialStateTransactionsWithRejections <- store.findEssentialStateAtSequencedTime(
              SequencedTime(ts6),
              includeRejected = true,
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
                dnd_p1p2,
                dop_synchronizer1,
                otk_p1,
                nsd_seq,
                dtc_p1_synchronizer1,
                dnd_p1seq,
                ptp_fred_p1,
                dtc_p2_synchronizer1,
              ),
            )
            expectTransactions(
              positiveTransactionsExclusive,
              Seq(
                nsd_p1,
                nsd_p2,
                dnd_p1p2,
                dop_synchronizer1,
                otk_p1,
                nsd_seq,
                dtc_p1_synchronizer1,
                dnd_p1seq,
              ),
            )
            expectTransactions(
              positiveTransactionsInclusive,
              positiveTransactions.result.map(_.transaction),
            )
            expectTransactions(
              selectiveMappingTransactions,
              Seq(dnd_p1p2, otk_p1, dnd_p1seq, ptp_fred_p1),
            )
            expectTransactions(uidFilterTransactions, Seq(ptp_fred_p1, dtc_p2_synchronizer1))
            expectTransactions(
              namespaceFilterTransactions,
              Seq(nsd_p2, dnd_p1seq, ptp_fred_p1, dtc_p2_synchronizer1),
            )

            // Essential state currently encompasses all transactions at the specified time
            expectTransactions(
              essentialStateTransactions,
              bootstrapTransactions.result
                .filter(tx => tx.validFrom.value <= ts6 && tx.rejectionReason.isEmpty)
                .map(_.transaction),
            )

            // Essential state with rejection currently encompasses all transactions at the specified time
            expectTransactions(
              essentialStateTransactionsWithRejections,
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
                otk_p2_proposal,
                ptp_fred_p1,
                dtc_p2_synchronizer1,
                dtc_p2_synchronizer1_update,
              ),
            )

            onboardingTransactionUnlessShutdown shouldBe Seq(
              nsd_p2,
              dtc_p2_synchronizer1,
              dtc_p2_synchronizer1_update,
            )

          }
        }

        "correctly store rejected and accepted topology transactions with the same unique key within a batch" in {
          val store = mk(synchronizer1_p1p2_synchronizerId)

          // * create two transactions with the same unique key but different content.
          // * use the signatures of the transaction to accept for the transaction to reject.
          // * put the rejected transaction before accepted one in the batch to be stored.
          // => if the DB unique key is not specific enough (ie doesn't cover the content), then
          //    the accepted transaction will not be stored correctly.

          val good_otk = makeSignedTx(
            OwnerToKeyMapping(p1Id, NonEmpty(Seq, factory.SigningKeys.key1))
          )(p1Key, factory.SigningKeys.key1)

          val bad_otkTx = makeSignedTx(
            OwnerToKeyMapping(p1Id, NonEmpty(Seq, factory.EncryptionKeys.key2))
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
              removeMapping = Map.empty,
              removeTxs = Set.empty,
              additions = Seq(
                ValidatedTopologyTransaction(
                  bad_otk,
                  Some(TopologyTransactionRejection.InvalidTopologyMapping("bad signature")),
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
      }

      "compute correctly effective state changes" when {
//        import DefaultTestIdentities.*

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
          val store = mk(synchronizer1_p1p2_synchronizerId)

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
                )
                .value
            )(p1Key)
            _ <- store.update(
              SequencedTime(ts1),
              EffectiveTime(ts2),
              removeMapping = Map.empty,
              removeTxs = Set.empty,
              additions = Seq(
                ValidatedTopologyTransaction(
                  transaction = partyToParticipant1,
                  rejectionReason = None,
                ),
                ValidatedTopologyTransaction(
                  transaction = rejectedPartyToParticipant,
                  rejectionReason = Some(InvalidTopologyMapping("sad")),
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
              atTs2Result <- store.findEffectiveStateChanges(
                fromEffectiveInclusive = ts2,
                onlyAtEffective = true,
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
              assertResult(atTs2Result, Seq(resultTs2))
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
                )
                .value,
              serial = PositiveInt.one,
            )(p1Key, p2Key)
            _ <- store.update(
              SequencedTime(ts2),
              EffectiveTime(ts3),
              removeMapping = Map(
                partyToParticipant2.mapping.uniqueKey -> partyToParticipant2.serial
              ),
              removeTxs = Set.empty,
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
                )
                .value,
              op = TopologyChangeOp.Remove,
              serial = PositiveInt.tryCreate(6),
            )(p1Key)
            _ <- store.update(
              SequencedTime(ts3),
              EffectiveTime(ts4),
              removeMapping = Map(
                partyToParticipant3.mapping.uniqueKey -> partyToParticipant3.serial
              ),
              removeTxs = Set.empty,
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
                )
                .value,
              op = TopologyChangeOp.Remove,
              serial = PositiveInt.tryCreate(10),
            )(p1Key)
            _ <- store.update(
              SequencedTime(ts4),
              EffectiveTime(ts5),
              removeMapping = Map(
                partyToParticipant4.mapping.uniqueKey -> partyToParticipant4.serial
              ),
              removeTxs = Set.empty,
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
                )
                .value,
              op = TopologyChangeOp.Replace,
              serial = PositiveInt.tryCreate(11),
            )(p1Key, p3Key)
            _ <- store.update(
              SequencedTime(ts5),
              EffectiveTime(ts6),
              removeMapping =
                Map(partyToParticipant5.mapping.uniqueKey -> partyToParticipant5.serial),
              removeTxs = Set.empty,
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
    }
  }
}
