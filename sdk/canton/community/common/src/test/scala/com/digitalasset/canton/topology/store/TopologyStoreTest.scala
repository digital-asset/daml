// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.FailOnShutdown
import com.digitalasset.canton.config.CantonRequireTypes.{String255, String256M}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.PositiveStoredTopologyTransactions
import com.digitalasset.canton.topology.store.TopologyStore.EffectiveStateChange
import com.digitalasset.canton.topology.store.TopologyTransactionRejection.InvalidTopologyMapping
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.topology.{DefaultTestIdentities, ParticipantId, PartyId}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

trait TopologyStoreTest extends AsyncWordSpec with TopologyStoreTestBase with FailOnShutdown {

  val testData = new TopologyStoreTestData(loggerFactory, executionContext)
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
  def topologyStore(mk: () => TopologyStore[TopologyStoreId]): Unit = {

    val bootstrapTransactions = StoredTopologyTransactions(
      Seq[
        (
            CantonTimestamp,
            (GenericSignedTopologyTransaction, Option[CantonTimestamp], Option[String]),
        )
      ](
        ts1 -> (tx1_NSD_Proposal, ts3.some, None),
        ts2 -> (tx2_OTK, ts3.some, None),
        ts3 -> (tx3_IDD_Removal, ts3.some, None),
        ts3 -> (tx3_NSD, None, None),
        ts3 -> (tx3_PTP_Proposal, ts5.some, None),
        ts4 -> (tx4_DND, None, None),
        ts4 -> (tx4_OTK_Proposal, None, None),
        ts5 -> (tx5_PTP, None, None),
        ts5 -> (tx5_DTC, ts6.some, None),
        ts6 -> (tx6_DTC_Update, None, None),
        ts6 -> (tx6_MDS, ts6.some, "invalid".some),
      ).map { case (from, (tx, until, rejection)) =>
        StoredTopologyTransaction(
          SequencedTime(from),
          EffectiveTime(from),
          until.map(EffectiveTime(_)),
          tx,
          rejection.map(String256M.tryCreate(_)),
        )
      }
    )

    "topology store" should {

      "deal with authorized transactions" when {

        "handle simple operations" in {
          val store = mk()

          for {
            _ <- update(store, ts1, add = Seq(tx1_NSD_Proposal))
            _ <- update(store, ts2, add = Seq(tx2_OTK))
            _ <- update(store, ts5, add = Seq(tx5_DTC))
            _ <- update(store, ts6, add = Seq(tx6_MDS))

            maxTs <- store.maxTimestamp(CantonTimestamp.MaxValue, includeRejected = true)
            retrievedTx <- store.findStored(CantonTimestamp.MaxValue, tx1_NSD_Proposal)
            txProtocolVersion <- store.findStoredForVersion(
              CantonTimestamp.MaxValue,
              tx1_NSD_Proposal.transaction,
              ProtocolVersion.v33,
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
              types =
                Seq(NamespaceDelegation.code, PartyToParticipant.code), // to test the types filter
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
              NonEmpty(Set, tx1_NSD_Proposal.hash),
            )
            txByMappingHash <- store.findTransactionsForMapping(
              EffectiveTime(ts2.immediateSuccessor), // increase since exclusive
              NonEmpty(Set, tx2_OTK.mapping.uniqueKey),
            )

            _ <- store.updateDispatchingWatermark(ts1)
            tsWatermark <- store.currentDispatchingWatermark

            _ <- update(
              store,
              ts4,
              removeMapping = Map(tx1_NSD_Proposal.mapping.uniqueKey -> tx1_NSD_Proposal.serial),
            )
            removedByMappingHash <- store.findStored(CantonTimestamp.MaxValue, tx1_NSD_Proposal)
            _ <- update(store, ts4, removeTxs = Set(tx2_OTK.hash))
            removedByTxHash <- store.findStored(CantonTimestamp.MaxValue, tx2_OTK)

            mdsTx <- store.findFirstMediatorStateForMediator(
              tx6_MDS.mapping.active.headOption.getOrElse(fail())
            )

            dtsTx <- store.findFirstTrustCertificateForParticipant(
              tx5_DTC.mapping.participantId
            )
          } yield {
            assert(maxTs.contains((SequencedTime(ts6), EffectiveTime(ts6))))
            retrievedTx.map(_.transaction) shouldBe Some(tx1_NSD_Proposal)
            txProtocolVersion.map(_.transaction) shouldBe Some(tx1_NSD_Proposal)

            expectTransactions(
              proposalTransactions,
              Seq(
                tx1_NSD_Proposal
              ), // only proposal transaction, TimeQuery.Range is inclusive on both sides
            )
            expectTransactions(
              proposalTransactionsFiltered,
              Seq(
                tx1_NSD_Proposal
              ),
            )
            expectTransactions(
              proposalTransactionsFiltered2,
              Nil, // no proposal transaction of type PartyToParticipant in the range
            )
            expectTransactions(positiveProposals, Seq(tx1_NSD_Proposal))

            txByTxHash shouldBe Seq(tx1_NSD_Proposal)
            txByMappingHash shouldBe Seq(tx2_OTK)

            tsWatermark shouldBe Some(ts1)

            removedByMappingHash.flatMap(_.validUntil) shouldBe Some(EffectiveTime(ts4))
            removedByTxHash.flatMap(_.validUntil) shouldBe Some(EffectiveTime(ts4))

            mdsTx.map(_.transaction) shouldBe Some(tx6_MDS)

            dtsTx.map(_.transaction) shouldBe Some(tx5_DTC)
          }
        }
        "able to filter with inspect" in {
          val store = mk()

          for {
            _ <- update(store, ts2, add = Seq(tx2_OTK))
            _ <- update(store, ts5, add = Seq(tx5_DTC))
            _ <- update(store, ts6, add = Seq(tx6_MDS))

            proposalTransactions <- inspect(
              store,
              TimeQuery.HeadState,
            )
            proposalTransactionsFiltered <- inspect(
              store,
              TimeQuery.HeadState,
              types = Seq(
                DomainTrustCertificate.code,
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
                tx2_OTK,
                tx5_DTC,
                tx6_MDS,
              ),
            )
            expectTransactions(
              proposalTransactionsFiltered,
              Seq(
                tx2_OTK,
                tx5_DTC,
              ),
            )
            expectTransactions(
              proposalTransactionsFiltered2,
              Nil, // no proposal transaction of type PartyToParticipant in the range
            )
          }
        }

        "able to inspect" in {
          val store = mk()

          for {
            _ <- store.bootstrap(bootstrapTransactions)
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
            idDaTransactions <- inspect(
              store,
              timeQuery = TimeQuery.Range(ts1.some, ts4.some),
              idFilter = Some("da"),
            )
            idNamespaceTransactions <- inspect(
              store,
              timeQuery = TimeQuery.Range(ts1.some, ts4.some),
              namespaceFilter = Some("decentralized-namespace"),
            )
            bothParties <- inspectKnownParties(store, ts6)
            onlyFred <- inspectKnownParties(store, ts6, filterParty = "fr::can")
            fredFullySpecified <- inspectKnownParties(
              store,
              ts6,
              filterParty = fredOfCanton.uid.toProtoPrimitive,
              filterParticipant = participantId1.uid.toProtoPrimitive,
            )
            onlyParticipant2 <- inspectKnownParties(store, ts6, filterParticipant = "participant2")
            neitherParty <- inspectKnownParties(store, ts6, "fred::canton", "participant2")
          } yield {
            expectTransactions(
              headStateTransactions,
              Seq(tx3_NSD, tx4_DND, tx5_PTP, tx6_DTC_Update),
            )
            expectTransactions(
              rangeBetweenTs2AndTs3Transactions,
              Seq(tx2_OTK, tx3_IDD_Removal, tx3_NSD),
            )
            expectTransactions(
              snapshotAtTs3Transactions,
              Seq(tx2_OTK), // tx2 include as until is inclusive, tx3 missing as from exclusive
            )
            expectTransactions(decentralizedNamespaceTransactions, Seq(tx4_DND))
            expectTransactions(removalTransactions, Seq(tx3_IDD_Removal))
            expectTransactions(idDaTransactions, Seq(tx3_IDD_Removal))
            expectTransactions(idNamespaceTransactions, Seq(tx4_DND))

            bothParties shouldBe Set(
              tx5_PTP.mapping.partyId,
              tx5_DTC.mapping.participantId.adminParty,
            )
            onlyFred shouldBe Set(tx5_PTP.mapping.partyId)
            fredFullySpecified shouldBe Set(tx5_PTP.mapping.partyId)
            onlyParticipant2 shouldBe Set(tx5_DTC.mapping.participantId.adminParty)
            neitherParty shouldBe Set.empty
          }
        }

        "handle rejected transactions" in {
          val store = mk()

          val bootstrapTransactions = StoredTopologyTransactions(
            Seq[
              (
                  CantonTimestamp,
                  (GenericSignedTopologyTransaction, Option[CantonTimestamp], Option[String256M]),
              )
            ](
              ts1 -> (tx2_OTK, None, None),
              ts2 -> (tx3_NSD, None, Some(String256M.tryCreate("rejection_reason"))),
              ts3 -> (tx6_MDS, None, None),
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
            _ <- store.bootstrap(bootstrapTransactions)
            headStateTransactions <- inspect(store, TimeQuery.HeadState)
          } yield {
            expectTransactions(
              headStateTransactions,
              Seq(tx2_OTK, tx6_MDS), // tx3_NSD was rejected
            )
          }
        }

        "able to findEssentialStateAtSequencedTime" in {
          val store = mk()
          for {
            _ <- update(store, ts2, add = Seq(tx2_OTK))
            _ <- update(store, ts5, add = Seq(tx5_DTC))
            _ <- update(store, ts6, add = Seq(tx6_MDS))

            transactionsAtTs6 <- store.findEssentialStateAtSequencedTime(
              asOfInclusive = SequencedTime(ts6),
              includeRejected = true,
            )
          } yield {
            expectTransactions(
              transactionsAtTs6,
              Seq(
                tx2_OTK,
                tx5_DTC,
                tx6_MDS,
              ),
            )
          }
        }

        "able to find positive transactions" in {
          val store = mk()

          for {
            _ <- store.bootstrap(bootstrapTransactions)
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
                  tx5_PTP.mapping.partyId.uid,
                  tx5_DTC.mapping.participantId.uid,
                )
              ),
            )
            namespaceFilterTransactions <- findPositiveTransactions(
              store,
              ts6,
              filterNamespace = Some(
                Seq(tx4_DND.mapping.namespace, tx5_DTC.mapping.namespace)
              ),
            )

            essentialStateTransactions <- store.findEssentialStateAtSequencedTime(
              SequencedTime(ts5),
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
                tx5_DTC.mapping.participantId,
                tx5_DTC.mapping.domainId,
              )
          } yield {
            expectTransactions(positiveTransactions, Seq(tx3_NSD, tx4_DND, tx5_PTP, tx5_DTC))
            expectTransactions(positiveTransactionsExclusive, Seq(tx3_NSD, tx4_DND))
            expectTransactions(
              positiveTransactionsInclusive,
              positiveTransactions.result.map(_.transaction),
            )
            expectTransactions(
              selectiveMappingTransactions,
              Seq( /* tx2_OKM only valid until ts3 */ tx4_DND, tx5_PTP),
            )
            expectTransactions(uidFilterTransactions, Seq(tx5_PTP, tx5_DTC))
            expectTransactions(namespaceFilterTransactions, Seq(tx4_DND, tx5_DTC))

            // Essential state currently encompasses all transactions at the specified time
            expectTransactions(
              essentialStateTransactions,
              Seq(
                tx1_NSD_Proposal,
                tx2_OTK,
                tx3_IDD_Removal,
                tx3_NSD,
                tx3_PTP_Proposal,
                tx4_DND,
                tx4_OTK_Proposal,
                tx5_PTP,
                tx5_DTC,
              ),
            )

            // Essential state with rejection currently encompasses all transactions at the specified time
            expectTransactions(
              essentialStateTransactionsWithRejections,
              Seq(
                tx1_NSD_Proposal,
                tx2_OTK,
                tx3_IDD_Removal,
                tx3_NSD,
                tx3_PTP_Proposal,
                tx4_DND,
                tx4_OTK_Proposal,
                tx5_PTP,
                tx5_DTC,
                tx6_DTC_Update,
                tx6_MDS,
              ),
            )

            upcomingTransactions shouldBe bootstrapTransactions.result.collect {
              case tx if tx.validFrom.value >= ts4 =>
                TopologyStore.Change.Other(tx.sequenced, tx.validFrom)
            }.distinct

            expectTransactions(
              dispatchingTransactionsAfter,
              Seq(
                tx2_OTK,
                tx3_IDD_Removal,
                tx3_NSD,
                tx4_DND,
                tx4_OTK_Proposal,
                tx5_PTP,
                tx5_DTC,
                tx6_DTC_Update,
              ),
            )

            onboardingTransactionUnlessShutdown shouldBe Seq(
              tx5_DTC,
              tx6_DTC_Update,
            )

          }
        }

        "correctly store rejected and accepted topology transactions with the same unique key within a batch" in {
          val store = mk()

          // * create two transactions with the same unique key but different content.
          // * use the signatures of the transaction to accept for the transaction to reject.
          // * put the rejected transaction before accepted one in the batch to be stored.
          // => if the DB unique key is not specific enough (ie doesn't cover the content), then
          //    the accepted transaction will not be stored correctly.

          val good_otk = makeSignedTx(
            OwnerToKeyMapping(participantId1, NonEmpty(Seq, factory.SigningKeys.key1))
          )

          val bad_otk = makeSignedTx(
            OwnerToKeyMapping(participantId1, NonEmpty(Seq, factory.EncryptionKeys.key2))
          ).copy(signatures = good_otk.signatures)

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
        import DefaultTestIdentities.*

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
          val store = mk()

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
                      participantId = participant1,
                      permission = ParticipantPermission.Submission,
                    )
                  ),
                )
                .value
            )
            proposedPartyToParticipant = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party3,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = participant1,
                      permission = ParticipantPermission.Submission,
                    )
                  ),
                )
                .value,
              isProposal = true,
            )
            rejectedPartyToParticipant = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party2,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = participant1,
                      permission = ParticipantPermission.Submission,
                    )
                  ),
                )
                .value
            )
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
                      participantId = participant1,
                      permission = ParticipantPermission.Confirmation,
                    ),
                    HostingParticipant(
                      participantId = participant2,
                      permission = ParticipantPermission.Submission,
                    ),
                  ),
                )
                .value,
              serial = PositiveInt.two,
            )
            partyToParticipant2transient2 = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party1,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = participant1,
                      permission = ParticipantPermission.Observation,
                    ),
                    HostingParticipant(
                      participantId = participant2,
                      permission = ParticipantPermission.Submission,
                    ),
                  ),
                )
                .value,
              serial = PositiveInt.three,
            )
            partyToParticipant2transient3 = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party1,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = participant1,
                      permission = ParticipantPermission.Observation,
                    ),
                    HostingParticipant(
                      participantId = participant2,
                      permission = ParticipantPermission.Submission,
                    ),
                  ),
                )
                .value,
              op = TopologyChangeOp.Remove,
              serial = PositiveInt.tryCreate(4),
            )
            partyToParticipant2 = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party1,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = participant1,
                      permission = ParticipantPermission.Submission,
                    ),
                    HostingParticipant(
                      participantId = participant2,
                      permission = ParticipantPermission.Submission,
                    ),
                  ),
                )
                .value,
              serial = PositiveInt.tryCreate(5),
            )
            party2ToParticipant = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party2,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = participant1,
                      permission = ParticipantPermission.Confirmation,
                    ),
                    HostingParticipant(
                      participantId = participant2,
                      permission = ParticipantPermission.Observation,
                    ),
                  ),
                )
                .value,
              serial = PositiveInt.one,
            )
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
                      participantId = participant1,
                      permission = ParticipantPermission.Submission,
                    ),
                    HostingParticipant(
                      participantId = participant2,
                      permission = ParticipantPermission.Submission,
                    ),
                  ),
                )
                .value,
              op = TopologyChangeOp.Remove,
              serial = PositiveInt.tryCreate(6),
            )
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
                      participantId = participant1,
                      permission = ParticipantPermission.Observation,
                    )
                  ),
                )
                .value,
              op = TopologyChangeOp.Replace,
              serial = PositiveInt.tryCreate(7),
            )
            partyToParticipant4transient2 = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party1,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = participant1,
                      permission = ParticipantPermission.Observation,
                    )
                  ),
                )
                .value,
              op = TopologyChangeOp.Remove,
              serial = PositiveInt.tryCreate(8),
            )
            partyToParticipant4transient3 = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party1,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = participant1,
                      permission = ParticipantPermission.Confirmation,
                    )
                  ),
                )
                .value,
              op = TopologyChangeOp.Replace,
              serial = PositiveInt.tryCreate(9),
            )
            partyToParticipant4 = makeSignedTx(
              mapping = PartyToParticipant
                .create(
                  partyId = party1,
                  threshold = PositiveInt.one,
                  participants = Seq(
                    HostingParticipant(
                      participantId = participant1,
                      permission = ParticipantPermission.Observation,
                    )
                  ),
                )
                .value,
              op = TopologyChangeOp.Remove,
              serial = PositiveInt.tryCreate(10),
            )
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
                      participantId = participant3,
                      permission = ParticipantPermission.Submission,
                    )
                  ),
                )
                .value,
              op = TopologyChangeOp.Replace,
              serial = PositiveInt.tryCreate(11),
            )
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
