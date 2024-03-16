// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AsyncWordSpec

trait TopologyStoreXTest extends AsyncWordSpec with TopologyStoreXTestBase {

  val testData = new TopologyStoreXTestData(loggerFactory, executionContext)
  import testData.*

  private lazy val submissionId = String255.tryCreate("submissionId")
  private lazy val submissionId2 = String255.tryCreate("submissionId2")

  protected def partyMetadataStore(mk: () => PartyMetadataStore): Unit = {
    import DefaultTestIdentities.*
    "inserting new succeeds" in {
      val store = mk()
      for {
        _ <- store.insertOrUpdatePartyMetadata(
          party1,
          Some(participant1),
          None,
          CantonTimestamp.Epoch,
          submissionId,
        )
        fetch <- store.metadataForParty(party1)
      } yield {
        fetch shouldBe Some(
          PartyMetadata(party1, None, Some(participant1))(CantonTimestamp.Epoch, submissionId)
        )
      }
    }

    "updating existing succeeds" in {
      val store = mk()
      for {
        _ <- store.insertOrUpdatePartyMetadata(
          party1,
          None,
          None,
          CantonTimestamp.Epoch,
          submissionId,
        )
        _ <- store.insertOrUpdatePartyMetadata(
          party2,
          None,
          None,
          CantonTimestamp.Epoch,
          submissionId,
        )
        _ <- store.insertOrUpdatePartyMetadata(
          party1,
          Some(participant1),
          Some(String255.tryCreate("MoreName")),
          CantonTimestamp.Epoch,
          submissionId,
        )
        _ <- store.insertOrUpdatePartyMetadata(
          party2,
          Some(participant3),
          Some(String255.tryCreate("Boooh")),
          CantonTimestamp.Epoch,
          submissionId,
        )
        meta1 <- store.metadataForParty(party1)
        meta2 <- store.metadataForParty(party2)
      } yield {
        meta1 shouldBe Some(
          PartyMetadata(party1, Some(String255.tryCreate("MoreName")), Some(participant1))(
            CantonTimestamp.Epoch,
            String255.empty,
          )
        )
        meta2 shouldBe Some(
          PartyMetadata(party2, Some(String255.tryCreate("Boooh")), Some(participant3))(
            CantonTimestamp.Epoch,
            String255.empty,
          )
        )
      }
    }

    "deal with delayed notifications" in {
      val store = mk()
      val rec1 =
        PartyMetadata(party1, None, Some(participant1))(CantonTimestamp.Epoch, submissionId)
      val rec2 =
        PartyMetadata(party2, Some(String255.tryCreate("Boooh")), Some(participant3))(
          CantonTimestamp.Epoch,
          submissionId2,
        )
      for {
        _ <- store.insertOrUpdatePartyMetadata(
          rec1.partyId,
          rec1.participantId,
          rec1.displayName,
          rec1.effectiveTimestamp,
          rec1.submissionId,
        )
        _ <- store.insertOrUpdatePartyMetadata(
          rec2.partyId,
          rec2.participantId,
          rec2.displayName,
          rec2.effectiveTimestamp,
          rec2.submissionId,
        )
        _ <- store.markNotified(rec2)
        notNotified <- store.fetchNotNotified()
      } yield {
        notNotified shouldBe Seq(rec1)
      }
    }

  }

  // TODO(#14066): Test coverage is rudimentary - enough to convince ourselves that queries basically seem to work.
  //  Increase coverage.
  def topologyStore(mk: () => TopologyStoreX[TopologyStoreId]): Unit = {

    val bootstrapTransactions = StoredTopologyTransactionsX(
      Seq[
        (CantonTimestamp, (GenericSignedTopologyTransactionX, Option[CantonTimestamp]))
      ](
        ts1 -> (tx1_NSD_Proposal, ts3.some),
        ts2 -> (tx2_OTK, ts3.some),
        ts3 -> (tx3_IDD_Removal, ts3.some),
        ts3 -> (tx3_NSD, None),
        ts3 -> (tx3_PTP_Proposal, ts5.some),
        ts4 -> (tx4_DND, None),
        ts4 -> (tx4_OTK_Proposal, None),
        ts5 -> (tx5_PTP, None),
        ts5 -> (tx5_DTC, ts6.some),
        ts6 -> (tx6_DTC_Update, None),
      ).map { case (from, (tx, until)) =>
        StoredTopologyTransactionX(
          SequencedTime(from),
          EffectiveTime(from),
          until.map(EffectiveTime(_)),
          tx,
        )
      }
    )

    "topology store x" should {

      "deal with authorized transactions" when {

        "handle simple operations" in {
          val store = mk()

          for {
            _ <- update(store, ts1, add = Seq(tx1_NSD_Proposal))
            _ <- update(store, ts2, add = Seq(tx2_OTK))
            _ <- update(store, ts5, add = Seq(tx5_DTC))
            _ <- update(store, ts6, add = Seq(tx6_MDS))

            maxTs <- store.maxTimestamp()
            retrievedTx <- store.findStored(CantonTimestamp.MaxValue, tx1_NSD_Proposal)
            txProtocolVersion <- store.findStoredForVersion(
              CantonTimestamp.MaxValue,
              tx1_NSD_Proposal.transaction,
              ProtocolVersion.v30,
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
                Seq(NamespaceDelegationX.code, PartyToParticipantX.code), // to test the types filter
            )
            proposalTransactionsFiltered2 <- inspect(
              store,
              TimeQuery.Range(ts1.some, ts4.some),
              proposals = true,
              types = Seq(PartyToParticipantX.code),
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
              ), // only proposal transaction, TimeQueryX.Range is inclusive on both sides
            )
            expectTransactions(
              proposalTransactionsFiltered,
              Seq(
                tx1_NSD_Proposal
              ),
            )
            expectTransactions(
              proposalTransactionsFiltered2,
              Nil, // no proposal transaction of type PartyToParticipantX in the range
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
                DomainTrustCertificateX.code,
                OwnerToKeyMappingX.code,
              ), // to test the types filter
            )
            proposalTransactionsFiltered2 <- inspect(
              store,
              TimeQuery.HeadState,
              types = Seq(PartyToParticipantX.code),
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
              Nil, // no proposal transaction of type PartyToParticipantX in the range
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
              types = Seq(DecentralizedNamespaceDefinitionX.code),
            )
            removalTransactions <- inspect(
              store,
              timeQuery = TimeQuery.Range(ts1.some, ts4.some),
              op = TopologyChangeOpX.Remove.some,
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
                DecentralizedNamespaceDefinitionX.code,
                OwnerToKeyMappingX.code,
                PartyToParticipantX.code,
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
              asOfInclusive = SequencedTime(ts5)
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
              .unwrap // FutureUnlessShutdown[_] -> Future[UnlessShutdown[_]]
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
                tx2_OTK,
                tx3_IDD_Removal,
                tx3_NSD,
                tx4_DND,
                tx4_OTK_Proposal,
                tx5_PTP,
                tx5_DTC,
              ),
            )

            upcomingTransactions shouldBe bootstrapTransactions.result.collect {
              case tx if tx.validFrom.value >= ts4 =>
                TopologyStoreX.Change.Other(tx.sequenced, tx.validFrom)
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

            onboardingTransactionUnlessShutdown.onShutdown(fail()) shouldBe Seq(
              tx4_DND,
              tx5_DTC,
              tx6_DTC_Update,
            )

          }
        }
      }
    }
  }
}
