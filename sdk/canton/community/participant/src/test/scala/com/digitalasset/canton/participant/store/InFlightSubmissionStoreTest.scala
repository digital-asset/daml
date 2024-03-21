// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.option.*
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.DefaultParticipantStateValues
import com.digitalasset.canton.participant.protocol.submission.*
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.{
  InFlightByMessageId,
  InFlightBySequencingInfo,
}
import com.digitalasset.canton.protocol.RootHash
import com.digitalasset.canton.sequencing.protocol.{MessageId, SequencerErrors}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{BaseTest, DefaultDamlValues, SequencerCounter}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
trait InFlightSubmissionStoreTest extends AsyncWordSpec with BaseTest {

  def mkChangeIdHash(index: Int) = ChangeIdHash(DefaultDamlValues.lfhash(index))

  lazy val changeId1 = mkChangeIdHash(1)
  lazy val changeId2 = mkChangeIdHash(2)
  lazy val changeId3 = mkChangeIdHash(3)
  lazy val changeId4 = mkChangeIdHash(4)
  lazy val submissionId1 = DefaultDamlValues.submissionId(1).some
  lazy val submissionId2 = DefaultDamlValues.submissionId(2).some
  lazy val domainId1 = DomainId.tryFromString("domain1::id")
  lazy val domainId2 = DomainId.tryFromString("domain2::id")
  lazy val messageId1 = new UUID(0, 1)
  lazy val messageId2 = new UUID(0, 2)
  lazy val messageId3 = new UUID(0, 3)
  lazy val messageId4 = new UUID(0, 4)
  lazy val traceContext1 = TraceContext.withNewTraceContext(Predef.identity)
  lazy val completionInfo = DefaultParticipantStateValues.completionInfo(List.empty)
  lazy val trackingData1 =
    TransactionSubmissionTrackingData(
      completionInfo,
      TransactionSubmissionTrackingData.TimeoutCause,
      None,
      testedProtocolVersion,
    )
  lazy val trackingData3 = TransactionSubmissionTrackingData(
    completionInfo,
    TransactionSubmissionTrackingData.CauseWithTemplate(
      SequencerErrors
        .SubmissionRequestMalformed("Some invalid batch")
        .rpcStatusWithoutLoggingContext()
    ),
    None,
    testedProtocolVersion,
  )
  lazy val submission1 = InFlightSubmission(
    changeId1,
    submissionId1,
    domainId1,
    messageId1,
    None,
    UnsequencedSubmission(CantonTimestamp.Epoch, trackingData1),
    traceContext1,
  )
  lazy val submission2 = InFlightSubmission(
    changeId2,
    submissionId2,
    domainId1,
    messageId2,
    None,
    UnsequencedSubmission(
      CantonTimestamp.Epoch.plusSeconds(20),
      TestSubmissionTrackingData.default,
    ),
    TraceContext.empty,
  )
  lazy val submission3 = InFlightSubmission(
    changeId3,
    submissionId1,
    domainId2,
    messageId3,
    None,
    UnsequencedSubmission(CantonTimestamp.Epoch.plusSeconds(30), trackingData3),
    TraceContext.empty,
  )
  lazy val sequencedSubmission1 =
    SequencedSubmission(SequencerCounter(10), CantonTimestamp.Epoch.plusSeconds(1))
  lazy val sequencedSubmission2 =
    SequencedSubmission(SequencerCounter(20), CantonTimestamp.Epoch.plusSeconds(11))

  def inFlightSubmissionStore(mk: () => InFlightSubmissionStore): Unit = {

    "register" should {
      "register an unknown submission" in {
        val store = mk()
        for {
          () <- store.register(submission1).valueOrFailShutdown("register submission")
          lookup <- valueOrFail(store.lookup(submission1.changeIdHash))("lookup submission")
          lookupUpto <- store.lookupUnsequencedUptoUnordered(
            submission1.submissionDomain,
            submission1.sequencingInfo.timeout,
          )
          lookupBefore <- store.lookupUnsequencedUptoUnordered(
            submission1.submissionDomain,
            submission1.sequencingInfo.timeout.minusMillis(1),
          )
          lookupOther <- store.lookupUnsequencedUptoUnordered(domainId2, CantonTimestamp.MaxValue)
          lookupMin <- store.lookupUnsequencedUptoUnordered(domainId1, CantonTimestamp.MinValue)
          lookupEarliest1 <- store.lookupEarliest(domainId1)
          lookupEarliest2 <- store.lookupEarliest(domainId2)
          lookupMsgId1 <- store.lookupSomeMessageId(
            submission1.submissionDomain,
            submission1.messageId,
          )
          lookupMsgIdWrongDomain <- store.lookupSomeMessageId(domainId2, submission1.messageId)
        } yield {
          lookup shouldBe submission1
          lookupUpto.toSet shouldBe Set(submission1)
          lookupBefore shouldBe Seq.empty
          lookupOther shouldBe Seq.empty
          lookupMin shouldBe Seq.empty
          lookupEarliest1 shouldBe Some(submission1.associatedTimestamp)
          lookupEarliest2 shouldBe None
          lookupMsgId1 shouldBe Some(submission1)
          lookupMsgIdWrongDomain shouldBe None
        }
      }

      "report an existing submission" in {
        val store = mk()
        for {
          () <- store.register(submission1).valueOrFailShutdown("register submission")
          existing1 <- store
            .register(submission1.copy(messageUuid = messageId2))
            .leftOrFailShutdown("re-register submission with different message ID")
          existing2 <- store
            .register(submission1.copy(submissionDomain = domainId2))
            .leftOrFailShutdown("re-register submission with different domain ID")
          lookup <- valueOrFail(store.lookup(submission1.changeIdHash))("lookup submission")
        } yield {
          existing1 shouldBe submission1
          existing2 shouldBe submission1
          lookup shouldBe submission1
        }
      }

      "be idempotent" in {
        val store = mk()
        for {
          () <- store.register(submission1).valueOrFailShutdown("register submission")
          () <- store.register(submission1).valueOrFailShutdown("register submission again")
          lookup <- valueOrFail(store.lookup(submission1.changeIdHash))("lookup submission")
        } yield {
          lookup shouldBe submission1
        }
      }
    }

    "updateRegistration" should {
      "update the given root hash only for the provided submission" in {
        val store = mk()
        val rootHash = RootHash(TestHash.digest(1))

        for {
          () <- store.register(submission1).valueOrFailShutdown("register submission1")
          () <- store.register(submission2).valueOrFailShutdown("register submission2")

          lookupBefore <- store.lookupUnsequencedUptoUnordered(
            submission1.submissionDomain,
            CantonTimestamp.MaxValue,
          )

          () <- store.updateRegistration(
            submission1,
            rootHash,
          )

          lookupAfter <- store.lookupUnsequencedUptoUnordered(
            submission1.submissionDomain,
            CantonTimestamp.MaxValue,
          )
        } yield {
          lookupBefore.toSet shouldBe Set(submission1, submission2)
          lookupAfter.toSet shouldBe Set(submission1.copy(rootHashO = Some(rootHash)), submission2)
        }
      }

      "update the given root hash only for unsequenced submissions" in {
        val store = mk()
        val rootHash = RootHash(TestHash.digest(1))

        for {
          () <- store.register(submission1).valueOrFailShutdown("register submission1")

          lookupUnseqBefore <- store.lookupUnsequencedUptoUnordered(
            submission1.submissionDomain,
            CantonTimestamp.MaxValue,
          )
          lookupSeqBefore <- store.lookupSequencedUptoUnordered(
            submission1.submissionDomain,
            CantonTimestamp.MaxValue,
          )

          () <- store.observeSequencing(
            domainId1,
            Map(submission1.messageId -> sequencedSubmission1),
          )

          () <- store.updateRegistration(
            submission1,
            rootHash,
          )

          lookupUnseqAfter <- store.lookupUnsequencedUptoUnordered(
            submission1.submissionDomain,
            CantonTimestamp.MaxValue,
          )
          lookupSeqAfter <- store.lookupSequencedUptoUnordered(
            submission1.submissionDomain,
            CantonTimestamp.MaxValue,
          )
        } yield {
          lookupUnseqBefore.loneElement shouldBe submission1
          lookupSeqBefore shouldBe empty
          lookupUnseqAfter shouldBe empty
          lookupSeqAfter.loneElement shouldBe submission1.copy(sequencingInfo =
            sequencedSubmission1
          )
        }
      }

      "update the given root hash only if it is not yet set" in {
        val store = mk()
        val rootHash1 = RootHash(TestHash.digest(1))
        val rootHash2 = RootHash(TestHash.digest(2))

        for {
          () <- store.register(submission1).valueOrFailShutdown("register submission1")

          lookupBefore <- store.lookupUnsequencedUptoUnordered(
            submission1.submissionDomain,
            CantonTimestamp.MaxValue,
          )

          () <- store.updateRegistration(
            submission1,
            rootHash1,
          )
          () <- store.updateRegistration(
            submission1,
            rootHash2,
          )

          lookupAfter <- store.lookupUnsequencedUptoUnordered(
            submission1.submissionDomain,
            CantonTimestamp.MaxValue,
          )
        } yield {
          lookupBefore.loneElement shouldBe submission1
          lookupAfter.loneElement shouldBe submission1.copy(rootHashO = Some(rootHash1))
        }
      }
    }

    "observeSequencing" should {
      "update the given message IDs" in {
        val store = mk()
        for {
          () <- store.register(submission1).valueOrFailShutdown("register submission1")
          () <- store.register(submission2).valueOrFailShutdown("register submission2")
          () <- store.register(submission3).valueOrFailShutdown("register submission3")
          lookupUpto1 <- store.lookupUnsequencedUptoUnordered(
            submission1.submissionDomain,
            CantonTimestamp.MaxValue,
          )
          () <- store.observeSequencing(
            domainId1,
            Map(
              submission1.messageId -> sequencedSubmission1,
              submission2.messageId -> sequencedSubmission2,
              submission3.messageId -> sequencedSubmission2,
              MessageId.fromUuid(messageId4) -> sequencedSubmission1,
            ),
          )
          lookupUpto2 <- store.lookupUnsequencedUptoUnordered(
            submission1.submissionDomain,
            CantonTimestamp.MaxValue,
          )
          sequenced1 <- valueOrFail(store.lookup(submission1.changeIdHash))("lookup submission1")
          sequenced2 <- valueOrFail(store.lookup(submission2.changeIdHash))("lookup submission2")
          unsequenced3 <- valueOrFail(store.lookup(submission3.changeIdHash))("lookup submission3")
          earliest1 <- store.lookupEarliest(domainId1)
          earliest2 <- store.lookupEarliest(domainId2)
          lookupSequenced <- store.lookupSequencedUptoUnordered(
            submission1.submissionDomain,
            CantonTimestamp.MaxValue,
          )
          lookupSequenced1 <- store.lookupSequencedUptoUnordered(
            submission1.submissionDomain,
            sequencedSubmission1.sequencingTime,
          )
          none <- store.lookupSequencedUptoUnordered(
            submission3.submissionDomain,
            CantonTimestamp.MaxValue,
          )
          lookupMsgId1 <- store.lookupSomeMessageId(
            submission1.submissionDomain,
            submission1.messageId,
          )
          lookupMsgId3 <- store.lookupSomeMessageId(
            submission3.submissionDomain,
            submission3.messageId,
          )
        } yield {
          lookupUpto1.toSet shouldBe Set(submission1, submission2)
          lookupUpto2 shouldBe Seq.empty
          sequenced1 shouldBe submission1.copy(sequencingInfo = sequencedSubmission1)
          sequenced2 shouldBe submission2.copy(sequencingInfo = sequencedSubmission2)
          unsequenced3 shouldBe submission3
          earliest1 shouldBe Some(sequencedSubmission1.sequencingTime)
          earliest2 shouldBe Some(submission3.associatedTimestamp)
          lookupSequenced.toSet shouldBe Set(sequenced1, sequenced2)
          lookupSequenced1.toSet shouldBe Set(sequenced1)
          none shouldBe Seq.empty
          lookupMsgId1 shouldBe Some(sequenced1)
          lookupMsgId3 shouldBe Some(submission3)
        }
      }

      "not update sequenced submissions" in {
        val store = mk()
        for {
          () <- store.register(submission1).valueOrFailShutdown("register submission1")
          () <- store.observeSequencing(
            domainId1,
            Map(submission1.messageId -> sequencedSubmission1),
          )
          () <- store.observeSequencing(
            domainId1,
            Map(submission1.messageId -> sequencedSubmission2),
          )
          sequenced1 <- valueOrFail(store.lookup(submission1.changeIdHash))("lookup submission1")
          earliest <- store.lookupEarliest(domainId1)
        } yield {
          sequenced1 shouldBe submission1.copy(sequencingInfo = sequencedSubmission1)
          earliest shouldBe Some(sequencedSubmission1.sequencingTime)
        }
      }

      "update all unsequenced submissions if there are several for the same message id" in {
        // This shouldn't happen in practice,
        // but we nevertheless include the test to ensure that all store implementations behave the same
        val store = mk()
        val submission2a = submission2.copy(messageUuid = submission1.messageUuid)
        val submission3a =
          submission3.copy(messageUuid = submission1.messageUuid, submissionDomain = domainId1)
        for {
          () <- store.register(submission1).valueOrFailShutdown("register submission1")
          () <- store.observeSequencing(
            domainId1,
            Map(submission1.messageId -> sequencedSubmission1),
          )
          () <- store.register(submission2a).valueOrFailShutdown("register submission2a")
          () <- store.register(submission3a).valueOrFailShutdown("register submission3a")
          () <- store.observeSequencing(
            domainId1,
            Map(submission1.messageId -> sequencedSubmission2),
          )
          sequenced1 <- valueOrFail(store.lookup(submission1.changeIdHash))("lookup submission1")
          sequenced2a <- valueOrFail(store.lookup(submission2a.changeIdHash))("lookup submission2a")
          sequenced3a <- valueOrFail(store.lookup(submission3a.changeIdHash))(
            "lookup submiission3a"
          )
          lookupMsgId1 <- store.lookupSomeMessageId(
            submission1.submissionDomain,
            submission1.messageId,
          )
          lookupMsgId2 <- store.lookupSomeMessageId(
            submission3.submissionDomain,
            submission3a.messageId,
          )
        } yield {
          sequenced1 shouldBe submission1.copy(sequencingInfo = sequencedSubmission1)
          sequenced2a shouldBe submission2a.copy(sequencingInfo = sequencedSubmission2)
          sequenced3a shouldBe submission3a.copy(sequencingInfo = sequencedSubmission2)

          lookupMsgId1 should (be(Some(sequenced1)) or be(Some(sequenced2a)) or be(
            Some(sequenced3a)
          ))
          lookupMsgId2 shouldBe None
        }
      }
    }

    "observeSequencedRootHash" should {
      "update the submission with the given root hash" in {
        val store = mk()
        val rootHash1 = RootHash(TestHash.digest(1))
        val rootHash2 = RootHash(TestHash.digest(2))

        def lookups = for {
          unsequenced <- store.lookupUnsequencedUptoUnordered(
            submission1.submissionDomain,
            CantonTimestamp.MaxValue,
          )
          sequenced <- store.lookupSequencedUptoUnordered(
            submission1.submissionDomain,
            CantonTimestamp.MaxValue,
          )
        } yield (unsequenced, sequenced)

        for {
          () <- store.register(submission1).valueOrFailShutdown("register submission1")
          () <- store.updateRegistration(
            submission1,
            rootHash1,
          )
          // Also test register() with an already provided root hash.
          // Currently this is not used, but the functionality is implemented.
          () <- store
            .register(submission2.copy(rootHashO = Some(rootHash2)))
            .valueOrFailShutdown("register submission2")

          lookups1 <- lookups
          (lookupUnsequenced1, lookupSequenced1) = lookups1

          () <- store.observeSequencedRootHash(
            rootHash1,
            sequencedSubmission1,
          )

          lookups2 <- lookups
          (lookupUnsequenced2, lookupSequenced2) = lookups2

          () <- store.observeSequencedRootHash(
            rootHash2,
            sequencedSubmission2,
          )

          lookups3 <- lookups
          (lookupUnsequenced3, lookupSequenced3) = lookups3
        } yield {
          val unsequenced1 = submission1.copy(rootHashO = Some(rootHash1))
          val sequenced1 = unsequenced1.copy(sequencingInfo = sequencedSubmission1)
          val unsequenced2 = submission2.copy(rootHashO = Some(rootHash2))
          val sequenced2 = unsequenced2.copy(sequencingInfo = sequencedSubmission2)

          lookupSequenced1 shouldBe empty
          lookupUnsequenced1.toSet shouldBe Set(unsequenced1, unsequenced2)

          lookupSequenced2.loneElement shouldBe sequenced1
          lookupUnsequenced2.loneElement shouldBe unsequenced2

          lookupSequenced3.toSet shouldBe Set(sequenced1, sequenced2)
          lookupUnsequenced3 shouldBe empty
        }
      }

      "not update a sequenced submission with a later one" in {
        val store = mk()
        val rootHash = RootHash(TestHash.digest(1))

        for {
          () <- store.register(submission1).valueOrFailShutdown("register submission1")
          () <- store.updateRegistration(
            submission1,
            rootHash,
          )

          () <- store.observeSequencedRootHash(
            rootHash,
            sequencedSubmission1,
          )
          () <- store.observeSequencedRootHash(
            rootHash,
            sequencedSubmission2,
          )

          sequenced1 <- valueOrFail(store.lookup(submission1.changeIdHash))("lookup submission1")
          earliest <- store.lookupEarliest(domainId1)
        } yield {
          sequenced1 shouldBe submission1.copy(
            sequencingInfo = sequencedSubmission1,
            rootHashO = Some(rootHash),
          )
          earliest shouldBe Some(sequencedSubmission1.sequencingTime)
        }
      }

      "update a sequenced submission with an earlier one" in {
        val store = mk()
        val rootHash = RootHash(TestHash.digest(1))

        for {
          () <- store.register(submission1).valueOrFailShutdown("register submission1")
          () <- store.updateRegistration(
            submission1,
            rootHash,
          )

          () <- store.observeSequencedRootHash(
            rootHash,
            sequencedSubmission2,
          )
          () <- store.observeSequencedRootHash(
            rootHash,
            sequencedSubmission1,
          )

          sequenced1 <- valueOrFail(store.lookup(submission1.changeIdHash))("lookup submission1")
          earliest <- store.lookupEarliest(domainId1)
        } yield {
          sequenced1 shouldBe submission1.copy(
            sequencingInfo = sequencedSubmission1,
            rootHashO = Some(rootHash),
          )
          earliest shouldBe Some(sequencedSubmission1.sequencingTime)
        }
      }

      "update all unsequenced submissions if there are several for the same root hash" in {
        // This shouldn't happen in practice,
        // but we nevertheless include the test to ensure that all store implementations behave the same
        val store = mk()
        val rootHash = RootHash(TestHash.digest(1))

        for {
          () <- store.register(submission1).valueOrFailShutdown("register submission1")
          () <- store.updateRegistration(
            submission1,
            rootHash,
          )
          () <- store.register(submission2).valueOrFailShutdown("register submission2")
          () <- store.updateRegistration(
            submission2,
            rootHash,
          )

          () <- store.observeSequencedRootHash(
            rootHash,
            sequencedSubmission1,
          )

          sequenced1 <- valueOrFail(store.lookup(submission1.changeIdHash))("lookup submission1")
          sequenced2 <- valueOrFail(store.lookup(submission2.changeIdHash))("lookup submission2")
        } yield {
          sequenced1 shouldBe submission1.copy(
            sequencingInfo = sequencedSubmission1,
            rootHashO = Some(rootHash),
          )
          sequenced2 shouldBe submission2.copy(
            sequencingInfo = sequencedSubmission1,
            rootHashO = Some(rootHash),
          )
        }
      }
    }

    "delete" should {
      "remove in-flight submissions" in {
        val store = mk()
        for {
          () <- store.register(submission1).valueOrFailShutdown("register submission1")
          () <- store.register(submission2).valueOrFailShutdown("register submission2")
          () <- store.register(submission3).valueOrFailShutdown("register submission3")
          () <- store.observeSequencing(
            domainId2,
            Map(submission3.messageId -> sequencedSubmission1),
          )
          () <- store.delete(
            InFlightByMessageId(domainId2, MessageId.fromUuid(messageId1)) +:
              Seq(submission1, submission3).map(_.referenceByMessageId)
          )
          lookup1 <- store.lookup(submission1.changeIdHash).value
          lookup2 <- valueOrFail(store.lookup(submission2.changeIdHash))("lookup submission2")
          lookup3 <- store.lookup(submission3.changeIdHash).value
          lookupUpto <- store.lookupUnsequencedUptoUnordered(
            submission1.submissionDomain,
            submission1.sequencingInfo.timeout,
          )
          earliest <- store.lookupEarliest(submission1.submissionDomain)
        } yield {
          lookup1 shouldBe None
          lookup2 shouldBe submission2
          lookup3 shouldBe None
          lookupUpto shouldBe Seq.empty
          earliest shouldBe Some(submission2.associatedTimestamp)
        }
      }

      "enable re-registration" in {
        val store = mk()
        val submission1a = submission1.copy(messageUuid = messageId4)
        for {
          () <- store.register(submission1).valueOrFailShutdown("register")
          () <- store.delete(Seq(submission1.referenceByMessageId))
          () <- store.register(submission1a).valueOrFailShutdown("reregister")
          lookup <- valueOrFail(store.lookup(submission1.changeIdHash))("lookup")
        } yield {
          lookup shouldBe submission1a
        }
      }

      "check the message ID" in {
        val store = mk()
        for {
          () <- store.register(submission1).valueOrFailShutdown("register")
          () <- store.delete(
            Seq(InFlightByMessageId(submission1.submissionDomain, MessageId.fromUuid(messageId4)))
          )
          lookup <- valueOrFail(store.lookup(submission1.changeIdHash))("lookup")
        } yield {
          lookup shouldBe submission1
        }
      }

      "remove sequenced in-flight submissions" in {
        val store = mk()
        for {
          () <- store.register(submission1).valueOrFailShutdown("register submission1")
          () <- store.register(submission2).valueOrFailShutdown("register submission2")
          () <- store.register(submission3).valueOrFailShutdown("register submission3")
          () <- store.observeSequencing(
            domainId2,
            Map(submission3.messageId -> sequencedSubmission1),
          )
          () <- store.observeSequencing(
            domainId1,
            Map(
              submission1.messageId -> sequencedSubmission1,
              submission2.messageId -> sequencedSubmission2,
            ),
          )
          () <- store.delete(Seq(InFlightBySequencingInfo(domainId1, sequencedSubmission1)))
          lookup1 <- store.lookup(submission1.changeIdHash).value
          lookup2 <- valueOrFail(store.lookup(submission2.changeIdHash))("lookup submission2")
          lookup3 <- valueOrFail(store.lookup(submission3.changeIdHash))(
            "lookup submission3"
          ) // Not removed because its on another domain
          // Reinsert submission 1
          () <- store.register(submission1).valueOrFailShutdown("reregister submission1")
          () <- store.observeSequencing(
            domainId1,
            Map(submission1.messageId -> sequencedSubmission1),
          )
          lookup1a <- valueOrFail(store.lookup(submission1.changeIdHash))("lookup submission1")
          () <- store.delete(
            Seq(
              InFlightBySequencingInfo(domainId1, sequencedSubmission1),
              InFlightBySequencingInfo(domainId1, sequencedSubmission2),
            )
          )
          lookup1b <- store.lookup(submission1.changeIdHash).value
          lookup2b <- store.lookup(submission1.changeIdHash).value
          () <- store.delete(Seq(InFlightBySequencingInfo(domainId2, sequencedSubmission1)))
          lookup3b <- store.lookup(submission3.changeIdHash).value
        } yield {
          lookup1 shouldBe None
          lookup2 shouldBe submission2.copy(sequencingInfo = sequencedSubmission2)
          lookup3 shouldBe submission3.copy(sequencingInfo = sequencedSubmission1)
          lookup1a shouldBe submission1.copy(sequencingInfo = sequencedSubmission1)
          lookup1b shouldBe None
          lookup2b shouldBe None
          lookup3b shouldBe None
        }
      }

      "mix by message id and by sequencing info" in {
        val store = mk()
        for {
          () <- store.register(submission1).valueOrFailShutdown("register submission1")
          () <- store.register(submission2).valueOrFailShutdown("register submission2")
          () <- store.register(submission3).valueOrFailShutdown("register submission3")
          () <- store.observeSequencing(
            domainId2,
            Map(submission3.messageId -> sequencedSubmission1),
          )
          () <- store.observeSequencing(
            domainId1,
            Map(submission1.messageId -> sequencedSubmission1),
          )
          () <- store.delete(
            Seq(
              submission2.referenceByMessageId,
              InFlightBySequencingInfo(domainId1, sequencedSubmission1),
              submission3.referenceByMessageId,
            )
          )
          lookup1 <- store.lookup(submission1.changeIdHash).value
          lookup2 <- store.lookup(submission2.changeIdHash).value
          lookup3 <- store.lookup(submission3.changeIdHash).value
        } yield {
          lookup1 shouldBe None
          lookup2 shouldBe None
          lookup3 shouldBe None
        }
      }
    }

    "updateUnsequenced" should {
      "ignore nonexistent submissions" in {
        val store = mk()
        for {
          () <- store.updateUnsequenced(
            submission1.changeIdHash,
            submission1.submissionDomain,
            submission1.messageId,
            UnsequencedSubmission(CantonTimestamp.MaxValue, TestSubmissionTrackingData.default),
          )
          lookup <- store.lookup(submission1.changeIdHash).value
        } yield {
          lookup shouldBe None
        }
      }

      "update the unsequenced submission" in {
        val store = mk()
        val newSequencingInfo1 =
          UnsequencedSubmission(submission1.associatedTimestamp, TestSubmissionTrackingData.default)
        val newSequencingInfo2 =
          UnsequencedSubmission(CantonTimestamp.MinValue, TestSubmissionTrackingData.default)
        for {
          () <- store.register(submission1).valueOrFailShutdown("register")
          () <- store.updateUnsequenced(
            submission1.changeIdHash,
            submission1.submissionDomain,
            submission1.messageId,
            newSequencingInfo1,
          )
          lookup1 <- valueOrFail(store.lookup(submission1.changeIdHash))("lookup 1")
          () <- store.updateUnsequenced(
            submission1.changeIdHash,
            submission1.submissionDomain,
            submission1.messageId,
            newSequencingInfo2,
          )
          lookup2 <- valueOrFail(store.lookup(submission1.changeIdHash))("lookup 2")
          earliest <- store.lookupEarliest(submission1.submissionDomain)
        } yield {
          lookup1 shouldBe submission1.copy(sequencingInfo = newSequencingInfo1)
          lookup2 shouldBe submission1.copy(sequencingInfo = newSequencingInfo2)
          earliest shouldBe Some(newSequencingInfo2.timeout)
        }
      }

      "not push the timeout out" in {
        val store = mk()
        val newSequencingInfo =
          UnsequencedSubmission(CantonTimestamp.MaxValue, TestSubmissionTrackingData.default)
        for {
          () <- store.register(submission1).valueOrFailShutdown("register")
          () <- loggerFactory.assertLogs(
            store.updateUnsequenced(
              submission1.changeIdHash,
              submission1.submissionDomain,
              submission1.messageId,
              newSequencingInfo,
            ),
            _.warningMessage should include regex
              s"Sequencing timeout for submission \\(change ID hash .*, message Id .* on .*\\) is at ${submission1.associatedTimestamp} before ${newSequencingInfo.timeout}",
          )
          lookup <- valueOrFail(store.lookup(submission1.changeIdHash))("lookup")
          earliest <- store.lookupEarliest(submission1.submissionDomain)
        } yield {
          lookup shouldBe submission1
          earliest shouldBe Some(submission1.associatedTimestamp)
        }
      }

      "not update a sequenced submission" in {
        val store = mk()
        val newSequencingInfo =
          UnsequencedSubmission(CantonTimestamp.MaxValue, TestSubmissionTrackingData.default)
        for {
          () <- store.register(submission1).valueOrFailShutdown("register")
          () <- store.observeSequencing(
            submission1.submissionDomain,
            Map(submission1.messageId -> sequencedSubmission1),
          )
          () <- loggerFactory.assertLogs(
            store.updateUnsequenced(
              submission1.changeIdHash,
              submission1.submissionDomain,
              submission1.messageId,
              newSequencingInfo,
            ),
            _.warningMessage should include(
              show"Submission (change ID hash ${submission1.changeIdHash}, message Id ${submission1.messageId}) on ${submission1.submissionDomain} has already been sequenced. $sequencedSubmission1"
            ),
          )
          lookup <- valueOrFail(store.lookup(submission1.changeIdHash))("lookup")
        } yield {
          lookup shouldBe submission1.copy(sequencingInfo = sequencedSubmission1)
        }
      }
    }
  }
}
