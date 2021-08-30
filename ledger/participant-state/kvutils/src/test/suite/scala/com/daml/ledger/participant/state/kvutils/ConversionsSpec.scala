// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.{Duration, Instant}

import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.configuration.LedgerTimeModel
import com.daml.ledger.participant.state.kvutils.Conversions.{
  buildTimestamp,
  commandDedupKey,
  decodeBlindingInfo,
  encodeBlindingInfo,
  extractDivulgedContracts,
  parseCompletionInfo,
}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlTransactionBlindingInfo.{
  DisclosureEntry,
  DivulgenceEntry,
}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntryId,
  DamlStateKey,
  DamlSubmitterInfo,
  DamlTransactionBlindingInfo,
  DamlTransactionRejectionEntry,
  Disputed,
  Duplicate,
  Inconsistent,
  PartyNotKnownOnLedger,
  ResourcesExhausted,
}
import com.daml.ledger.participant.state.kvutils.committer.transaction.Rejection
import com.daml.lf.crypto
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Relation.Relation
import com.daml.lf.engine.Error
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.{BlindingInfo, NodeId, TransactionOuterClass, TransactionVersion}
import com.daml.lf.value.Value.{ContractId, ContractInst, ValueText}
import com.daml.lf.value.ValueOuterClass
import com.google.protobuf.ByteString
import io.grpc.Status.Code
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.{ListMap, ListSet}
import scala.jdk.CollectionConverters._

class ConversionsSpec extends AnyWordSpec with Matchers with OptionValues {
  "Conversions" should {
    "correctly and deterministically encode Blindinginfo" in {
      encodeBlindingInfo(
        wronglySortedBlindingInfo,
        Map(
          contractId0 -> apiContractInstance0,
          contractId1 -> apiContractInstance1,
        ),
      ) shouldBe correctlySortedEncodedBlindingInfo
    }

    "correctly decode BlindingInfo" in {
      val decodedBlindingInfo =
        decodeBlindingInfo(correctlySortedEncodedBlindingInfo)
      decodedBlindingInfo.disclosure.toSet should contain theSameElementsAs wronglySortedBlindingInfo.disclosure.toSet
      decodedBlindingInfo.divulgence.toSet should contain theSameElementsAs wronglySortedBlindingInfo.divulgence.toSet
    }

    "correctly extract divulged contracts" in {
      val maybeDivulgedContracts = extractDivulgedContracts(correctlySortedEncodedBlindingInfo)

      maybeDivulgedContracts shouldBe Right(
        Map(
          contractId0 -> lfContractInstance0,
          contractId1 -> lfContractInstance1,
        )
      )
    }

    "return Left with missing contract ids when extracting divulged contracts if a contract instance is missing" in {
      val encodedBlindingInfoWithMissingContractInstance =
        correctlySortedEncodedBlindingInfo.toBuilder
          .addDivulgences(
            DivulgenceEntry.newBuilder().setContractId("some cid")
          )
          .build()

      val maybeDivulgedContracts =
        extractDivulgedContracts(encodedBlindingInfoWithMissingContractInstance)

      maybeDivulgedContracts shouldBe Left(Vector("some cid"))
    }

    "deterministically encode deduplication keys with multiple submitters (order independence)" in {
      val key1 = deduplicationKeyBytesFor(List("alice", "bob"))
      val key2 = deduplicationKeyBytesFor(List("bob", "alice"))
      key1 shouldBe key2
    }

    "deterministically encode deduplication keys with multiple submitters (duplicate submitters)" in {
      val key1 = deduplicationKeyBytesFor(List("alice", "bob"))
      val key2 = deduplicationKeyBytesFor(List("alice", "bob", "alice"))
      key1 shouldBe key2
    }

    "correctly encode deduplication keys with multiple submitters" in {
      val key1 = deduplicationKeyBytesFor(List("alice"))
      val key2 = deduplicationKeyBytesFor(List("alice", "bob"))
      key1 should not be key2
    }

    "encode/decode rejections" should {
      val submitterInfo = DamlSubmitterInfo.newBuilder().build()

      def rejectionEntryIsConverted(rejection: Rejection, expectedCode: Code) = {
        val encodedEntry = Conversions
          .encodeTransactionRejectionEntry(
            submitterInfo,
            rejection,
          )
          .build()
        Conversions
          .decodeTransactionRejectionEntry(encodedEntry)
          .value
          .code shouldBe expectedCode.value()
      }

      "convert validation failure" in {
        rejectionEntryIsConverted(
          Rejection.ValidationFailure(Error.Package(Error.Package.Internal("ERROR", "ERROR"))),
          Code.INVALID_ARGUMENT,
        )
      }

      "convert internal duplicate keys" in {
        val encodedEntry = Conversions
          .encodeTransactionRejectionEntry(
            submitterInfo,
            Rejection.InternallyInconsistentTransaction.DuplicateKeys,
          )
          .build()
        Conversions
          .decodeTransactionRejectionEntry(encodedEntry) shouldBe None
      }

      "convert internal inconsistent keys" in {
        rejectionEntryIsConverted(
          Rejection.InternallyInconsistentTransaction.InconsistentKeys,
          Code.ABORTED,
        )
      }

      "convert external inconsistent contracts" in {
        rejectionEntryIsConverted(
          Rejection.ExternallyInconsistentTransaction.InconsistentContracts,
          Code.ABORTED,
        )
      }

      "convert external external duplicate keys" in {
        val encodedEntry = Conversions
          .encodeTransactionRejectionEntry(
            submitterInfo,
            Rejection.ExternallyInconsistentTransaction.DuplicateKeys,
          )
          .build()
        Conversions
          .decodeTransactionRejectionEntry(encodedEntry) shouldBe None
      }

      "convert external inconsistent keys" in {
        rejectionEntryIsConverted(
          Rejection.ExternallyInconsistentTransaction.InconsistentKeys,
          Code.ABORTED,
        )
      }

      "convert missing input state" in {
        rejectionEntryIsConverted(
          Rejection.MissingInputState(DamlStateKey.getDefaultInstance),
          Code.INVALID_ARGUMENT,
        )
      }

      "convert invalid participant state" in {
        rejectionEntryIsConverted(
          Rejection.InvalidParticipantState(Err.InternalError("error")),
          Code.ABORTED,
        )
      }

      "convert ledger time out of range" in {
        val now = Instant.now
        rejectionEntryIsConverted(
          Rejection.LedgerTimeOutOfRange(LedgerTimeModel.OutOfRange(now, now, now)),
          Code.ABORTED,
        )
      }

      "convert record time out of range" in {
        val now = Instant.now()
        rejectionEntryIsConverted(
          Rejection.RecordTimeOutOfRange(now, now),
          Code.INVALID_ARGUMENT,
        )
      }

      "convert causal monotonicity violated" in {
        rejectionEntryIsConverted(
          Rejection.CausalMonotonicityViolated,
          Code.INVALID_ARGUMENT,
        )
      }

      "convert submitting party not know on ledger" in {
        rejectionEntryIsConverted(
          Rejection.SubmittingPartyNotKnownOnLedger(Ref.Party.assertFromString("party")),
          Code.INVALID_ARGUMENT,
        )
      }

      "convert parties not known on ledger" in {
        rejectionEntryIsConverted(
          Rejection.PartiesNotKnownOnLedger(Seq.empty),
          Code.INVALID_ARGUMENT,
        )
      }

      "convert submitter cannot act via participant" in {
        rejectionEntryIsConverted(
          Rejection.SubmitterCannotActViaParticipant(
            Ref.Party.assertFromString("party"),
            Ref.ParticipantId.assertFromString("id"),
          ),
          Code.PERMISSION_DENIED,
        )
      }

      "convert v1 rejections" should {

        "handle inconsistent" in {
          Conversions
            .decodeTransactionRejectionEntry(
              DamlTransactionRejectionEntry
                .newBuilder()
                .setInconsistent(Inconsistent.newBuilder())
                .build()
            )
            .value
            .code shouldBe Code.ABORTED.value()
        }

        "handle disputed" in {
          Conversions
            .decodeTransactionRejectionEntry(
              DamlTransactionRejectionEntry
                .newBuilder()
                .setDisputed(Disputed.newBuilder())
                .build()
            )
            .value
            .code shouldBe Code.INVALID_ARGUMENT.value()
        }

        "handle resources exhausted" in {
          Conversions
            .decodeTransactionRejectionEntry(
              DamlTransactionRejectionEntry
                .newBuilder()
                .setResourcesExhausted(ResourcesExhausted.newBuilder())
                .build()
            )
            .value
            .code shouldBe Code.ABORTED.value()
        }

        "handle duplicate" in {
          Conversions
            .decodeTransactionRejectionEntry(
              DamlTransactionRejectionEntry
                .newBuilder()
                .setDuplicateCommand(Duplicate.newBuilder())
                .build()
            ) shouldBe None
        }

        "handle party not known on ledger" in {
          Conversions
            .decodeTransactionRejectionEntry(
              DamlTransactionRejectionEntry
                .newBuilder()
                .setPartyNotKnownOnLedger(PartyNotKnownOnLedger.newBuilder())
                .build()
            )
            .value
            .code shouldBe Code.INVALID_ARGUMENT.value()
        }
      }
    }

    "decode completion info" should {
      val entryId =
        DamlLogEntryId.newBuilder().setEntryId(ByteString.copyFromUtf8("entryid")).build()
      val recordTime = Instant.now()
      def submitterInfo = {
        DamlSubmitterInfo.newBuilder().setApplicationId("id").setCommandId("commandId")
      }

      "use entry id as submission id" in {
        val completionInfo = parseCompletionInfo(
          entryId,
          recordTime,
          submitterInfo.build(),
        )
        completionInfo.submissionId shouldBe s"${Conversions.FillerSubmissionIdPrefix}entryid"
      }

      "use defined submission id" in {
        val submissionId = "submissionId"
        val completionInfo = parseCompletionInfo(
          entryId,
          recordTime,
          submitterInfo.setSubmissionId(submissionId).build(),
        )
        completionInfo.submissionId shouldBe submissionId
      }

      "calculate duration for deduplication for backwards compatibility with deduplicate until" in {
        val completionInfo = parseCompletionInfo(
          entryId,
          recordTime,
          submitterInfo.setDeduplicateUntil(buildTimestamp(recordTime.plusSeconds(30))).build(),
        )
        completionInfo.optDeduplicationPeriod.value shouldBe DeduplicationPeriod
          .DeduplicationDuration(Duration.ofSeconds(30))
      }

      "handle deduplication which is the past relative to record time" in {
        val completionInfo = parseCompletionInfo(
          entryId,
          recordTime,
          submitterInfo.setDeduplicateUntil(buildTimestamp(recordTime.minusSeconds(30))).build(),
        )
        completionInfo.optDeduplicationPeriod.value shouldBe DeduplicationPeriod
          .DeduplicationDuration(Duration.ofSeconds(30))
      }
    }
  }

  private def newDisclosureEntry(node: NodeId, parties: List[String]) =
    DisclosureEntry.newBuilder
      .setNodeId(node.index.toString)
      .addAllDisclosedToLocalParties(parties.asJava)
      .build

  private def newDivulgenceEntry(
      contractId: String,
      parties: List[String],
      contractInstance: TransactionOuterClass.ContractInstance,
  ) =
    DivulgenceEntry.newBuilder
      .setContractId(contractId)
      .addAllDivulgedToLocalParties(parties.asJava)
      .setContractInstance(contractInstance)
      .build

  private lazy val party0: Party = Party.assertFromString("party0")
  private lazy val party1: Party = Party.assertFromString("party1")
  private lazy val contractId0: ContractId = ContractId.V1(wronglySortedHashes.tail.head)
  private lazy val contractId1: ContractId = ContractId.V1(wronglySortedHashes.head)
  private lazy val node0: NodeId = NodeId(0)
  private lazy val node1: NodeId = NodeId(1)
  private lazy val wronglySortedPartySet = ListSet(party1, party0)
  private lazy val wronglySortedHashes: List[Hash] =
    List(crypto.Hash.hashPrivateKey("hash0"), crypto.Hash.hashPrivateKey("hash1")).sorted.reverse
  private lazy val wronglySortedDisclosure: Relation[NodeId, Party] =
    ListMap(node1 -> wronglySortedPartySet, node0 -> wronglySortedPartySet)
  private lazy val wronglySortedDivulgence: Relation[ContractId, Party] =
    ListMap(contractId1 -> wronglySortedPartySet, contractId0 -> wronglySortedPartySet)
  private lazy val wronglySortedBlindingInfo = BlindingInfo(
    disclosure = wronglySortedDisclosure,
    divulgence = wronglySortedDivulgence,
  )

  private lazy val Seq(
    (apiContractInstance0, lfContractInstance0),
    (apiContractInstance1, lfContractInstance1),
  ) =
    Seq("contract 0", "contract 1").map(discriminator =>
      apiContractInstance(discriminator) -> lfContractInstance(discriminator)
    )

  private lazy val correctlySortedParties = List(party0, party1)
  private lazy val correctlySortedPartiesAsStrings =
    correctlySortedParties.asInstanceOf[List[String]]
  private lazy val correctlySortedEncodedBlindingInfo =
    DamlTransactionBlindingInfo.newBuilder
      .addAllDisclosures(
        List(
          newDisclosureEntry(node0, correctlySortedPartiesAsStrings),
          newDisclosureEntry(node1, correctlySortedPartiesAsStrings),
        ).asJava
      )
      .addAllDivulgences(
        List(
          newDivulgenceEntry(
            contractId0.coid,
            correctlySortedPartiesAsStrings,
            apiContractInstance0,
          ),
          newDivulgenceEntry(
            contractId1.coid,
            correctlySortedPartiesAsStrings,
            apiContractInstance1,
          ),
        ).asJava
      )
      .build

  private def deduplicationKeyBytesFor(parties: List[String]): Array[Byte] = {
    val submitterInfo = DamlSubmitterInfo.newBuilder
      .setApplicationId("test")
      .setCommandId("a command ID")
      .setDeduplicateUntil(com.google.protobuf.Timestamp.getDefaultInstance)
      .addAllSubmitters(parties.asJava)
      .build
    val deduplicationKey = commandDedupKey(submitterInfo)
    deduplicationKey.toByteArray
  }

  private def apiContractInstance(discriminator: String) =
    TransactionOuterClass.ContractInstance
      .newBuilder()
      .setTemplateId(
        ValueOuterClass.Identifier
          .newBuilder()
          .setPackageId("some")
          .addModuleName("template")
          .addName("name")
      )
      .setArgVersioned(
        ValueOuterClass.VersionedValue
          .newBuilder()
          .setVersion(TransactionVersion.VDev.protoValue)
          .setValue(
            ValueOuterClass.Value.newBuilder().setText(discriminator).build().toByteString
          )
      )
      .build()

  private def lfContractInstance(discriminator: String) =
    TransactionBuilder(TransactionVersion.VDev).versionContract(
      ContractInst(
        Ref.Identifier.assertFromString("some:template:name"),
        ValueText(discriminator),
        "",
      )
    )
}
