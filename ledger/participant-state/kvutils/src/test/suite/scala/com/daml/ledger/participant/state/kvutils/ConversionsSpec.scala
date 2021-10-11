// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.{Duration, Instant}

import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.configuration.LedgerTimeModel
import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlTransactionBlindingInfo.{
  DisclosureEntry,
  DivulgenceEntry,
}
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.committer.transaction.Rejection
import com.daml.ledger.participant.state.kvutils.store.DamlStateKey
import com.daml.ledger.participant.state.v2.Update.CommandRejected
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
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.{TextFormat, Timestamp}
import com.google.rpc.error_details.ErrorInfo
import io.grpc.Status.Code
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.{Table, forAll}
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn
import scala.collection.immutable.{ListMap, ListSet}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

@nowarn("msg=deprecated")
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
      val now = Instant.now

      "convert rejection to proto models and back to expected grpc code" in {
        forAll(
          Table[Rejection, Code, Map[String, String]](
            ("rejection", "expected code", "expected additional details"),
            (
              Rejection.ValidationFailure(Error.Package(Error.Package.Internal("ERROR", "ERROR"))),
              Code.INTERNAL,
              Map.empty,
            ),
            (
              Rejection.InternallyInconsistentTransaction.InconsistentKeys,
              Code.INTERNAL,
              Map.empty,
            ),
            (
              Rejection.InternallyInconsistentTransaction.DuplicateKeys,
              Code.INTERNAL,
              Map.empty,
            ),
            (
              Rejection.ExternallyInconsistentTransaction.InconsistentContracts,
              Code.FAILED_PRECONDITION,
              Map.empty,
            ),
            (
              Rejection.ExternallyInconsistentTransaction.InconsistentKeys,
              Code.FAILED_PRECONDITION,
              Map.empty,
            ),
            (
              Rejection.ExternallyInconsistentTransaction.DuplicateKeys,
              Code.FAILED_PRECONDITION,
              Map.empty,
            ),
            (
              Rejection.MissingInputState(DamlStateKey.getDefaultInstance),
              Code.INTERNAL,
              Map.empty,
            ),
            (
              Rejection.InvalidParticipantState(Err.InternalError("error")),
              Code.INTERNAL,
              Map.empty,
            ),
            (
              Rejection.InvalidParticipantState(
                Err.ArchiveDecodingFailed(Ref.PackageId.assertFromString("id"), "reason")
              ),
              Code.INTERNAL,
              Map("package_id" -> "id"),
            ),
            (
              Rejection.InvalidParticipantState(Err.MissingDivulgedContractInstance("id")),
              Code.INTERNAL,
              Map("contract_id" -> "id"),
            ),
            (
              Rejection.RecordTimeOutOfRange(now, now),
              Code.FAILED_PRECONDITION,
              Map.empty,
            ),
            (
              Rejection.LedgerTimeOutOfRange(LedgerTimeModel.OutOfRange(now, now, now)),
              Code.FAILED_PRECONDITION,
              Map.empty,
            ),
            (
              Rejection.CausalMonotonicityViolated,
              Code.FAILED_PRECONDITION,
              Map.empty,
            ),
            (
              Rejection.SubmittingPartyNotKnownOnLedger(Ref.Party.assertFromString("party")),
              Code.FAILED_PRECONDITION,
              Map.empty,
            ),
            (
              Rejection.PartiesNotKnownOnLedger(Seq.empty),
              Code.FAILED_PRECONDITION,
              Map.empty,
            ),
            (
              Rejection.MissingInputState(partyStateKey("party")),
              Code.INTERNAL,
              Map("key" -> "party: \"party\"\n"),
            ),
            (
              Rejection.RecordTimeOutOfRange(Instant.EPOCH, Instant.EPOCH),
              Code.FAILED_PRECONDITION,
              Map(
                "minimum_record_time" -> Instant.EPOCH.toString,
                "maximum_record_time" -> Instant.EPOCH.toString,
              ),
            ),
            (
              Rejection.SubmittingPartyNotKnownOnLedger(party0),
              Code.FAILED_PRECONDITION,
              Map("submitter_party" -> party0),
            ),
            (
              Rejection.PartiesNotKnownOnLedger(Iterable(party0, party1)),
              Code.FAILED_PRECONDITION,
              Map("parties" -> s"""[\"$party0\",\"$party1\"]"""),
            ),
          )
        ) { (rejection, expectedCode, expectedAdditionalDetails) =>
          val encodedEntry = Conversions
            .encodeTransactionRejectionEntry(
              submitterInfo,
              rejection,
            )
            .build()
          val finalReason = Conversions
            .decodeTransactionRejectionEntry(encodedEntry)
            .value
          finalReason.code shouldBe expectedCode.value()
          finalReason.definiteAnswer shouldBe false
          val actualDetails = finalReasonToDetails(finalReason)
          actualDetails should contain allElementsOf (expectedAdditionalDetails)
        }
      }

      "produce metadata that can be easily parsed" in {
        forAll(
          Table[Rejection, String, String => Any, Any](
            ("rejection", "metadata key", "metadata parser", "expected parsed metadata"),
            (
              Rejection.MissingInputState(partyStateKey("party")),
              "key",
              TextFormat.parse(_, classOf[DamlStateKey]),
              partyStateKey("party"),
            ),
            (
              Rejection.RecordTimeOutOfRange(Instant.EPOCH, Instant.EPOCH),
              "minimum_record_time",
              Instant.parse(_),
              Instant.EPOCH,
            ),
            (
              Rejection.RecordTimeOutOfRange(Instant.EPOCH, Instant.EPOCH),
              "maximum_record_time",
              Instant.parse(_),
              Instant.EPOCH,
            ),
            (
              Rejection.PartiesNotKnownOnLedger(Iterable(party0, party1)),
              "parties",
              jsonString => {
                val objectMapper = new ObjectMapper
                objectMapper.readValue(jsonString, classOf[java.util.List[_]]).asScala
              },
              mutable.Buffer(party0, party1),
            ),
          )
        ) { (rejection, metadataKey, metadataParser, expectedParsedMetadata) =>
          val encodedEntry = Conversions
            .encodeTransactionRejectionEntry(
              submitterInfo,
              rejection,
            )
            .build()
          val finalReason = Conversions
            .decodeTransactionRejectionEntry(encodedEntry)
            .value
          finalReason.definiteAnswer shouldBe false
          val actualDetails = finalReasonToDetails(finalReason).toMap
          metadataParser(actualDetails(metadataKey)) shouldBe expectedParsedMetadata
        }
      }

      "convert v1 rejections" should {

        "handle with expected status codes" in {
          forAll(
            Table[
              DamlTransactionRejectionEntry.Builder => DamlTransactionRejectionEntry.Builder,
              Code,
              Map[String, String],
            ](
              ("rejection builder", "code", "expected additional details"),
              (
                _.setInconsistent(Inconsistent.newBuilder()),
                Code.FAILED_PRECONDITION,
                Map.empty,
              ),
              (
                _.setDisputed(Disputed.newBuilder()),
                Code.INTERNAL,
                Map.empty,
              ),
              (
                _.setResourcesExhausted(ResourcesExhausted.newBuilder()),
                Code.ABORTED,
                Map.empty,
              ),
              (
                _.setPartyNotKnownOnLedger(PartyNotKnownOnLedger.newBuilder()),
                Code.FAILED_PRECONDITION,
                Map.empty,
              ),
              (
                _.setDuplicateCommand(Duplicate.newBuilder()),
                Code.ALREADY_EXISTS,
                Map.empty,
              ),
              (
                _.setSubmitterCannotActViaParticipant(
                  SubmitterCannotActViaParticipant
                    .newBuilder()
                    .setSubmitterParty("party")
                    .setParticipantId("id")
                ),
                Code.PERMISSION_DENIED,
                Map(
                  "submitter_party" -> "party",
                  "participant_id" -> "id",
                ),
              ),
              (
                _.setInvalidLedgerTime(
                  InvalidLedgerTime
                    .newBuilder()
                    .setLowerBound(Timestamp.newBuilder().setSeconds(1L))
                    .setLedgerTime(Timestamp.newBuilder().setSeconds(2L))
                    .setUpperBound(Timestamp.newBuilder().setSeconds(3L))
                ),
                Code.FAILED_PRECONDITION,
                Map(
                  "lower_bound" -> "seconds: 1\n",
                  "ledger_time" -> "seconds: 2\n",
                  "upper_bound" -> "seconds: 3\n",
                ),
              ),
            )
          ) { (rejectionBuilder, code, expectedAdditionalDetails) =>
            {
              val finalReason = Conversions
                .decodeTransactionRejectionEntry(
                  rejectionBuilder(DamlTransactionRejectionEntry.newBuilder())
                    .build()
                )
                .value
              finalReason.code shouldBe code.value()
              finalReason.definiteAnswer shouldBe false
              val actualDetails = finalReasonToDetails(finalReason)
              actualDetails should contain allElementsOf (expectedAdditionalDetails)
            }
          }
        }
      }
    }

    "decode completion info" should {
      val recordTime = Instant.now()
      def submitterInfo = {
        DamlSubmitterInfo.newBuilder().setApplicationId("id").setCommandId("commandId")
      }

      "use empty submission id" in {
        val completionInfo = parseCompletionInfo(
          recordTime,
          submitterInfo.build(),
        )
        completionInfo.submissionId shouldBe None
      }

      "use defined submission id" in {
        val submissionId = "submissionId"
        val completionInfo = parseCompletionInfo(
          recordTime,
          submitterInfo.setSubmissionId(submissionId).build(),
        )
        completionInfo.submissionId.value shouldBe submissionId
      }

      "calculate duration for deduplication for backwards compatibility with deduplicate until" in {
        val completionInfo = parseCompletionInfo(
          recordTime,
          submitterInfo.setDeduplicateUntil(buildTimestamp(recordTime.plusSeconds(30))).build(),
        )
        completionInfo.optDeduplicationPeriod.value shouldBe DeduplicationPeriod
          .DeduplicationDuration(Duration.ofSeconds(30))
      }

      "handle deduplication which is the past relative to record time by using absolute values" in {
        val completionInfo = parseCompletionInfo(
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

  private[this] val txVersion = TransactionVersion.StableVersions.max

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
          .setVersion(txVersion.protoValue)
          .setValue(
            ValueOuterClass.Value.newBuilder().setText(discriminator).build().toByteString
          )
      )
      .build()

  private def lfContractInstance(discriminator: String) =
    new TransactionBuilder(_ => txVersion).versionContract(
      ContractInst(
        Ref.Identifier.assertFromString("some:template:name"),
        ValueText(discriminator),
        "",
      )
    )

  private def finalReasonToDetails(
      finalReason: CommandRejected.FinalReason
  ): Seq[(String, String)] = finalReason.status.details.flatMap(_.unpack[ErrorInfo].metadata)
}
