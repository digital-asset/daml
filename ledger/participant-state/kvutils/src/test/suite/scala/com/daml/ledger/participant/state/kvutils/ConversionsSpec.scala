// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.error.{DamlContextualizedErrorLogger, ErrorResource}
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.configuration.LedgerTimeModel
import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.committer.transaction.Rejection
import com.daml.ledger.participant.state.kvutils.store.{DamlStateKey, Identifier}
import com.daml.ledger.participant.state.kvutils.store.events.DamlTransactionBlindingInfo.{
  DisclosureEntry,
  DivulgenceEntry,
}
import com.daml.ledger.participant.state.kvutils.store.events.{
  DamlSubmitterInfo,
  DamlTransactionBlindingInfo,
  DamlTransactionRejectionEntry,
  Duplicate,
}
import com.daml.ledger.participant.state.v2.Update.CommandRejected
import com.daml.lf.crypto
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Party, QualifiedName}
import com.daml.lf.data.Relation.Relation
import com.daml.lf.data.Time.{Timestamp => LfTimestamp}
import com.daml.lf.engine.Error
import com.daml.lf.kv.contracts.RawContractInstance
import com.daml.lf.transaction.{BlindingInfo, NodeId, TransactionOuterClass, TransactionVersion}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.ValueOuterClass
import com.google.rpc.error_details.{ErrorInfo, ResourceInfo}
import io.grpc.Status.Code
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.{Table, forAll}
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import scala.annotation.nowarn
import scala.collection.immutable.{ListMap, ListSet}
import scala.jdk.CollectionConverters._

@nowarn("msg=deprecated")
class ConversionsSpec extends AnyWordSpec with Matchers with OptionValues {
  implicit private val errorLoggingContext: DamlContextualizedErrorLogger =
    DamlContextualizedErrorLogger.forTesting(getClass)

  "Conversions" should {
    "correctly and deterministically encode Blindinginfo" in {
      encodeBlindingInfo(
        wronglySortedBlindingInfo,
        Map(
          contractId0 -> rawApiContractInstance0,
          contractId1 -> rawApiContractInstance1,
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
          contractId0 -> rawApiContractInstance0,
          contractId1 -> rawApiContractInstance1,
        )
      )
    }

    "return Left with missing contract IDs when extracting divulged contracts if a contract instance is missing" in {
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

      "convert rejection to proto models and back to expected grpc v2 code" in {
        forAll(
          Table[Rejection, Code, Map[String, String], Map[ErrorResource, String]](
            (
              "Rejection",
              "Expected Code",
              "Expected Additional Details",
              "Expected Resources",
            ),
            (
              Rejection.ValidationFailure(
                Error.Package(Error.Package.Internal("ERROR", "ERROR", None))
              ),
              Code.FAILED_PRECONDITION,
              Map.empty,
              Map.empty,
            ),
            (
              Rejection.InternallyInconsistentTransaction.InconsistentKeys,
              Code.INTERNAL,
              Map.empty,
              Map.empty,
            ),
            (
              Rejection.InternallyInconsistentTransaction.DuplicateKeys,
              Code.INTERNAL,
              Map.empty,
              Map.empty,
            ),
            (
              Rejection.ExternallyInconsistentTransaction.InconsistentContracts,
              Code.FAILED_PRECONDITION,
              Map.empty,
              Map.empty,
            ),
            (
              Rejection.ExternallyInconsistentTransaction.InconsistentKeys,
              Code.FAILED_PRECONDITION,
              Map.empty,
              Map.empty,
            ),
            (
              Rejection.ExternallyInconsistentTransaction.DuplicateKeys,
              Code.ALREADY_EXISTS,
              Map.empty,
              Map.empty,
            ),
            (
              Rejection.MissingInputState(DamlStateKey.getDefaultInstance),
              Code.INTERNAL,
              Map.empty,
              Map.empty,
            ),
            (
              Rejection.InvalidParticipantState(Err.InternalError("error")),
              Code.INTERNAL,
              Map.empty,
              Map.empty,
            ),
            (
              Rejection.InvalidParticipantState(
                Err.ArchiveDecodingFailed(Ref.PackageId.assertFromString("id"), "reason")
              ),
              Code.INTERNAL,
              Map.empty, // package ID could be useful but the category is security sensitive
              Map.empty,
            ),
            (
              Rejection.InvalidParticipantState(Err.MissingDivulgedContractInstance("id")),
              Code.INTERNAL,
              Map.empty, // contract ID could be useful but the category is security sensitive
              Map.empty,
            ),
            (
              Rejection.RecordTimeOutOfRange(LfTimestamp.Epoch, LfTimestamp.Epoch),
              Code.FAILED_PRECONDITION,
              Map(
                "minimum_record_time" -> LfTimestamp.Epoch.toString,
                "maximum_record_time" -> LfTimestamp.Epoch.toString,
              ),
              Map.empty,
            ),
            (
              Rejection.LedgerTimeOutOfRange(
                LedgerTimeModel.OutOfRange(LfTimestamp.Epoch, LfTimestamp.Epoch, LfTimestamp.Epoch)
              ),
              Code.FAILED_PRECONDITION,
              Map(
                "ledger_time" -> LfTimestamp.Epoch.toString,
                "ledger_time_lower_bound" -> LfTimestamp.Epoch.toString,
                "ledger_time_upper_bound" -> LfTimestamp.Epoch.toString,
              ),
              Map.empty,
            ),
            (
              Rejection.CausalMonotonicityViolated,
              Code.FAILED_PRECONDITION,
              Map.empty,
              Map.empty,
            ),
            (
              Rejection.MissingInputState(partyStateKey("party")),
              Code.INTERNAL,
              Map.empty, // the missing state key could be useful but the category is security sensitive
              Map.empty,
            ),
            (
              Rejection.PartiesNotKnownOnLedger(Seq.empty),
              Code.NOT_FOUND,
              Map.empty,
              Map.empty,
            ),
            (
              Rejection.SubmittingPartyNotKnownOnLedger(party0),
              Code.NOT_FOUND,
              Map.empty,
              Map(ErrorResource.Party -> party0),
            ),
            (
              Rejection.PartiesNotKnownOnLedger(Iterable(party0, party1)),
              Code.NOT_FOUND,
              Map.empty,
              Map(ErrorResource.Party -> party0, ErrorResource.Party -> party1),
            ),
          )
        ) { (rejection, expectedCode, expectedAdditionalDetails, expectedResources) =>
          checkErrors(
            submitterInfo,
            rejection,
            expectedCode,
            expectedAdditionalDetails,
            expectedResources,
          )
        }
      }
    }

    "decode duplicate command v2" in {
      val finalReason = Conversions
        .decodeTransactionRejectionEntry(
          DamlTransactionRejectionEntry
            .newBuilder()
            .setDuplicateCommand(Duplicate.newBuilder().setSubmissionId("submissionId"))
            .build()
        )
      finalReason.code shouldBe Code.ALREADY_EXISTS.value()
      finalReason.definiteAnswer shouldBe false
      val actualDetails = finalReasonDetails(finalReason)
      actualDetails should contain allElementsOf Map(
        "existing_submission_id" -> "submissionId"
      )
    }

    "decode completion info" should {
      val recordTime = LfTimestamp.now()
      def submitterInfo = {
        DamlSubmitterInfo.newBuilder().setApplicationId("id").setCommandId("commandId")
      }

      "use empty submission ID" in {
        val completionInfo = parseCompletionInfo(
          recordTime,
          submitterInfo.build(),
        )
        completionInfo.submissionId shouldBe None
      }

      "use defined submission ID" in {
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
          submitterInfo
            .setDeduplicateUntil(buildTimestamp(recordTime.add(Duration.ofSeconds(30))))
            .build(),
        )
        completionInfo.optDeduplicationPeriod.value shouldBe DeduplicationPeriod
          .DeduplicationDuration(Duration.ofSeconds(30))
      }

      "handle deduplication which is the past relative to record time by using absolute values" in {
        val completionInfo = parseCompletionInfo(
          recordTime,
          submitterInfo
            .setDeduplicateUntil(buildTimestamp(recordTime.add(Duration.ofSeconds(30))))
            .build(),
        )
        completionInfo.optDeduplicationPeriod.value shouldBe DeduplicationPeriod
          .DeduplicationDuration(Duration.ofSeconds(30))
      }
    }

    "encode/decode Identifiers" should {
      "successfully encode various names" in {
        forAll(
          Table[Ref.ModuleName, Ref.DottedName, Seq[String], Seq[String]](
            ("Module Name", "Name", "Expected Modules", "Expected Names"),
            (
              Ref.ModuleName.assertFromString("module"),
              Ref.DottedName.assertFromString("name"),
              Seq("module"),
              Seq("name"),
            ),
            (
              Ref.ModuleName.assertFromString("module.name"),
              Ref.DottedName.assertFromString("dotted.name"),
              Seq("module", "name"),
              Seq("dotted", "name"),
            ),
          )
        ) { (moduleName, name, expectedModules, expectedNames) =>
          val id = Ref.Identifier(
            Ref.PackageId.assertFromString("packageId"),
            QualifiedName(moduleName, name),
          )

          val actual = Conversions.encodeIdentifier(id)
          actual.getPackageId shouldBe "packageId"
          actual.getModuleNameList.asScala shouldBe expectedModules
          actual.getNameList.asScala shouldBe expectedNames
        }
      }

      "decode various Identifiers" in {
        forAll(
          Table[Identifier, Either[Err.DecodeError, Ref.Identifier]](
            ("Identifier", "Expected Error or Ref.Identifier"),
            (
              Identifier
                .newBuilder()
                .setPackageId("packageId")
                .addModuleName("module")
                .addName("name")
                .build(),
              Right(
                Ref.Identifier(
                  Ref.PackageId.assertFromString("packageId"),
                  QualifiedName(
                    Ref.ModuleName.assertFromString("module"),
                    Ref.DottedName.assertFromString("name"),
                  ),
                )
              ),
            ),
            (
              Identifier
                .newBuilder()
                .setPackageId("packageId")
                .addModuleName("module")
                .addModuleName("name")
                .addName("dotted")
                .addName("name")
                .build(),
              Right(
                Ref.Identifier(
                  Ref.PackageId.assertFromString("packageId"),
                  QualifiedName(
                    Ref.ModuleName.assertFromString("module.name"),
                    Ref.DottedName.assertFromString("dotted.name"),
                  ),
                )
              ),
            ),
            (
              Identifier
                .newBuilder()
                .addModuleName("module")
                .addName("name")
                .build(),
              Left(Err.DecodeError("Identifier", "Invalid package ID: ''")),
            ),
            (
              Identifier
                .newBuilder()
                .setPackageId("packageId")
                .addModuleName(">>>")
                .addName("name")
                .build(),
              Left(Err.DecodeError("Identifier", "Invalid module segments: '>>>'")),
            ),
            (
              Identifier
                .newBuilder()
                .setPackageId("packageId")
                .addModuleName("module")
                .addName("???")
                .build(),
              Left(Err.DecodeError("Identifier", "Invalid name segments: '???'")),
            ),
          )
        ) { (identifier, expectedErrorOrRefIdentifier) =>
          val actual = Conversions.decodeIdentifier(identifier)
          actual shouldBe expectedErrorOrRefIdentifier
        }
      }
    }
  }

  private def checkErrors(
      submitterInfo: DamlSubmitterInfo,
      rejection: Rejection,
      expectedCode: Code,
      expectedAdditionalDetails: Map[String, String],
      expectedResources: Map[ErrorResource, String],
  ) = {
    val encodedEntry = Conversions
      .encodeTransactionRejectionEntry(
        submitterInfo,
        rejection,
      )
      .build()
    val finalReason = Conversions
      .decodeTransactionRejectionEntry(encodedEntry)
    finalReason.code shouldBe expectedCode.value()
    finalReason.definiteAnswer shouldBe false
    val actualDetails = finalReasonDetails(finalReason)
    val actualResources = finalReasonResources(finalReason)
    actualDetails should contain allElementsOf expectedAdditionalDetails
    actualResources should contain allElementsOf expectedResources
  }

  private def newDisclosureEntry(node: NodeId, parties: List[String]) =
    DisclosureEntry.newBuilder
      .setNodeId(node.index.toString)
      .addAllDisclosedToLocalParties(parties.asJava)
      .build

  private def newDivulgenceEntry(
      contractId: String,
      parties: List[String],
      rawContractInstance: RawContractInstance,
  ) =
    DivulgenceEntry.newBuilder
      .setContractId(contractId)
      .addAllDivulgedToLocalParties(parties.asJava)
      .setRawContractInstance(rawContractInstance.byteString)
      .build

  private lazy val party0: Party = Party.assertFromString("party0")
  private lazy val party1: Party = Party.assertFromString("party1")
  private lazy val contractId0: ContractId = ContractId.V1(wronglySortedHashes.tail.head)
  private lazy val contractId1: ContractId = ContractId.V1(wronglySortedHashes.head)
  private lazy val node0: NodeId = NodeId(0)
  private lazy val node1: NodeId = NodeId(1)
  private lazy val rawApiContractInstance0 = rawApiContractInstance("contract 0")
  private lazy val rawApiContractInstance1 = rawApiContractInstance("contract 1")

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
            rawApiContractInstance0,
          ),
          newDivulgenceEntry(
            contractId1.coid,
            correctlySortedPartiesAsStrings,
            rawApiContractInstance1,
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

  private def rawApiContractInstance(discriminator: String) = {
    val contractInstance = TransactionOuterClass.ContractInstance
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
    RawContractInstance(contractInstance.toByteString)
  }

  private def finalReasonDetails(
      finalReason: CommandRejected.FinalReason
  ): Seq[(String, String)] =
    finalReason.status.details.flatMap { anyProto =>
      if (anyProto.is[ErrorInfo])
        anyProto.unpack[ErrorInfo].metadata
      else
        Map.empty[String, String]
    }

  private def finalReasonResources(
      finalReason: CommandRejected.FinalReason
  ): Seq[(ErrorResource, String)] =
    finalReason.status.details.flatMap { anyProto =>
      if (anyProto.is[ResourceInfo]) {
        val resourceInfo = anyProto.unpack[ResourceInfo]
        Map(ErrorResource.fromString(resourceInfo.resourceType).get -> resourceInfo.resourceName)
      } else {
        Map.empty[ErrorResource, String]
      }
    }
}
