// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data

import java.time.Duration
import java.util.Optional

import com.daml.ledger.api.v1.CommandsOuterClass
import com.daml.ledger.api.v1.CommandsOuterClass.Commands.DeduplicationPeriodCase
import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

// Allows using deprecated Protobuf fields for testing
@annotation.nowarn("cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\..*")
final class SubmitCommandsRequestSpec extends AnyFlatSpec with Matchers {

  behavior of "SubmitCommandsRequest.toProto/fromProto"

  it should "return the expected submissionId in different overloads" in {
    val commandsSubmission =
      CommandsSubmission
        .create("applicationId", "commandId", List.empty[Command].asJava)
        .withWorkflowId("workflowId")
        .withActAs("Alice")

    withClue("[No submissionId provided]") {
      val proto =
        SubmitCommandsRequest.toProto(
          "ledgerId",
          commandsSubmission,
        )

      proto.getSubmissionId shouldBe ""

      val request = SubmitCommandsRequest.fromProto(proto)

      request.getSubmissionId shouldEqual Optional.empty()
    }

    withClue(
      "[submissionId provided]"
    ) {
      val proto =
        SubmitCommandsRequest.toProto(
          "ledgerId",
          "submissionId",
          commandsSubmission,
        )

      proto.getSubmissionId shouldBe "submissionId"

      val request = SubmitCommandsRequest.fromProto(proto)

      request.getSubmissionId shouldEqual Optional.of("submissionId")
    }
  }

  it should "return the expected deduplicationTime/deduplicationDuration in different overloads (set)" in {

    val duration = Duration.ofSeconds(42, 47)
    val commandsSubmission =
      CommandsSubmission
        .create("applicationId", "commandId", List.empty[Command].asJava)
        .withWorkflowId("workflowId")
        .withActAs("Alice")
        .withDeduplicationTime(Optional.of(duration))

    withClue("[submissionId not provided]") {
      val proto =
        SubmitCommandsRequest.toProto(
          "ledgerId",
          commandsSubmission,
        )

      // We are sticking on the now deprecated deduplicationTime on purpose for backward compatibility
      proto.getDeduplicationPeriodCase shouldBe DeduplicationPeriodCase.DEDUPLICATION_TIME
      proto.hasDeduplicationTime shouldBe true
      proto.getDeduplicationTime.getSeconds shouldBe 42
      proto.getDeduplicationTime.getNanos shouldBe 47

      proto.hasDeduplicationDuration shouldBe false

      val request = SubmitCommandsRequest.fromProto(proto)

      request.getDeduplicationTime shouldEqual Optional.of(duration)
    }

    withClue(
      "[submissionId provided]"
    ) {
      val proto =
        SubmitCommandsRequest.toProto(
          "ledgerId",
          "submissionId",
          commandsSubmission,
        )

      // We are sticking on the now deprecated deduplicationTime on purpose for backward compatibility
      proto.getDeduplicationPeriodCase shouldBe DeduplicationPeriodCase.DEDUPLICATION_TIME
      proto.hasDeduplicationTime shouldBe true
      proto.getDeduplicationTime.getSeconds shouldBe 42
      proto.getDeduplicationTime.getNanos shouldBe 47

      proto.hasDeduplicationDuration shouldBe false

      val request = SubmitCommandsRequest.fromProto(proto)

      request.getDeduplicationTime shouldEqual Optional.of(duration)
    }
  }

  it should "return the expected deduplicationTime/deduplicationDuration in different overloads (unset)" in {
    val commandsSubmission =
      CommandsSubmission
        .create("applicationId", "commandId", List.empty[Command].asJava)
        .withWorkflowId("workflowId")
        .withActAs("Alice")

    withClue("[submissionId not provided]") {
      val proto =
        SubmitCommandsRequest.toProto(
          "ledgerId",
          commandsSubmission,
        )

      // We are sticking on the now deprecated deduplicationTime on purpose for backward compatibility
      proto.getDeduplicationPeriodCase shouldBe DeduplicationPeriodCase.DEDUPLICATIONPERIOD_NOT_SET
      proto.hasDeduplicationTime shouldBe false

      proto.hasDeduplicationDuration shouldBe false

      val request = SubmitCommandsRequest.fromProto(proto)

      request.getDeduplicationTime shouldEqual Optional.empty()
    }

    withClue(
      "[submissionId provided]"
    ) {
      val proto =
        SubmitCommandsRequest.toProto(
          "ledgerId",
          "submissionId",
          commandsSubmission,
        )

      // We are sticking on the now deprecated deduplicationTime on purpose for backward compatibility
      proto.getDeduplicationPeriodCase shouldBe DeduplicationPeriodCase.DEDUPLICATIONPERIOD_NOT_SET
      proto.hasDeduplicationTime shouldBe false

      proto.hasDeduplicationDuration shouldBe false

      val request = SubmitCommandsRequest.fromProto(proto)

      request.getDeduplicationTime shouldEqual Optional.empty()
    }
  }

  behavior of "SubmitCommandsRequest.fromProto"

  it should "set the deduplicationTime field even when only deduplicationDuration is set" in {
    val commandsSubmission =
      CommandsSubmission
        .create("applicationId", "commandId", List.empty[Command].asJava)
        .withWorkflowId("workflowId")
        .withActAs("Alice")

    val proto =
      CommandsOuterClass.Commands
        .newBuilder(
          SubmitCommandsRequest.toProto(
            "ledgerId",
            commandsSubmission,
          )
        )
        .setDeduplicationDuration(
          com.google.protobuf.Duration.newBuilder().setSeconds(42).setNanos(47).build()
        )
        .build()

    val request = SubmitCommandsRequest.fromProto(proto)

    request.getDeduplicationTime shouldEqual Optional.of(Duration.ofSeconds(42, 47))

  }

  it should "correctly en- and decode packageId selection preference" in {
    val preference = List("my-preference", "noones-preference")
    val commandsSubmission =
      CommandsSubmission
        .create("applicationId", "commandId", List.empty[Command].asJava)
        .withWorkflowId("workflowId")
        .withActAs("Alice")
        .withPackageIdSelectionPreference(preference.asJava)

    val proto =
      CommandsOuterClass.Commands
        .newBuilder(
          SubmitCommandsRequest.toProto(
            "ledgerId",
            commandsSubmission,
          )
        )
        .build()

    val request = SubmitCommandsRequest.fromProto(proto)

    request.getPackageIdSelectionPreference.asScala should contain theSameElementsInOrderAs preference

  }

  it should "correctly en- and decode disclosed contract" in {
    val disclosed = List(new DisclosedContract(
      new Identifier("some-package", "some-module", "some-template"),
      "some-contract-id",
      ByteString.fromHex("0123456789ABCDEF")
    ))
    val commandsSubmission =
      CommandsSubmission
        .create("applicationId", "commandId", List.empty[Command].asJava)
        .withWorkflowId("workflowId")
        .withActAs("Alice")
        .withDisclosedContracts(disclosed.asJava)

    val proto =
      CommandsOuterClass.Commands
        .newBuilder(
          SubmitCommandsRequest.toProto(
            "ledgerId",
            commandsSubmission,
          )
        )
        .build()

    val request = SubmitCommandsRequest.fromProto(proto)

    request.getDisclosedContracts.asScala should contain theSameElementsInOrderAs disclosed

  }
}
