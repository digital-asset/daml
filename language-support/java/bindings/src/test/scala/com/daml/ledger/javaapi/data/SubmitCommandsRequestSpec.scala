// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import java.time.{Duration, Instant}
import java.util.Optional

import com.daml.ledger.api.v1.CommandsOuterClass
import com.daml.ledger.api.v1.CommandsOuterClass.Commands.DeduplicationPeriodCase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

// Allows using deprecated Protobuf fields for testing
@annotation.nowarn("cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\..*")
final class SubmitCommandsRequestSpec extends AnyFlatSpec with Matchers {

  behavior of "SubmitCommandsRequest.toProto/fromProto"

  it should "return the expected submissionId in different overloads" in {

    withClue("(String, String, String, String, String, Optional, Optional, Optional, List)") {
      val proto =
        SubmitCommandsRequest.toProto(
          "ledgerId",
          "workflowId",
          "applicationId",
          "commandId",
          "Alice",
          Optional.empty[Instant](),
          Optional.empty[Duration](),
          Optional.empty[Duration](),
          List.empty[Command].asJava,
        )

      proto.getSubmissionId shouldBe ""

      val request = SubmitCommandsRequest.fromProto(proto)

      request.getSubmissionId shouldEqual Optional.empty()
    }

    withClue(
      "(String, String, String, String, String, String, Optional, Optional, Optional, List)"
    ) {
      val proto =
        SubmitCommandsRequest.toProto(
          "ledgerId",
          "workflowId",
          "applicationId",
          "commandId",
          "submissionId",
          "Alice",
          Optional.empty[Instant](),
          Optional.empty[Duration](),
          Optional.empty[Duration](),
          List.empty[Command].asJava,
        )

      proto.getSubmissionId shouldBe "submissionId"

      val request = SubmitCommandsRequest.fromProto(proto)

      request.getSubmissionId shouldEqual Optional.of("submissionId")
    }

    withClue("(String, String, String, String, List, List, Optional, Optional, Optional, List)") {
      val proto =
        SubmitCommandsRequest.toProto(
          "ledgerId",
          "workflowId",
          "applicationId",
          "commandId",
          List("Alice").asJava,
          List.empty[String].asJava,
          Optional.empty[Instant](),
          Optional.empty[Duration](),
          Optional.empty[Duration](),
          List.empty[Command].asJava,
        )

      proto.getSubmissionId shouldBe ""

      val request = SubmitCommandsRequest.fromProto(proto)

      request.getSubmissionId shouldEqual Optional.empty()
    }

    withClue(
      "(String, String, String, String, String, List, List, Optional, Optional, Optional, List)"
    ) {
      val proto =
        SubmitCommandsRequest.toProto(
          "ledgerId",
          "workflowId",
          "applicationId",
          "commandId",
          "submissionId",
          List("Alice").asJava,
          List.empty[String].asJava,
          Optional.empty[Instant](),
          Optional.empty[Duration](),
          Optional.empty[Duration](),
          List.empty[Command].asJava,
        )

      proto.getSubmissionId shouldBe "submissionId"

      val request = SubmitCommandsRequest.fromProto(proto)

      request.getSubmissionId shouldEqual Optional.of("submissionId")

    }

  }

  it should "return the expected deduplicationTime/deduplicationDuration in different overloads (set)" in {

    val duration = Duration.ofSeconds(42, 47)

    withClue("(String, String, String, String, String, Optional, Optional, Optional, List)") {
      val proto =
        SubmitCommandsRequest.toProto(
          "ledgerId",
          "workflowId",
          "applicationId",
          "commandId",
          "Alice",
          Optional.empty[Instant](),
          Optional.empty[Duration](),
          Optional.of(duration),
          List.empty[Command].asJava,
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
      "(String, String, String, String, String, String, Optional, Optional, Optional, List)"
    ) {
      val proto =
        SubmitCommandsRequest.toProto(
          "ledgerId",
          "workflowId",
          "applicationId",
          "commandId",
          "submissionId",
          "Alice",
          Optional.empty[Instant](),
          Optional.empty[Duration](),
          Optional.of(duration),
          List.empty[Command].asJava,
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

    withClue("(String, String, String, String, List, List, Optional, Optional, Optional, List)") {
      val proto =
        SubmitCommandsRequest.toProto(
          "ledgerId",
          "workflowId",
          "applicationId",
          "commandId",
          List("Alice").asJava,
          List.empty[String].asJava,
          Optional.empty[Instant](),
          Optional.empty[Duration](),
          Optional.of(duration),
          List.empty[Command].asJava,
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
      "(String, String, String, String, String, List, List, Optional, Optional, Optional, List)"
    ) {
      val proto =
        SubmitCommandsRequest.toProto(
          "ledgerId",
          "workflowId",
          "applicationId",
          "commandId",
          "submissionId",
          List("Alice").asJava,
          List.empty[String].asJava,
          Optional.empty[Instant](),
          Optional.empty[Duration](),
          Optional.of(duration),
          List.empty[Command].asJava,
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

    withClue("(String, String, String, String, String, Optional, Optional, Optional, List)") {
      val proto =
        SubmitCommandsRequest.toProto(
          "ledgerId",
          "workflowId",
          "applicationId",
          "commandId",
          "Alice",
          Optional.empty[Instant](),
          Optional.empty[Duration](),
          Optional.empty[Duration](),
          List.empty[Command].asJava,
        )

      // We are sticking on the now deprecated deduplicationTime on purpose for backward compatibility
      proto.getDeduplicationPeriodCase shouldBe DeduplicationPeriodCase.DEDUPLICATIONPERIOD_NOT_SET
      proto.hasDeduplicationTime shouldBe false

      proto.hasDeduplicationDuration shouldBe false

      val request = SubmitCommandsRequest.fromProto(proto)

      request.getDeduplicationTime shouldEqual Optional.empty()
    }

    withClue(
      "(String, String, String, String, String, String, Optional, Optional, Optional, List)"
    ) {
      val proto =
        SubmitCommandsRequest.toProto(
          "ledgerId",
          "workflowId",
          "applicationId",
          "commandId",
          "submissionId",
          "Alice",
          Optional.empty[Instant](),
          Optional.empty[Duration](),
          Optional.empty[Duration](),
          List.empty[Command].asJava,
        )

      // We are sticking on the now deprecated deduplicationTime on purpose for backward compatibility
      proto.getDeduplicationPeriodCase shouldBe DeduplicationPeriodCase.DEDUPLICATIONPERIOD_NOT_SET
      proto.hasDeduplicationTime shouldBe false

      proto.hasDeduplicationDuration shouldBe false

      val request = SubmitCommandsRequest.fromProto(proto)

      request.getDeduplicationTime shouldEqual Optional.empty()
    }

    withClue("(String, String, String, String, List, List, Optional, Optional, Optional, List)") {
      val proto =
        SubmitCommandsRequest.toProto(
          "ledgerId",
          "workflowId",
          "applicationId",
          "commandId",
          List("Alice").asJava,
          List.empty[String].asJava,
          Optional.empty[Instant](),
          Optional.empty[Duration](),
          Optional.empty[Duration](),
          List.empty[Command].asJava,
        )

      // We are sticking on the now deprecated deduplicationTime on purpose for backward compatibility
      proto.getDeduplicationPeriodCase shouldBe DeduplicationPeriodCase.DEDUPLICATIONPERIOD_NOT_SET
      proto.hasDeduplicationTime shouldBe false

      proto.hasDeduplicationDuration shouldBe false

      val request = SubmitCommandsRequest.fromProto(proto)

      request.getDeduplicationTime shouldEqual Optional.empty()
    }

    withClue(
      "(String, String, String, String, String, List, List, Optional, Optional, Optional, List)"
    ) {
      val proto =
        SubmitCommandsRequest.toProto(
          "ledgerId",
          "workflowId",
          "applicationId",
          "commandId",
          "submissionId",
          List("Alice").asJava,
          List.empty[String].asJava,
          Optional.empty[Instant](),
          Optional.empty[Duration](),
          Optional.empty[Duration](),
          List.empty[Command].asJava,
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

    val proto =
      CommandsOuterClass.Commands
        .newBuilder(
          SubmitCommandsRequest.toProto(
            "ledgerId",
            "workflowId",
            "applicationId",
            "commandId",
            "Alice",
            Optional.empty[Instant](),
            Optional.empty[Duration](),
            Optional.empty[Duration](),
            List.empty[Command].asJava,
          )
        )
        .setDeduplicationDuration(
          com.google.protobuf.Duration.newBuilder().setSeconds(42).setNanos(47).build()
        )
        .build()

    val request = SubmitCommandsRequest.fromProto(proto)

    request.getDeduplicationTime shouldEqual Optional.of(Duration.ofSeconds(42, 47))

  }

}
