// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc

import com.daml.grpc.GrpcStatus
import com.google.protobuf.any.{Any as AnyProto}
import com.google.protobuf.{Any as AnyJavaProto}
import com.google.rpc.error_details.ErrorInfo
import com.google.rpc.status.{Status as StatusProto}
import com.google.rpc.{ErrorInfo as JavaErrorInfo, Status as StatusJavaProto}
import io.grpc.Status.Code
import io.grpc.{Metadata, Status}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.*
import org.scalatest.wordspec.AnyWordSpec

class GrpcStatusSpec extends AnyWordSpec with Matchers {
  import GrpcStatusSpec.*

  "toProto" should {
    "return Protobuf status" in {
      val testCases = Table(
        ("gRPC status", "metadata", "expected Protobuf status"),
        (
          Status.OK,
          emptyMetadata,
          StatusProto(Code.OK.value()),
        ),
        (
          Status.ABORTED.withDescription("aborted"),
          emptyMetadata,
          StatusProto(Code.ABORTED.value(), "aborted"),
        ),
        (
          Status.ALREADY_EXISTS.withDescription("already exists"), {
            val statusProto = StatusJavaProto
              .newBuilder()
              .setCode(Code.ALREADY_EXISTS.value())
              .addDetails(AnyJavaProto.pack(aJavaErrorInfo))
              .build()
            io.grpc.protobuf.StatusProto.toStatusException(statusProto).getTrailers
          },
          StatusProto(
            Code.ALREADY_EXISTS.value(),
            "already exists",
            Seq(AnyProto.pack(anErrorInfo)),
          ),
        ),
      )

      forAll(testCases) { case (grpcStatus, metadata, expected) =>
        GrpcStatus.toProto(grpcStatus, metadata) shouldBe expected
      }
    }
  }

  "toJavaProto" should {
    "return Java Protobuf status" in {
      val testCases = Table(
        ("Scala Protobuf status", "expected Java Protobuf status"),
        (
          StatusProto(Code.OK.value()),
          StatusJavaProto.newBuilder().setCode(Code.OK.value()).build(),
        ),
        (
          StatusProto(Code.ABORTED.value(), "aborted"),
          StatusJavaProto.newBuilder().setCode(Code.ABORTED.value()).setMessage("aborted").build(),
        ),
        (
          StatusProto(
            Code.ALREADY_EXISTS.value(),
            "already exists",
            Seq(AnyProto.pack(anErrorInfo)),
          ),
          StatusJavaProto
            .newBuilder()
            .setCode(Code.ALREADY_EXISTS.value())
            .setMessage("already exists")
            .addDetails(AnyJavaProto.pack(aJavaErrorInfo))
            .build(),
        ),
      )

      forAll(testCases) { case (status, expectedJavaStatus) =>
        GrpcStatus.toJavaProto(status) shouldBe expectedJavaStatus
      }
    }
  }
}

object GrpcStatusSpec {
  private val emptyMetadata = new Metadata()

  private val aJavaErrorInfo: JavaErrorInfo =
    JavaErrorInfo.newBuilder().putMetadata("key", "value").build()

  private val anErrorInfo: ErrorInfo = ErrorInfo(metadata = Map("key" -> "value"))
}
