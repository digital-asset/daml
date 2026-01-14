// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.google.protobuf.any.Any
import com.google.rpc.error_details.{ErrorInfo, RetryInfo}
import com.google.rpc.status.Status
import io.circe.syntax.*
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GrpcStatusEncoderSpec extends AnyFlatSpec with Matchers with OptionValues {
  import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*

  it should "encode Status with ErrorInfo detail" in {
    val errorInfo = ErrorInfo("reason", "domain", Map("key" -> "value"))
    val errorInfoProto = Any.pack(errorInfo)
    val status = Status(3, "error", Seq(errorInfoProto))

    val json = status.asJson
    val detailsList = json.hcursor.downField("details").values.toList
    detailsList.size shouldBe 1
    val details = detailsList.head
    details.head.hcursor
      .get[String]("valueDecoded")
      .toOption
      .fold(fail("Expected valueDecoded to be present")) { valueDecoded =>
        valueDecoded should be(errorInfo.toString)
      }
  }

  it should "encode Status with RetryInfo detail" in {
    val retryInfo = RetryInfo()
    val retryInfoProto = Any.pack(retryInfo)
    val status = Status(1, "retry", Seq(retryInfoProto))

    val json = status.asJson
    val detailsOpt = json.hcursor.downField("details").values
    detailsOpt should not be empty
    val details = detailsOpt.value
    details.head.hcursor.get[String]("valueDecoded").isRight shouldBe true
    details.head.hcursor
      .get[String]("valueDecoded")
      .toOption
      .fold(fail("Expected valueDecoded to be present")) { valueDecoded =>
        valueDecoded should be(retryInfo.toString)
      }
  }

  it should "encode Status with unknown detail type" in {
    val unknownAny = Any(
      typeUrl = "type.googleapis.com/unknown.Type",
      value = com.google.protobuf.ByteString.copyFromUtf8("data"),
    )
    val status = Status(2, "unknown", Seq(unknownAny))

    val json = status.asJson
    val detailsOpt = json.hcursor.downField("details").values
    detailsOpt should not be empty
    val details = detailsOpt.value
    details.head.hcursor.get[String]("valueDecoded").isRight shouldBe true
    details.head.hcursor
      .get[String]("valueDecoded")
      .toOption
      .fold(fail("Expected valueDecoded to be present")) { valueDecoded =>
        valueDecoded should be("Unknown type for decoding")
      }
  }

  it should "encode Status with no details" in {
    val status = Status(0, "ok", Seq.empty)

    val json = status.asJson
    val detailsOpt = json.hcursor.downField("details").values
    detailsOpt should not be empty
    detailsOpt.fold(fail("Expected details to be present"))(_.isEmpty shouldBe true)
  }
}
