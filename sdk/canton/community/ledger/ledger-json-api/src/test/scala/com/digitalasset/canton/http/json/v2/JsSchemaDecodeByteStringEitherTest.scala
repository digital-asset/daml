// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.canton.http.json.v2.JsSchema.{
  BYTE_STRING_PARSE_ERROR_TEMPLATE,
  DirectScalaPbRwImplicits,
}
import com.google.protobuf.ByteString as PbByteString
import io.circe.parser.decode
import io.circe.{DecodingFailure, Error}
import org.scalatest.EitherValues
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class JsSchemaDecodeByteStringEitherTest extends AnyFreeSpec with Matchers with EitherValues {

  private val VALID_INPUT = "AQID" // Base64 for JSON
  private val INVALID_INPUT = "invalid-base64"

  private def invokeDecodeByteStringEither(input: String): Either[Error, PbByteString] = {
    import DirectScalaPbRwImplicits.*
    decode[PbByteString](s"\"$input\"")
  }

  "decodeByteStringEither" - {
    "should decode a valid base64 string" in {
      val res = invokeDecodeByteStringEither(VALID_INPUT)
      res match {
        case Right(bs) => bs.toByteArray.toSeq shouldBe Seq(1.toByte, 2.toByte, 3.toByte)
        case Left(err) => fail(s"Expected Right but got Left($err)")
      }
    }

    "should return DecodeFailure on invalid base64" in {
      val res = invokeDecodeByteStringEither(INVALID_INPUT)
      res match {
        case Left(err) =>
          err match {
            case DecodingFailure(message, _) =>
              message shouldBe BYTE_STRING_PARSE_ERROR_TEMPLATE.format(INVALID_INPUT)
            case _ => fail(s"Expected DecodingFailure but got $err")
          }
        case Right(v) => fail(s"Expected Left but got Right($v)")
      }
    }
  }
}
