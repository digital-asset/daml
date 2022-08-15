// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.meteringreport

import com.daml.platform.apiserver.meteringreport.MeteringReport.{Check, ParticipantReport, Scheme}
import spray.json.enrichAny
import spray.json._
import DefaultJsonProtocol._

import java.nio.charset.StandardCharsets
import scala.util.{Failure, Success, Try}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.JavaSerializable",
    "org.wartremover.warts.Product",
    "org.wartremover.warts.Serializable",
  )
)
object JcsSigner {

  import HmacSha256._

  type Error = String

  sealed trait VerificationStatus
  object VerificationStatus {
    case object Ok extends VerificationStatus
    final case class DigestMismatch(expected: String) extends VerificationStatus
    final case class InvalidJson(message: String) extends VerificationStatus
    case object MissingCheckSection extends VerificationStatus
    final case class UnknownScheme(scheme: String) extends VerificationStatus
    final case class CheckGeneration(message: String) extends VerificationStatus
  }

  import VerificationStatus._

  def sign(
      report: ParticipantReport,
      scheme: Scheme,
      key: Key,
  ): Either[Error, ParticipantReport] = {
    generateCheck(report: ParticipantReport, scheme: Scheme, key: Key).map(check =>
      report.copy(check = Some(check))
    )
  }

  private def generateCheck(
      report: ParticipantReport,
      scheme: Scheme,
      key: Key,
  ): Either[Error, Check] = {
    for {
      jcs <- Jcs.serialize(report.copy(check = None).toJson)
      digest <- HmacSha256.compute(key, jcs.getBytes(StandardCharsets.UTF_8)).left.map(_.getMessage)
    } yield Check(scheme, toBase64(digest))
  }

  def verify(json: String, keyLookup: Scheme => Option[Key]): VerificationStatus = {
    Try(json.toJson.convertTo[ParticipantReport]) match {
      case Success(report) => verify(report, keyLookup)
      case Failure(e) => InvalidJson(e.getMessage)
    }
  }

  def verify(report: ParticipantReport, keyLookup: Scheme => Option[Key]): VerificationStatus = {
    val result: Either[VerificationStatus, VerificationStatus] = for {
      actual <- report.check.toRight(MissingCheckSection)
      key <- keyLookup(actual.scheme).toRight(UnknownScheme(actual.scheme))
      expected <- generateCheck(report, actual.scheme, key).left.map(CheckGeneration)
    } yield {
      if (actual == expected) Ok else DigestMismatch(expected.digest)
    }
    result.fold[VerificationStatus](identity, identity)
  }

}
