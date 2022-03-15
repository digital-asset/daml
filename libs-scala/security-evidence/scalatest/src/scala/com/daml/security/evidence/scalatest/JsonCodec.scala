// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.security.evidence.scalatest

import com.daml.security.evidence.tag.Reliability.{AdverseScenario, Component, Remediation}
import com.daml.security.evidence.tag.Security.{Attack, HappyCase}
import com.daml.security.evidence.tag._
import io.circe.generic.extras.semiauto.deriveEnumerationCodec
import io.circe.generic.semiauto.deriveCodec
import io.circe.{Codec, Json}

object JsonCodec {
  import cats.syntax.functor._
  import io.circe.generic.auto._
  import io.circe.syntax._
  import io.circe.{Decoder, Encoder}

  implicit val encodeEvidenceTag: Encoder[EvidenceTag] = Encoder.instance {
    case e: Operability.OperabilityTest => e.asJson
    case e: Reliability.ReliabilityTest => e.asJson
    case e: Security.SecurityTest => e.asJson
    case e: MissingTest => e.asJson
    case e: FuncTest => e.asJson
  }

  implicit val decodeEvidenceTag: Decoder[EvidenceTag] =
    List[Decoder[EvidenceTag]](
      Decoder[Operability.OperabilityTest].widen,
      Decoder[Reliability.ReliabilityTest].widen,
      Decoder[Security.SecurityTest].widen,
      Decoder[MissingTest].widen,
      Decoder[FuncTest].widen,
    ).reduceLeft(_ or _)

  implicit val attackcodec: Codec[Attack] = deriveCodec[Attack]
  implicit val happycasecodec: Codec[HappyCase] = deriveCodec[HappyCase]
  implicit val remediationcodec: Codec[Remediation] = deriveCodec[Remediation]
  implicit val componentoncodec: Codec[Component] = deriveCodec[Component]
  implicit val AdverseScenariocodec: Codec[AdverseScenario] = deriveCodec[AdverseScenario]
  implicit val propertyCodec: Codec[com.daml.security.evidence.tag.Security.SecurityTest.Property] =
    deriveEnumerationCodec[com.daml.security.evidence.tag.Security.SecurityTest.Property]
  implicit val encoderHappyOrAttack
      : Encoder[com.daml.security.evidence.tag.Security.HappyOrAttack] =
    Encoder.encodeEither("happy", "attack")
  implicit val decoderHappyOrAttack
      : Decoder[com.daml.security.evidence.tag.Security.HappyOrAttack] =
    Decoder.decodeEither("happy", "attack")
  implicit val layerCodec: Codec[com.daml.security.evidence.tag.Security.SecurityTestLayer] =
    deriveEnumerationCodec[com.daml.security.evidence.tag.Security.SecurityTestLayer]
  implicit val suiteEncoder: Encoder[com.daml.security.evidence.tag.Security.SecurityTestSuite] =
    Encoder.forProduct1("layer")(ts => ts.securityTestLayer)
  implicit val encoderRe: Encoder[com.daml.security.evidence.tag.Reliability.ReliabilityTestSuite] =
    (_: com.daml.security.evidence.tag.Reliability.ReliabilityTestSuite) => Json.obj()
}
