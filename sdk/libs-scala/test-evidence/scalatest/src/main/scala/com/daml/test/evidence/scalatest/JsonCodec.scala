// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.scalatest

import com.daml.test.evidence.tag._
import io.circe.generic.extras.semiauto.deriveEnumerationCodec
import io.circe.generic.semiauto.deriveCodec
import io.circe.{Codec, Json, Decoder, Encoder}
import cats.syntax.functor._
import io.circe.generic.auto._
import io.circe.syntax._

object JsonCodec {

  object ReliabilityJson {
    import com.daml.test.evidence.tag.Reliability._
    implicit val codecRemediation: Codec[Remediation] = deriveCodec[Remediation]
    implicit val codecComponent: Codec[Component] = deriveCodec[Component]
    implicit val codecAdverseScenario: Codec[AdverseScenario] = deriveCodec[AdverseScenario]
    implicit val encoderReliabilityTestSuite: Encoder[ReliabilityTestSuite] =
      (_: ReliabilityTestSuite) => Json.obj()
  }

  object SecurityJson {
    import com.daml.test.evidence.tag.Security._
    implicit val codecAttack: Codec[Attack] = deriveCodec[Attack]
    implicit val codecHappyCase: Codec[HappyCase] = deriveCodec[HappyCase]
    implicit val propertyCodec: Codec[SecurityTest.Property] =
      deriveEnumerationCodec[SecurityTest.Property]
    implicit val encoderHappyOrAttack: Encoder[HappyOrAttack] =
      Encoder.encodeEither("happy", "attack")
    implicit val decoderHappyOrAttack: Decoder[HappyOrAttack] =
      Decoder.decodeEither("happy", "attack")
    implicit val encoderSecurityTestSuite: Encoder[SecurityTestSuite] =
      (_: SecurityTestSuite) => Json.obj()
  }

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
}
