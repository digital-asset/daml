package com.daml.ledger.security.test
// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import com.daml.security.evidence.tag.Reliability.{AdverseScenario, Component, Remediation}
import com.daml.security.evidence.tag.Security.{Attack, HappyCase}
import com.daml.security.evidence.tag._
import io.circe.generic.extras.semiauto.deriveEnumerationCodec
import io.circe.generic.semiauto.deriveCodec
import io.circe.{Codec, Json}
import org.scalactic.source
import org.scalatest.Tag
import org.scalatest.wordspec.FixtureAnyWordSpec

import scala.language.implicitConversions

object SystematicTesting {
  implicit def tagToContainer(tag: com.daml.security.evidence.tag.EvidenceTag): Tag =
    new TagContainer(tag)

  class TagContainer(testTag: com.daml.security.evidence.tag.EvidenceTag)
      extends Tag("SystematicTesting") {
    override val name: String = SystematicTesting.JsonCodec.encodeEvidenceTag(testTag).noSpaces
  }

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
    implicit val propertyCodec
        : Codec[com.daml.security.evidence.tag.Security.SecurityTest.Property] =
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
    implicit val encoderRe
        : Encoder[com.daml.security.evidence.tag.Reliability.ReliabilityTestSuite] =
      (_: com.daml.security.evidence.tag.Reliability.ReliabilityTestSuite) => Json.obj()
  }

}

/** Provides alternative versions `when_`, `can_`, `should_`, `must_` of `when`, `can`, `should`, `must` that expose the description through a variable. I.e.:
  * <pre>
  * "my condition" when_ { condition =>
  *   ...
  * }
  * </pre>
  *
  * This is helpful to avoid duplicating test descriptions when annotating reliability/security/operability tests.
  */
trait AccessTestScenario extends FixtureAnyWordSpec {
  implicit class ScenarioWrapper(str: String) {

    def when_(f: String => Unit)(implicit pos: source.Position): Unit = {
      new WordSpecStringWrapper(str).when(f(str))
    }

    def can_(f: String => Unit)(implicit pos: source.Position): Unit = {
      convertToStringCanWrapper(str).can(f(str))
    }

    def should_(f: String => Unit)(implicit pos: source.Position): Unit = {
      import org.scalatest.matchers.should.Matchers._
      convertToStringShouldWrapper(str).should(f(str))
    }

    def must_(f: String => Unit)(implicit pos: source.Position): Unit = {
      convertToStringMustWrapperForVerb(str).must(f(str))
    }

    def taggedAs_(f: String => Tag): ResultOfTaggedAsInvocationOnString = {
      convertToWordSpecStringWrapper(str).taggedAs(f(str))
    }
  }
}

trait OperabilityTestHelpers extends AccessTestScenario {

  def operabilityTest(component: String)(dependency: String)(setting: String)(
      cause: String
  )(remediation: String): ResultOfTaggedAsInvocationOnString = {
    import SystematicTesting._
    new WordSpecStringWrapper(remediation)
      .taggedAs(
        com.daml.security.evidence.tag.Operability.OperabilityTest(
          component = component,
          dependency = dependency,
          setting = setting,
          cause = cause,
          remediation = remediation,
        )
      )
  }

}
