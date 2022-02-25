package com.daml.ledger.security.test
// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import better.files.File
import com.daml.ledger.security.test.SystematicTesting.Operability.OperabilityTest
import io.circe.generic.extras.semiauto.deriveEnumerationCodec
import io.circe.generic.semiauto.{deriveCodec, deriveDecoder, deriveEncoder}
import io.circe.{Codec, Decoder, Encoder, Json}
import org.scalactic.source
import org.scalatest.Tag
import org.scalatest.wordspec.FixtureAnyWordSpec

object SystematicTesting {

  sealed abstract class TestTag extends Tag("SystematicTesting") {
    override val name: String = TestTag.encoder(this).noSpaces
  }

  object TestTag {
    implicit val encoder: Encoder[TestTag] = deriveEncoder[TestTag]
    implicit val decoder: Decoder[TestTag] = deriveDecoder[TestTag]
  }

  /** A functional test case.
    *
    * Captures a test for a functional requirement in addition to the non-functional requirements such as security or
    * reliability.
    *
    * TODO(soren): Refine data captured in a functional test case tag.
    *
    * @param topics high-level topics such as key requirements (e.g. no double spends, party migration)
    *               This can be officially stated key requirements or other important topics.
    * @param features features tested (e.g. roll sequencer keys, repair.add)
    */
  final case class FuncTest(topics: Seq[String], features: Seq[String]) extends TestTag

  /** A tag for a missing test case.
    *
    * Similar to a to-do entry for test coverage but can be included in the inventory.
    * Alternatively one can use `ignore` with the appropriate tag already applied.
    */
  final case class MissingTest(missingCoverage: String) extends TestTag

  /** Convert a multi-line string into a single-line one */
  private def singleLine(multiLine: String): String =
    multiLine.linesIterator.map(_.trim).mkString(" ")

  object Security {

    /** Security-relevant information on a test-suite level. */
    trait SecurityTestSuite {

      /** The layer that the security test suite tests, such as on the network/API level or the ledger model. */
      protected def securityTestLayer: SecurityTestLayer

      // TODO(soren): Include security-relevant configuration that is tested from the environment
    }

    object SecurityTestSuite {
      implicit val encoder: Encoder[SecurityTestSuite] =
        Encoder.forProduct1("layer")(ts => ts.securityTestLayer)
    }

    sealed trait SecurityTestLayer
    object SecurityTestLayer {

      /** Security tests that covers attacks on the network transport and API level.
        * I.e., very generic attacks such as those from OWASP top ten.
        *
        * Example: a ledger api client with TLS enabled when presented an untrusted server certificate
        * must refuse to submit ledger api commands.
        */
      case object Network extends SecurityTestLayer

      /** Security tests that cover the Daml Ledger model properties
        *
        * Example: if a ledger api user tries to use an inactive contract, the ledger api must reject the command.
        */
      case object LedgerModel extends SecurityTestLayer

      /** Security test that cover Canton specific security properties.
        * I.e., properties that are not covered by the ledger model, but too specific for the Network category.
        *
        * Example: if a participant operator tries to remove a vetted package, the participant must reject the command.
        */
      case object KeyRequirements extends SecurityTestLayer

    }

    implicit val layerCodec: Codec[SecurityTestLayer] = deriveEnumerationCodec[SecurityTestLayer]

    type HappyOrAttack = Either[HappyCase, Attack]
    implicit val encoderHappyOrAttack: Encoder[HappyOrAttack] =
      Encoder.encodeEither("happy", "attack")
    implicit val decoderHappyOrAttack: Decoder[HappyOrAttack] =
      Decoder.decodeEither("happy", "attack")

    /** A security-relevant test
      *
      * @param property The security property that is tested
      * @param asset The asset that needs to be protected. For API-level security tests this should capture the node + interface.
      * @param scenario The happy case or a particular attack
      * @param unimplemented indicates whether the test case has not yet been implemented
      * @param file The filename that contains the test
      * @param line The line number of the test
      *
      * TODO(soren): Consider to refine the `asset` to capture also interface information for API-level security tests.
      */
    final case class SecurityTest(
        property: SecurityTest.Property,
        asset: String,
        scenario: Option[HappyOrAttack],
        unimplemented: Boolean,
        file: String,
        line: Int,
    ) extends TestTag {

      def setAttack(
          attack: Attack
      )(implicit lineNo: sourcecode.Line, fileName: sourcecode.File): SecurityTest =
        SecurityTest(property, asset, attack)

      def setHappyCase(
          happyCase: String
      )(implicit lineNo: sourcecode.Line, fileName: sourcecode.File): SecurityTest =
        SecurityTest(property, asset, happyCase)

      def toBeImplemented: SecurityTest = copy(unimplemented = true)
    }

    object SecurityTest {
      sealed trait Property extends Product with Serializable
      object Property {

        /** Privacy of an asset. We use privacy in a broad sense and also include data confidentiality here. */
        case object Privacy extends Property

        case object Integrity extends Property

        /** Availability of an asset, primarily with regard to denial of service attacks. */
        case object Availability extends Property

        /** A request leads to a definite response within a defined period of time.
          */
        case object Finality extends Property

        case object Authenticity extends Property

        case object Authorization extends Property

        /** Whether a secure configuration can effectively be enforced */
        case object SecureConfiguration extends Property
      }

      implicit val propertyCodec: Codec[Property] = deriveEnumerationCodec[Property]

      private def apply(
          property: Property,
          asset: String,
          scenario: Option[HappyOrAttack],
      )(implicit
          lineNo: sourcecode.Line,
          fileName: sourcecode.File,
      ): SecurityTest = {
        val relPath = File.currentWorkingDirectory.relativize(File(fileName.value))
        new SecurityTest(
          property = property,
          asset = asset,
          scenario = scenario,
          unimplemented = false,
          file = relPath.toString,
          line = lineNo.value,
        )
      }

      def apply(property: Property, asset: String)(implicit
          lineNo: sourcecode.Line,
          fileName: sourcecode.File,
      ): SecurityTest =
        apply(property = property, asset = asset, None)

      def apply(property: Property, asset: String, happyCase: String)(implicit
          lineNo: sourcecode.Line,
          fileName: sourcecode.File,
      ): SecurityTest =
        apply(property, asset, Some(Left(HappyCase(happyCase))))

      def apply(property: Property, asset: String, attack: Attack)(implicit
          lineNo: sourcecode.Line,
          fileName: sourcecode.File,
      ): SecurityTest =
        apply(property, asset, Some(Right(attack)))
    }

    /** An attack to compromise the security property of an asset and how the system mitigates such an attack.
      *
      * @param actor who executes the attack
      * @param threat what the actor does
      * @param mitigation how the system prevents success of the attack
      */
    sealed abstract case class Attack(actor: String, threat: String, mitigation: String)

    object Attack {
      implicit val codec: Codec[Attack] = deriveCodec[Attack]
      def apply(actor: String, threat: String, mitigation: String): Attack =
        new Attack(
          actor = singleLine(actor),
          threat = singleLine(threat),
          mitigation = singleLine(mitigation),
        ) {}
    }

    object CommonAttacks {

      /** An active attacker on the network tries to impersonate a server.
        * Uses TLS and verification of the server certificate against a trusted root CA to authenticate the server.
        */
      def impersonateServerWithTls(nodeName: String): Attack =
        Attack(
          actor = "Active attacker on the network",
          threat = s"Impersonate the $nodeName node",
          mitigation = "Use and verify a TLS server certificate against a trusted root CA",
        )

      def impersonateClientWithTls(apiName: String): Attack =
        Attack(
          actor = s"Network participant that can reach the $apiName",
          threat = s"Impersonate a $apiName client",
          mitigation = "Use and verify a TLS client certificate against a trusted root CA",
        )

    }

    /** A security test should also cover the happy case when no attack is present to ensure functional correctness */
    sealed abstract case class HappyCase(description: String)

    object HappyCase {
      implicit val codec: Codec[HappyCase] = deriveCodec[HappyCase]
      def apply(description: String): HappyCase = new HappyCase(
        description = singleLine(description)
      ) {}
    }

  }

  object Reliability {

    // TODO(soren): Capture test-suite level data for reliability tests
    trait ReliabilityTestSuite

    object ReliabilityTestSuite {
      implicit val encoder: Encoder[ReliabilityTestSuite] = (_: ReliabilityTestSuite) => Json.obj()
    }

    /** A reliability relevant test.
      *
      * Generally, for a given failure or degradation of a component, the next dependent of the component which can remediate the failure
      * should be the component that should be tested/annotated to remain available. If the failing component 'itself'
      * can remediate the failure (e.g. when the active mediator crashes, the other passive replicated mediators will
      * remediate by becoming active), then the next dependent should be annotated as the component to remain available.
      *
      * For example, if the sequencer-database connection is blocked, then the component that should remain available
      * is the sequencer as next dependent. It should, e.g., not be the participant connected to the sequencer (which also wouldn't be able to
      * process transactions over that sequencer anymore) because the participant is not the next dependent.
      *
      * @param component the component that should remain available, continue processing or recover
      * @param dependencyFailure the failure scenario of a dependency and its remediation
      * @param remediation how the system remediates the adverse scenario
      * @param outcome in what way the component is still available, can continue operating or recovers
      */
    final case class ReliabilityTest(
        component: Component,
        dependencyFailure: AdverseScenario,
        remediation: Remediation,
        outcome: String,
    ) extends TestTag

    /** @param remediator the component carrying out the remediation
      *  @param action what steps are taken for the remediation
      */
    case class Remediation(remediator: String, action: String)
    object Remediation {
      implicit val codec: Codec[Remediation] = deriveCodec[Remediation]
    }

    /** @param name name of the component that must remain available, continue processing or recover
      * @param setting whether the component is replicated, embedded etc.
      */
    case class Component(name: String, setting: String)
    object Component {
      implicit val codec: Codec[Component] = deriveCodec[Component]
    }

    /** Description of a scenario where the dependency of a component is adversely impacted.
      * This can be any sort of (usually network) failure or degradation that could adversely affect the reliability
      * of the component and requires logic within Canton (e.g. retries) to stay reliable. It should not be security-
      * relevant scenarios.
      * This should also not be a scenario that essentially only tests the underlying network stack (e.g. slow bandwidth)
      * and doesn't test anything Canton-specific (i.e. where we don't do a specific remediation except correctly handling
      * the possible errors of the underlying network stack).
      *
      * @param dependency what dependency is affected
      * @param details how the dependency is adversely affected
      */
    case class AdverseScenario(dependency: String, details: String)
    object AdverseScenario {
      implicit val codec: Codec[AdverseScenario] = deriveCodec[AdverseScenario]
    }

  }

  object Operability {

    trait OperabilityTestSuite

    object OperabilityTestSuite {
      implicit val encoder: Encoder[OperabilityTestSuite] = (_: OperabilityTestSuite) => Json.obj()
    }

    final case class OperabilityTest(
        component: String,
        dependency: String,
        setting: String,
        cause: String,
        remediation: String,
    ) extends TestTag

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
  )(remediation: String): ResultOfTaggedAsInvocationOnString =
    new WordSpecStringWrapper(remediation)
      .taggedAs(
        OperabilityTest(
          component = component,
          dependency = dependency,
          setting = setting,
          cause = cause,
          remediation = remediation,
        )
      )

}
