// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.tag

import better.files.File

object Security {

  /** Security-relevant information on a test-suite level. */
  trait SecurityTestSuite {

    /** The layer that the security test suite tests, such as on the network/API level or the ledger model. */
    // TODO test evidencing: This field may need to be revised. Usually a suite mixes test cases from different layers.
    //  So it gets hard to choose a single layer that describes all tests.
    def securityTestLayer: SecurityTestLayer

    // TODO test evidencing: Include security-relevant configuration that is tested from the environment
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

  type HappyOrAttack = Either[HappyCase, Attack]

  /** A security-relevant test
    *
    * @param property The security property that is tested
    * @param asset The asset that needs to be protected. For API-level security tests this should capture the node + interface.
    * @param scenario The happy case or a particular attack
    * @param unimplemented indicates whether the test case has not yet been implemented
    * @param file The filename that contains the test
    * @param line The line number of the test
    *
    * TODO test evidencing: Consider to refine the `asset` to capture also interface information for API-level security tests.
    */
  final case class SecurityTest(
      property: SecurityTest.Property,
      asset: String,
      scenario: Option[HappyOrAttack],
      unimplemented: Boolean,
      file: String,
      line: Int,
  ) extends EvidenceTag {

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

      case object Confidentiality extends Property

      case object Authentication extends Property

      case object NonRepudiation extends Property

      case object Resilience extends Property
    }

    private def apply(
        property: Property,
        asset: String,
        scenario: Option[HappyOrAttack],
    )(implicit
        lineNo: sourcecode.Line,
        fileName: sourcecode.File,
    ): SecurityTest = {
      val relPath = File.currentWorkingDirectory.relativize(File(fileName.value))
      SecurityTest(
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
    def apply(actor: String, threat: String, mitigation: String): Attack =
      new Attack(
        actor = EvidenceTag.singleLine(actor),
        threat = EvidenceTag.singleLine(threat),
        mitigation = EvidenceTag.singleLine(mitigation),
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
    def apply(description: String): HappyCase = new HappyCase(
      description = EvidenceTag.singleLine(description)
    ) {}
  }

}
