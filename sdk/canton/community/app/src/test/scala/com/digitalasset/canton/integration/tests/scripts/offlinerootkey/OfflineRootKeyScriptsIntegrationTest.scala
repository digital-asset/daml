// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.scripts.offlinerootkey

import better.files.*
import com.digitalasset.canton.config.IdentityConfig.Manual
import com.digitalasset.canton.crypto.{SigningKeySpec, SigningKeyUsage}
import com.digitalasset.canton.integration.plugins.UseH2
import com.digitalasset.canton.integration.tests.scripts.ScriptsIntegrationTest
import com.digitalasset.canton.integration.tests.scripts.offlinerootkey.OfflineRootKeyScriptsIntegrationTest.{
  assemblePath,
  offlineRootKeyPath,
  preparePath,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  IsolatedEnvironments,
}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId.Authorized
import com.google.protobuf.ByteString
import monocle.macros.syntax.lens.*
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.sys.process.Process

object OfflineRootKeyScriptsIntegrationTest {
  lazy val offlineRootKeyPath: File = ScriptsIntegrationTest.examplesPath / "offline-root-key"
  lazy val preparePath: File = offlineRootKeyPath / "prepare-cert.sh"
  lazy val assemblePath: File = offlineRootKeyPath / "assemble-cert.sh"
}

/** Test the offline root namespace key scripts with all supported key specs
  */
trait OfflineRootKeyScriptsIntegrationTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with BeforeAndAfterEach
    with ScalaCheckPropertyChecks {

  registerPlugin(new UseH2(loggerFactory))

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.init.identity).replace(Manual)
        )
      )

  protected def keySpec: SigningKeySpec

  private def rootKeySpecAlgorithm: String = keySpec match {
    case SigningKeySpec.EcCurve25519 => "ed25519"
    case SigningKeySpec.EcP256 | SigningKeySpec.EcSecp256k1 => "ecdsa256"
    case SigningKeySpec.EcP384 => "ecdsa384"
  }

  "offline root key init" should {
    s"work with ${keySpec.name}" in { implicit env =>
      import env.*

      val processLogger = mkProcessLogger()
      val tmpDir = better.files.File.newTemporaryDirectory()
      val outputRootPrefix = (tmpDir / "root").pathAsString
      val outputIntermediatePrefix = (tmpDir / "intermediate").pathAsString

      val offlineRootKey = global_secret.keys.secret.generate_key(keySpec, SigningKeyUsage.All)
      val rootPubKeyPath = (tmpDir / "public_root_key.der")
      rootPubKeyPath.outputStream.apply(os => offlineRootKey.toProtoV30.publicKey.writeTo(os))

      val intermediatePubKeyPath = (tmpDir / "public_intermediate_key.der")
      val intermediateKey = participant1.keys.secret
        .generate_signing_key("IntermediateKey", SigningKeyUsage.NamespaceOnly)

      participant1.keys.public.download_to(intermediateKey.id, intermediatePubKeyPath.pathAsString)

      Process(
        Seq(
          preparePath.pathAsString,
          "--root-delegation",
          "--root-pub-key",
          rootPubKeyPath.pathAsString,
          "--target-pub-key",
          rootPubKeyPath.pathAsString,
          "--output",
          outputRootPrefix,
        ),
        cwd = offlineRootKeyPath.toJava,
      ).!(processLogger) shouldBe 0
      Process(
        Seq(
          preparePath.pathAsString,
          "--intermediate-delegation",
          "--root-pub-key",
          rootPubKeyPath.pathAsString,
          "--canton-target-pub-key",
          intermediatePubKeyPath.pathAsString,
          "--output",
          outputIntermediatePrefix,
        ),
        cwd = offlineRootKeyPath.toJava,
      ).!(processLogger) shouldBe 0

      val rootSignaturePath = File(s"$outputRootPrefix.sig")
      val intermediateSignaturePath = File(s"$outputIntermediatePrefix.sig")

      rootSignaturePath.outputStream.apply(os =>
        global_secret
          .sign(
            ByteString.copyFrom(File(s"$outputRootPrefix.hash").byteArray),
            offlineRootKey.fingerprint,
            SigningKeyUsage.NamespaceOnly,
          )
          .toProtoV30
          .signature
          .writeTo(os)
      )
      intermediateSignaturePath.outputStream.apply(os =>
        global_secret
          .sign(
            ByteString.copyFrom(File(s"$outputIntermediatePrefix.hash").byteArray),
            offlineRootKey.fingerprint,
            SigningKeyUsage.NamespaceOnly,
          )
          .toProtoV30
          .signature
          .writeTo(os)
      )

      Process(
        Seq(
          assemblePath.pathAsString,
          "--prepared-transaction",
          (tmpDir / "root.prep").pathAsString,
          "--signature",
          rootSignaturePath.pathAsString,
          "--signature-algorithm",
          rootKeySpecAlgorithm,
          "--output",
          outputRootPrefix,
        ),
        cwd = offlineRootKeyPath.toJava,
      ).!(processLogger) shouldBe 0
      Process(
        Seq(
          assemblePath.pathAsString,
          "--prepared-transaction",
          (tmpDir / "intermediate.prep").pathAsString,
          "--signature",
          intermediateSignaturePath.pathAsString,
          "--signature-algorithm",
          rootKeySpecAlgorithm,
          "--output",
          outputIntermediatePrefix,
        ),
        cwd = offlineRootKeyPath.toJava,
      ).!(processLogger) shouldBe 0

      participant1.topology.init_id(
        "offlinekey",
        offlineRootKey.fingerprint.toProtoPrimitive,
        delegationFiles = Seq(
          (tmpDir / "root.cert").pathAsString,
          (tmpDir / "intermediate.cert").pathAsString,
        ),
      )

      eventually() {
        participant1.is_initialized shouldBe true
      }

      participant1.topology.namespace_delegations
        .list(
          Authorized,
          filterNamespace = offlineRootKey.fingerprint.toProtoPrimitive,
          filterTargetKey = Some(offlineRootKey.fingerprint.toProtoPrimitive),
        )
        .loneElement
        .item
        .target
        .keySpec shouldBe keySpec
    }
  }
}

class OfflineRootKeyScriptsEd25519IntegrationTest extends OfflineRootKeyScriptsIntegrationTest {
  override protected def keySpec: SigningKeySpec = SigningKeySpec.EcCurve25519
}
class OfflineRootKeyScriptsEcp256IntegrationTest extends OfflineRootKeyScriptsIntegrationTest {
  override protected def keySpec: SigningKeySpec = SigningKeySpec.EcP256
}
class OfflineRootKeyScriptsSecP256k1IntegrationTest extends OfflineRootKeyScriptsIntegrationTest {
  override protected def keySpec: SigningKeySpec = SigningKeySpec.EcSecp256k1
}
class OfflineRootKeyScriptsEcp384IntegrationTest extends OfflineRootKeyScriptsIntegrationTest {
  override protected def keySpec: SigningKeySpec = SigningKeySpec.EcP384
}
