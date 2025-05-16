// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples

import better.files.File
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.integration.CommunityIntegrationTest
import com.digitalasset.canton.integration.plugins.UseH2
import com.digitalasset.canton.integration.tests.examples.ExampleIntegrationTest.examplesPath
import com.digitalasset.canton.integration.tests.examples.OpenSslOfflineRootKeyDemoExampleIntegrationTest.demoFolder
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId.Authorized
import com.digitalasset.canton.topology.transaction.{DelegationRestriction, TopologyMapping}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

object OpenSslOfflineRootKeyDemoExampleIntegrationTest {
  lazy val demoFolder: File = examplesPath / "10-offline-root-namespace-init"
}

sealed abstract class OpenSslOfflineRootKeyDemoExampleIntegrationTest
    extends ExampleIntegrationTest(
      demoFolder / "external-init-example.conf"
    )
    with CommunityIntegrationTest
    with ScalaCheckPropertyChecks {

  override def afterAll(): Unit = {
    super.afterAll()
    // Delete the temp files created by the test
    (demoFolder / "tmp").delete(swallowIOExceptions = true)
  }

  private def delegationRestrictions(
      fingerprint: Fingerprint
  )(implicit env: FixtureParam) = {
    import env.*
    participant1.topology.namespace_delegations
      .list(
        store = Authorized,
        filterTargetKey = Some(fingerprint),
      )
      .loneElement
      .item
      .restriction
  }

  "run offline root namespace key init demo" in { implicit env =>
    import env.*

    val tmpDir = better.files.File.newTemporaryDirectory("tmp")
    ExampleIntegrationTest.ensureSystemProperties(
      "canton-examples.openssl-script-dir" -> demoFolder.pathAsString
    )
    ExampleIntegrationTest.ensureSystemProperties(
      "canton-examples.openssl-keys-dir" -> tmpDir.pathAsString
    )
    runScript(demoFolder / "bootstrap.canton")(environment)
    participant1.is_initialized shouldBe true

    // Check root key restrictions
    delegationRestrictions(
      participant1.id.fingerprint
    ) shouldBe DelegationRestriction.CanSignAllMappings

    val namespaceDelegationFingerprint = participant1.keys.public
      .list()
      .find(_.name.map(_.unwrap).contains("IntermediateKey"))
      .value
      .id

    // Check intermediate key restrictions
    delegationRestrictions(
      namespaceDelegationFingerprint
    ) shouldBe DelegationRestriction.CanSignAllButNamespaceDelegations

    // Run the script adding a key with signing restrictions
    runScript(demoFolder / "restricted-key.canton")(environment)

    // Check restricted key restrictions
    delegationRestrictions(
      participant1.keys.public
        .list()
        .find(_.name.map(_.unwrap).contains("RestrictedKey"))
        .value
        .id
    ) shouldBe DelegationRestriction.CanSignSpecificMappings(
      NonEmpty.mk(
        Set,
        TopologyMapping.Code.PartyToParticipant,
        TopologyMapping.Code.PartyToKeyMapping,
      )
    )

    // Run the script revoking the delegation
    runScript(demoFolder / "revoke-namespace-delegation.canton")(environment)

    // Check the delegation is gone
    participant1.topology.namespace_delegations
      .list(
        store = Authorized,
        filterTargetKey = Some(namespaceDelegationFingerprint),
      ) shouldBe empty
  }
}

final class OpenSslOfflineRootKeyDemoExampleIntegrationTestH2
    extends OpenSslOfflineRootKeyDemoExampleIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
}
