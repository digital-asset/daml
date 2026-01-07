// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms

import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.crypto.{SigningKeyUsage, SigningPublicKey}
import com.digitalasset.canton.integration.plugins.UseBftSequencer
import com.digitalasset.canton.integration.tests.security.KeyManagementIntegrationTestHelper
import com.digitalasset.canton.integration.tests.security.kms.aws.AwsKmsCryptoIntegrationTestBase
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentSetupPlugin,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllButNamespaceDelegations

import java.util.concurrent.atomic.AtomicInteger

trait NamespaceIntermediateKmsKeyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with KmsCryptoIntegrationTestBase
    with KeyManagementIntegrationTestHelper {

  protected val namespaceKeys: Array[String]
  protected val rotationKey: String

  private def setupIntermediateKey(
      counter: AtomicInteger
  )(node: InstanceReference): SigningPublicKey = {

    val namespaceDelegations =
      node.topology.namespace_delegations.list(
        store = TopologyStoreId.Authorized,
        filterNamespace = node.namespace.toProtoPrimitive,
      )

    // create a new namespace intermediate key
    val intermediateKey =
      node.keys.secret
        .register_kms_signing_key(
          namespaceKeys(counter.getAndIncrement()),
          SigningKeyUsage.NamespaceOnly,
        )

    // Create a namespace delegation for the intermediate key with the namespace root key
    node.topology.namespace_delegations.propose_delegation(
      node.namespace,
      intermediateKey,
      CanSignAllButNamespaceDelegations,
    )

    // Check that the new namespace delegations appears
    eventually() {
      val updatedNamespaceDelegations = node.topology.namespace_delegations.list(
        store = TopologyStoreId.Authorized,
        filterNamespace = node.namespace.toProtoPrimitive,
      )
      assertResult(1, updatedNamespaceDelegations)(
        updatedNamespaceDelegations.length - namespaceDelegations.length
      )
    }

    intermediateKey
  }

  "create and rotate intermediate keys for participant" in { implicit env =>
    import env.*

    rotateIntermediateNamespaceKeyAndPing(
      participant1,
      Some(rotationKey),
      setupIntermediateKey(new AtomicInteger(0)),
    )
  }

  setupPlugins(
    withAutoInit = false,
    storagePlugin = Option.empty[EnvironmentSetupPlugin],
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )
}

class NamespaceIntermediateAwsKmsKeyReferenceIntegrationTest
    extends NamespaceIntermediateKmsKeyIntegrationTest
    with AwsKmsCryptoIntegrationTestBase {
  override protected val namespaceKeys: Array[String] = Array(
    "alias/canton-kms-test-namespace-intermediate-signing-key-participant",
    "alias/canton-kms-test-namespace-intermediate-signing-key-2-participant",
  )

  override protected val rotationKey: String = "alias/canton-kms-test-signing-key"
}
