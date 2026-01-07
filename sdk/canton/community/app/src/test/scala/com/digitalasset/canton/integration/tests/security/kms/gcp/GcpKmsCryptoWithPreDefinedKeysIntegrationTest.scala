// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms.gcp

import com.digitalasset.canton.integration.EnvironmentDefinition.allNodeNames
import com.digitalasset.canton.integration.EnvironmentSetupPlugin
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.security.CryptoIntegrationTest
import com.digitalasset.canton.integration.tests.security.kms.KmsCryptoWithPreDefinedKeysIntegrationTest

/** Runs a crypto integration tests with ALL nodes (except participant3 that is not involved in the
  * tests) using a GCP KMS provider and pre-generated keys. Consequently, keys must be registered in
  * Canton and nodes MUST be manually initialized.
  */
class GcpKmsCryptoWithPreDefinedKeysBftOrderingIntegrationTestAllNodes
    extends CryptoIntegrationTest(
      GcpKmsCryptoIntegrationTestBase.defaultGcpKmsCryptoConfig
    )
    with GcpKmsCryptoIntegrationTestBase {

  // all nodes are protected using KMS as a provider
  override lazy protected val protectedNodes: Set[String] = allNodeNames(
    environmentDefinition.baseConfig
  )

  setupPlugins(
    withAutoInit = false,
    storagePlugin = Option.empty[EnvironmentSetupPlugin],
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )
}

/** Runs a crypto integration tests with one participant using a GCP KMS provider with pre-generated
  * keys. Runs with persistence so we also check that it is able to recover from an unexpected
  * shutdown.
  */
class GcpKmsCryptoWithPreDefinedKeysBftOrderingIntegrationTestPostgres
    extends CryptoIntegrationTest(
      GcpKmsCryptoIntegrationTestBase.defaultGcpKmsCryptoConfig
    )
    with GcpKmsCryptoIntegrationTestBase
    with KmsCryptoWithPreDefinedKeysIntegrationTest {
  setupPlugins(
    withAutoInit = false,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )
}
