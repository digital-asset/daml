// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms.aws

import com.digitalasset.canton.integration.plugins.{UseAwsKms, UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.security.kms.MigrationClearToEncryptedStoreIntegrationTest

/** Tests a migration from a clear crypto private store to an encrypted private store. It requires a
  * node to restart and to set-up an encrypted private store and AWS KMS in the config files.
  */
class AwsMigrationClearToEncryptedStoreBftOrderingIntegrationTestPostgres
    extends MigrationClearToEncryptedStoreIntegrationTest {

  override protected val kmsPlugin = new UseAwsKms(
    nodes = protectedNodes,
    timeouts = timeouts,
    loggerFactory = loggerFactory,
  )

  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UsePostgres(loggerFactory))

}
