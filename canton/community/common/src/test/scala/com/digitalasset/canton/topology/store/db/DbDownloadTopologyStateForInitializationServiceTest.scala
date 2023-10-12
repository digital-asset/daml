// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import com.digitalasset.canton.store.db.{H2Test, MigrationMode, PostgresTest}
import com.digitalasset.canton.topology.store.DownloadTopologyStateForInitializationServiceTest

class DownloadTopologyStateForInitializationServiceTestPostgres
    extends DownloadTopologyStateForInitializationServiceTest
    with DbTopologyStoreXHelper
    with PostgresTest {

  // TODO(#12373) remove this from unstable/dev when releasing BFT
  override val migrationMode: MigrationMode = MigrationMode.DevVersion
}

class DownloadTopologyStateForInitializationServiceTestH2
    extends DownloadTopologyStateForInitializationServiceTest
    with DbTopologyStoreXHelper
    with H2Test {

  // TODO(#12373) remove this from unstable/dev when releasing BFT
  override val migrationMode: MigrationMode = MigrationMode.DevVersion
}
