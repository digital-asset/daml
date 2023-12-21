// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import com.digitalasset.canton.store.db.{H2Test, PostgresTest}
import com.digitalasset.canton.topology.store.DownloadTopologyStateForInitializationServiceTest

class DownloadTopologyStateForInitializationServiceTestPostgres
    extends DownloadTopologyStateForInitializationServiceTest
    with DbTopologyStoreXHelper
    with PostgresTest

class DownloadTopologyStateForInitializationServiceTestH2
    extends DownloadTopologyStateForInitializationServiceTest
    with DbTopologyStoreXHelper
    with H2Test
