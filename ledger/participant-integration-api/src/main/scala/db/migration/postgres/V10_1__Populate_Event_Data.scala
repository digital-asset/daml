// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Note: package name must correspond exactly to the flyway 'locations' setting, which defaults to
// 'db.migration.postgres' for postgres migrations
package com.daml.platform.db.migration.postgres

import com.daml.platform.db.migration.EmptyJavaMigration

/** This migration has been effectively deleted by replacing it with a no-op migration */
private[migration] class V10_1__Populate_Event_Data extends EmptyJavaMigration
