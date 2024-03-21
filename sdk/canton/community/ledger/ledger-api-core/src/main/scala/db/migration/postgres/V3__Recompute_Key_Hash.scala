// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Note: package name must correspond exactly to the flyway 'locations' setting, which defaults to
// 'db.migration.postgres' for postgres migrations
package com.digitalasset.canton.platform.db.migration.postgres

import com.digitalasset.canton.platform.db.migration.EmptyJavaMigration

/** This migration has been effectively deleted by replacing it with a no-op migration */
private[migration] class V3__Recompute_Key_Hash extends EmptyJavaMigration
