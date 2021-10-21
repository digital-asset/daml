// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

// Aggregate all specs in a single run to not start a new database fixture for each one
final class JdbcLedgerDaoValidatedPostgresqlSpec
    extends AsyncFlatSpec
    with Matchers
    with JdbcLedgerDaoSuite
    with JdbcLedgerDaoBackendPostgresql
    with JdbcLedgerDaoPostCommitValidationSpec
    with JdbcAppendOnlyTransactionInsertion
