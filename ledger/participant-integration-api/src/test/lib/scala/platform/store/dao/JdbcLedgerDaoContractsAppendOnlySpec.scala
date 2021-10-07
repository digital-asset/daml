// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{LoneElement, OptionValues}

// These tests use lookups of the contract state at a specific event sequential ID, an operation that
// is not supported by the old mutating schema.
// TODO append-only: Merge this class with JdbcLedgerDaoContractsSpec
private[dao] trait JdbcLedgerDaoContractsAppendOnlySpec extends LoneElement with OptionValues {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

}
