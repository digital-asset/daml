// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

final class OraclePruningTest extends AbstractPruningTest with HttpServiceOracleInt {
  override def disableContractPayloadIndexing = false
}
