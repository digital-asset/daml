// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

final class IndexerStabilityH2Spec extends IndexerStabilitySpec {

  override def jdbcUrl: String = "jdbc:h2:mem:indexer_stability_spec;db_close_delay=-1"
  override def haModeSupported: Boolean = false
}
