// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.ha

import com.digitalasset.canton.platform.store.testing.oracle.OracleAroundAll

final class IndexerStabilitySpecOracle extends IndexerStabilitySpec with OracleAroundAll
