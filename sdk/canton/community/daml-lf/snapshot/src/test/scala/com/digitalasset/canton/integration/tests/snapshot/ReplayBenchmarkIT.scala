// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.snapshot

import com.digitalasset.daml.lf.testing.snapshot.ReplayBenchmarkITBase
import com.digitalasset.daml.lf.value.ContractIdVersion

// TODO(#23971) This will likely break when Canton starts to produce V2 contract IDs.
// Ping Andreas Lochbihler on the #team-daml-language slack channel when this happens.
class ReplayBenchmarkIT_V1 extends ReplayBenchmarkITBase(ContractIdVersion.V1)
