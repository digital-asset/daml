// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import com.digitalasset.ledger.api.health.HealthChecks

trait WriteHealthChecks {
  def healthChecks: HealthChecks = HealthChecks.empty
}
