// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.daml.ledger.api.v2.interactive.interactive_submission_service as iss

object HashingSchemeVersionConverter {
  def toLAPIProto(hashingSchemeVersion: HashingSchemeVersion): iss.HashingSchemeVersion =
    iss.HashingSchemeVersion.fromValue(hashingSchemeVersion.index)
}
