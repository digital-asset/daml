// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script

import com.digitalasset.daml.lf.data.Ref.Location
import com.digitalasset.daml.lf.transaction.IncompleteTransaction

final case class CurrentSubmission(
    commitLocation: Option[Location],
    ptx: IncompleteTransaction,
)
