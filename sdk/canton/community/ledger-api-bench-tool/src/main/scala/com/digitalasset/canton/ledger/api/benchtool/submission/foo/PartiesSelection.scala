// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission.foo

import com.daml.ledger.javaapi.data.Party

final case class PartiesSelection(
    observers: List[Party],
    divulgees: List[Party],
)
