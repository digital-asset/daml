// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission.foo

import com.daml.ledger.client.binding.Primitive

case class PartiesSelection(
    observers: List[Primitive.Party],
    divulgees: List[Primitive.Party],
)
