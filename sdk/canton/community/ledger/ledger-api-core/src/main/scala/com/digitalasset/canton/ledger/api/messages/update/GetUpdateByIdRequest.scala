// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.update

import com.digitalasset.canton.ledger.api.{UpdateFormat, UpdateId}

final case class GetUpdateByIdRequest(
    updateId: UpdateId,
    updateFormat: UpdateFormat,
)
