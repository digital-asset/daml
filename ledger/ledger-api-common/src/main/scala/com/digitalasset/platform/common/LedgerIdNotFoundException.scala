// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.common

class LedgerIdNotFoundException(attempts: Int)
    extends RuntimeException(
      s"""No ledger ID found in the index database after $attempts attempts."""
    )
