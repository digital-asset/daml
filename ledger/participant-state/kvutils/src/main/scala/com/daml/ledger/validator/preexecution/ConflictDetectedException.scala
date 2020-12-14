// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

final class ConflictDetectedException
    extends RuntimeException(
      "A conflict has been detected with other submissions during post-execution.")
