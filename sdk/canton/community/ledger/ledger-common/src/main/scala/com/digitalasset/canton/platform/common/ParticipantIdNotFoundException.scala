// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.common

class ParticipantIdNotFoundException(attempts: Int)
    extends RuntimeException(
      s"""No participant ID found in the index database after $attempts attempts."""
    )
