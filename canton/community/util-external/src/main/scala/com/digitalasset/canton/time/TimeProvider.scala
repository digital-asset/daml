// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

trait TimeProvider {

  /** Potentially non-monotonic time provider
    */
  def nowInMicrosecondsSinceEpoch: Long

}
