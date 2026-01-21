// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.google.common.annotations.VisibleForTesting

/** Wrapper around the lower bound. Since changing the value should only be done in tests, we use a
  * var instead of an atomic reference.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
final case class SequencingTimeBound(private var t: Option[CantonTimestamp]) {
  @inline def get: Option[CantonTimestamp] = t

  @VisibleForTesting def set(newValue: Option[CantonTimestamp]): Unit = t = newValue
}
