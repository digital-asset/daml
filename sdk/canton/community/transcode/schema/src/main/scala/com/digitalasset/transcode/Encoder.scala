// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode

import com.digitalasset.transcode.schema.DynamicValue

/** Encodes intermediary DynamicValue representation into target protocol representation. */
@FunctionalInterface
trait Encoder[A] {
  def fromDynamicValue(dv: DynamicValue): A
}
