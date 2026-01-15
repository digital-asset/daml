// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode

import com.digitalasset.transcode.schema.DynamicValue

/** Decodes target protocol representation into intermediary DynamicValue representation. */
@FunctionalInterface
trait Decoder[A] {
  def toDynamicValue(v: A): DynamicValue
}
