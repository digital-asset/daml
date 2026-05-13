// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

final case class QueryRange[A](startInclusive: A, endInclusive: A) {
  def map[B](f: A => B): QueryRange[B] =
    copy(startInclusive = f(startInclusive), endInclusive = f(endInclusive))
}
