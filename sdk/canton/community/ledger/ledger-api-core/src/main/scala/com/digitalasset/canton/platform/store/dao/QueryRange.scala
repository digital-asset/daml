// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

final case class QueryRange[A](startExclusive: A, endInclusive: A) {
  def map[B](f: A => B): QueryRange[B] =
    copy(startExclusive = f(startExclusive), endInclusive = f(endInclusive))
}
