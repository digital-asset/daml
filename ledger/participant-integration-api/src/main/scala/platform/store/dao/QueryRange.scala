// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

final case class QueryRange[A](startExclusive: A, endInclusive: A) {
  def map[B](f: A => B): QueryRange[B] =
    copy(startExclusive = f(startExclusive), endInclusive = f(endInclusive))
}
