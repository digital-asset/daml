// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.caching.SizedCache
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.ContractId
import com.daml.lf.data.Ref.Party
import com.daml.metrics.Metrics

import scala.concurrent.ExecutionContext

object KeyStateCache {
  def apply(cacheSize: Long, metrics: Metrics)(implicit
      ec: ExecutionContext
  ): StateCache[GlobalKey, KeyStateValue] =
    StateCache(
      cache = SizedCache.from[GlobalKey, KeyStateValue](
        SizedCache.Configuration(cacheSize),
        metrics.daml.execution.keyStateCache,
      ),
      metrics = metrics,
    )

  sealed trait KeyStateValue extends Product with Serializable

  object KeyStateValue {

    final case class Assigned(contractId: ContractId, createWitnesses: Set[Party])
        extends KeyStateValue

    final case object Unassigned extends KeyStateValue
  }
}
