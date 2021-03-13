package com.daml.platform.store.state

import com.daml.caching.{Cache, SizedCache}
import com.daml.lf.data.Ref.Party
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.ContractId
import com.daml.metrics.Metrics
import com.daml.platform.store.state.ContractsKeyCache.KeyStateUpdate

import scala.concurrent.ExecutionContext

case class ContractsKeyCache(cache: Cache[GlobalKey, KeyStateUpdate])(implicit
    protected val ec: ExecutionContext
) extends StateCache[GlobalKey, KeyStateUpdate]

object ContractsKeyCache {
  sealed trait KeyStateUpdate extends Product with Serializable
  final case class Assigned(assignedAt: Long, contractId: ContractId, createWitnesses: Set[Party])
      extends KeyStateUpdate
  final case object Unassigned extends KeyStateUpdate

  def apply(metrics: Metrics, cacheSize: Long = 500000L)(implicit
      executionContext: ExecutionContext
  ): ContractsKeyCache = ContractsKeyCache(
    SizedCache.from[GlobalKey, KeyStateUpdate](
      SizedCache.Configuration(cacheSize),
      metrics.daml.execution.contractsKeyStateCache,
    )
  )
}
