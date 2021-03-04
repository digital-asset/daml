package com.daml.platform.store.state

import com.daml.caching.{Cache, SizedCache}
import com.daml.lf.data.Ref.Party
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.ContractId
import com.daml.metrics.Metrics
import com.daml.platform.store.state.ContractsKeyCache.{
  Assigned,
  KeyCacheValue,
  KeyStateUpdate,
  Unassigned,
}

import scala.concurrent.ExecutionContext

case class ContractsKeyCache(cache: Cache[GlobalKey, KeyCacheValue])(implicit
    protected val ec: ExecutionContext
) extends StateCache[GlobalKey, KeyStateUpdate, KeyCacheValue] {
  override protected def toUpdateAction(u: KeyStateUpdate): Option[KeyCacheValue] =
    u match {
      case Unassigned =>
        Some(Option.empty[(ContractId, Set[Party])])
      case Assigned(contractId, createWitnesses) =>
        Some(Some(contractId -> createWitnesses))
      case ContractsKeyCache.DontUpdate => None
    }
}

object ContractsKeyCache {
  type KeyCacheValue = Option[(ContractId, Set[Party])]
  sealed trait KeyStateUpdate extends Product with Serializable
  final case class Assigned(contractId: ContractId, createWitnesses: Set[Party])
      extends KeyStateUpdate
  final case object Unassigned extends KeyStateUpdate
  final case object DontUpdate extends KeyStateUpdate

  def apply(metrics: Metrics)(implicit
      executionContext: ExecutionContext
  ): ContractsKeyCache = ContractsKeyCache(
    SizedCache.from[GlobalKey, KeyCacheValue](
      SizedCache.Configuration(10000L),
      metrics.daml.execution.contractsKeyStateCache,
    )
  )
}
