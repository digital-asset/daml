package com.daml.platform.store.state

import com.daml.caching.{Cache, SizedCache}
import com.daml.lf.data.Ref.Party
import com.daml.metrics.Metrics
import com.daml.platform.store.dao.events.{Contract, ContractId}
import com.daml.platform.store.state.ContractsStateCache.ContractCacheValue

import scala.concurrent.ExecutionContext

class ContractsStateCache(val cache: Cache[ContractId, ContractCacheValue])(implicit
    val ec: ExecutionContext
) extends StateCache[ContractId, ContractCacheValue]

object ContractsStateCache {
  // Caching possible lifecycle states of a contract, together with its stakeholders
  sealed trait ContractCacheValue
  // Inexistent contracts (e.g. coming from malformed submissions)
  // -- This situation is outside the happy flow, but it may indicate an bug
  final case object NotFound extends ContractCacheValue

  sealed trait ExistingContractValue extends ContractCacheValue
  final case class Active(contract: Contract, stakeholders: Set[Party])
      extends ExistingContractValue
  // For archivals we need the contract still so it can be served to divulgees that did not observe the archival
  final case class Archived(archivedAt: Long, stakeholders: Set[Party])
      extends ExistingContractValue

  def apply(metrics: Metrics)(implicit
      ec: ExecutionContext
  ): ContractsStateCache = new ContractsStateCache(
    SizedCache.from[ContractId, ContractCacheValue](
      SizedCache.Configuration(500000L),
      metrics.daml.execution.contractsStateCache,
    )
  )
}
