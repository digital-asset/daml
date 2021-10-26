// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.ledger.resources.Resource
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.platform.store.LfValueTranslationCache
import com.daml.platform.store.interfaces.LedgerDaoContractsReader

import scala.concurrent.Future

class TranslationCacheBackedContractStore(
    lfValueTranslationCache: LfValueTranslationCache.Cache,
    contractsReader: LedgerDaoContractsReader,
) extends ContractStore {
  override def lookupActiveContract(
      readers: Set[Party],
      contractId: ContractId,
  )(implicit loggingContext: LoggingContext): Future[Option[Contract]] =
    lfValueTranslationCache.contracts
      .getIfPresent(LfValueTranslationCache.ContractCache.Key(contractId)) match {
      case Some(createArgument) =>
        contractsReader.lookupActiveContractWithCachedArgument(
          readers,
          contractId,
          createArgument.argument,
        )
      case None =>
        contractsReader.lookupActiveContractAndLoadArgument(readers, contractId)
    }

  override def lookupContractKey(readers: Set[Party], key: Key)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]] =
    contractsReader.lookupContractKey(key, readers)

  /** @return The maximum ledger effective time of all contracts in ids, fails as follows:
    *         - if ids is empty or not all the non-divulged ids can be found, a failed [[Future]]
    *         - if all ids are found but each refer to a divulged contract, a successful [[None]]
    */
  override def lookupMaximumLedgerTime(ids: Set[ContractId])(implicit
      loggingContext: LoggingContext
  ): Future[Option[Timestamp]] =
    contractsReader.lookupMaximumLedgerTime(ids)
}

object TranslationCacheBackedContractStore {
  def owner(
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      contractsReader: LedgerDaoContractsReader,
  ): Resource[TranslationCacheBackedContractStore] =
    Resource.successful(
      new TranslationCacheBackedContractStore(
        lfValueTranslationCache,
        contractsReader,
      )
    )
}
