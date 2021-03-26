// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events.contracts

import java.time.Instant

import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.ledger.resources.Resource
import com.daml.lf.data.Ref
import com.daml.lf.transaction.GlobalKey
import com.daml.logging.LoggingContext
import com.daml.platform.store.dao.events._

import scala.concurrent.Future

class TranslationCacheBackedContractsStore(
    lfValueTranslationCache: LfValueTranslation.Cache,
    contractsReader: LedgerDaoContractsReader,
) extends ContractStore {
  override def lookupActiveContract(
      readers: Set[Party],
      contractId: ContractId,
  )(implicit loggingContext: LoggingContext): Future[Option[Contract]] =
    lfValueTranslationCache.contracts
      .getIfPresent(LfValueTranslation.ContractCache.Key(contractId)) match {
      case Some(createArgument) =>
        contractsReader.lookupActiveContractWithCachedArgument(
          contractId,
          readers,
          createArgument.argument,
        )
      case None =>
        contractsReader.lookupActiveContractAndLoadArgument(contractId, readers)
    }

  override def lookupContractKey(readers: Set[Ref.Party], key: GlobalKey)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]] =
    contractsReader.lookupContractKey(key, readers)

  /** @return The maximum ledger effective time of all contracts in ids, fails as follows:
    *         - if ids is empty or not all the non-divulged ids can be found, a failed [[Future]]
    *         - if all ids are found but each refer to a divulged contract, a successful [[None]]
    */
  override def lookupMaximumLedgerTime(ids: Set[ContractId])(implicit
      loggingContext: LoggingContext
  ): Future[Option[Instant]] =
    contractsReader.lookupMaximumLedgerTime(ids)
}

object TranslationCacheBackedContractsStore {
  def owner(
      lfValueTranslationCache: LfValueTranslation.Cache,
      contractsReader: LedgerDaoContractsReader,
  ): Resource[TranslationCacheBackedContractsStore] =
    Resource.successful(
      new TranslationCacheBackedContractsStore(
        lfValueTranslationCache,
        contractsReader,
      )
    )
}
