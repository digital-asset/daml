// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.daml.lf.data.Ref.{ChoiceName, Identifier}
import com.digitalasset.daml.lf.engine.{Engine, Enricher, Result}
import com.digitalasset.daml.lf.transaction.{FatContractInstance, VersionedTransaction}
import com.digitalasset.daml.lf.value.Value

/** Enricher for LF values.
  */
trait LfEnricher {

  def enrichVersionedTransaction(
      versionedTransaction: VersionedTransaction
  ): Result[VersionedTransaction]

  def enrichContractInstance(contract: FatContractInstance): Result[FatContractInstance]

  def enrichContractValue(tyCon: Identifier, value: Value): Result[Value]

  def enrichChoiceArgument(
      toIdentifier: Identifier,
      interfaceId: Option[Identifier],
      choiceName: ChoiceName,
      unversioned: Value,
  ): Result[Value]

  def enrichChoiceResult(
      toIdentifier: Identifier,
      interfaceId: Option[Identifier],
      choiceName: ChoiceName,
      unversioned: Value,
  ): Result[Value]

  def enrichContractKey(toIdentifier: Identifier, value: Value): Result[Value]

  def enrichView(interfaceId: Identifier, value: Value): Result[Value]
}

object LfEnricher {

  def apply(engine: Engine, forbidLocalContractIds: Boolean): LfEnricher =
    new LfEnricherImpl(
      new Enricher(
        engine = engine,
        addTrailingNoneFields = false,
        forbidLocalContractIds = forbidLocalContractIds,
      )
    )

  class LfEnricherImpl(delegate: Enricher) extends LfEnricher {

    override def enrichVersionedTransaction(
        versionedTransaction: VersionedTransaction
    ): Result[VersionedTransaction] = delegate.enrichVersionedTransaction(versionedTransaction)

    override def enrichContractInstance(
        contract: FatContractInstance
    ): Result[FatContractInstance] =
      delegate.enrichContract(contract)

    override def enrichContractValue(tyCon: Identifier, value: Value): Result[Value] =
      delegate.enrichContract(tyCon, value)

    override def enrichChoiceArgument(
        toIdentifier: Identifier,
        interfaceId: Option[Identifier],
        choiceName: ChoiceName,
        unversioned: Value,
    ): Result[Value] =
      delegate.enrichChoiceArgument(toIdentifier, interfaceId, choiceName, unversioned)

    override def enrichChoiceResult(
        toIdentifier: Identifier,
        interfaceId: Option[Identifier],
        choiceName: ChoiceName,
        unversioned: Value,
    ): Result[Value] =
      delegate.enrichChoiceResult(toIdentifier, interfaceId, choiceName, unversioned)

    override def enrichContractKey(toIdentifier: Identifier, value: Value): Result[Value] =
      delegate.enrichContractKey(toIdentifier, value)

    override def enrichView(interfaceId: Identifier, value: Value): Result[Value] =
      delegate.enrichView(interfaceId, value)

  }

}
