// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.data

import com.digitalasset.canton.TransferCounter
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.util.EitherUtil

final case class ActiveContractData(
    contractId: LfContractId,
    transferCounter: TransferCounter,
    toc: TimeOfChange,
)

/*
Invariant: the pair (activeContractData.contractId, activeContractData.toc) is unique
Reason: otherwise the `asMap` below would discard data.
 */
final case class ActiveContractsData private (
    contracts: Iterable[ActiveContractData]
) {

  def contractIds: Seq[LfContractId] = contracts.map(_.contractId).toSeq

  def asMap: Map[(LfContractId, TimeOfChange), TransferCounter] =
    contracts.view.map(tc => (tc.contractId, tc.toc) -> tc.transferCounter).toMap

  def asSeq: Seq[ActiveContractData] =
    contracts.toSeq

}

object ActiveContractsData {
  /*
  Checks that there is only one transfer counter per (cid, toc)
   */
  private def checkCidTocUniqueness(
      contracts: Iterable[ActiveContractData]
  ): Either[String, Unit] = {
    val duplicates = contracts
      .groupMap(contract => (contract.contractId, contract.toc))(_.transferCounter)
      .filter { case (_, transferCounters) =>
        transferCounters.sizeCompare(1) > 0
      }
      .keySet

    EitherUtil.condUnitE(
      duplicates.isEmpty,
      s"The following (contractId, toc) have several transfer counters: $duplicates",
    )
  }

  def create(
      toc: TimeOfChange,
      contracts: Seq[(LfContractId, TransferCounter)],
  ): Either[String, ActiveContractsData] = {
    val activeContractsData = contracts.map { case (cid, tc) => ActiveContractData(cid, tc, toc) }
    checkCidTocUniqueness(activeContractsData).map(_ => ActiveContractsData(activeContractsData))
  }

  def create(
      contracts: Seq[(LfContractId, TransferCounter, TimeOfChange)]
  ): Either[String, ActiveContractsData] = {
    val activeContractsData = contracts.map(ActiveContractData.tupled)
    checkCidTocUniqueness(activeContractsData).map(_ => ActiveContractsData(activeContractsData))
  }
}
