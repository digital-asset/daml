// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.data

import cats.syntax.either.*
import com.digitalasset.canton.TransferCounterO
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.version.ProtocolVersion

final case class ActiveContractData(contractId: LfContractId, transferCounter: TransferCounterO)

final case class ActiveContractsData private (
    protocolVersion: ProtocolVersion,
    toc: TimeOfChange,
    contracts: Iterable[ActiveContractData],
) {

  if (protocolVersion < ProtocolVersion.CNTestNet) {
    require(
      contracts.forall(tc => tc.transferCounter.isEmpty),
      s"The reassignment counter must be empty for protocol version lower than '${ProtocolVersion.CNTestNet}'.",
    )
  } else {
    require(
      contracts.forall(tc => tc.transferCounter.isDefined),
      s"The reassignment counter must be defined for protocol version '${ProtocolVersion.CNTestNet}' or higher.",
    )
  }

  def contractIds: Seq[LfContractId] = contracts.map(_.contractId).toSeq

  def asMap: Map[LfContractId, (TransferCounterO, TimeOfChange)] =
    contracts.view.map(tc => tc.contractId -> (tc.transferCounter, toc)).toMap

  def asSeq: Seq[ActiveContractData] =
    contracts.toSeq

}

object ActiveContractsData {

  def create(
      protocolVersion: ProtocolVersion,
      toc: TimeOfChange,
      contracts: Seq[(LfContractId, TransferCounterO)],
  ): Either[String, ActiveContractsData] = {
    Either
      .catchOnly[IllegalArgumentException](
        ActiveContractsData(
          protocolVersion,
          toc,
          contracts.map(ActiveContractData.tupled),
        )
      )
      .leftMap(_.getMessage)

  }

}
