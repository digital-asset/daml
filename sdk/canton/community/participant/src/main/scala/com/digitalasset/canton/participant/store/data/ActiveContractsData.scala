// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.data

import cats.syntax.either.*
import com.digitalasset.canton.participant.store.ActiveContractStore.ActivenessChangeDetail
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.version.ProtocolVersion

final case class ActiveContractData(
    contractId: LfContractId
)

final case class ActiveContractsData private (
    protocolVersion: ProtocolVersion,
    toc: TimeOfChange,
    contracts: Iterable[ActiveContractData],
) {
  def contractIds: Seq[LfContractId] = contracts.map(_.contractId).toSeq

  def asMap(
      change: ActivenessChangeDetail
  ): Map[(LfContractId, TimeOfChange), ActivenessChangeDetail] =
    contracts.view.map(tc => (tc.contractId, toc) -> change).toMap

  def asSeq: Seq[ActiveContractData] =
    contracts.toSeq

}

object ActiveContractsData {

  def create(
      protocolVersion: ProtocolVersion,
      toc: TimeOfChange,
      contracts: Seq[LfContractId],
  ): Either[String, ActiveContractsData] = {
    Either
      .catchOnly[IllegalArgumentException](
        ActiveContractsData(
          protocolVersion,
          toc,
          contracts.map(ActiveContractData),
        )
      )
      .leftMap(_.getMessage)

  }

}
