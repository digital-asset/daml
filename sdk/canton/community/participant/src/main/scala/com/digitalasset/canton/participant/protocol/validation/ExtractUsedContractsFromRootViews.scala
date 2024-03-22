// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.data.TransactionView
import com.digitalasset.canton.protocol.{LfContractId, SerializableContract}

object ExtractUsedContractsFromRootViews {

  /** Extract the input contracts for all unblinded [[com.digitalasset.canton.data.ViewParticipantData]] nodes
    * in the `rootViews`.
    */
  def apply(
      rootViews: List[TransactionView]
  ): List[(LfContractId, SerializableContract)] = {
    val coreInputs = List.newBuilder[(LfContractId, SerializableContract)]

    def go(view: TransactionView): Unit = {
      coreInputs ++= view.viewParticipantData.unwrap
        .map(_.coreInputs.map { case (cid, inputContract) =>
          (cid, inputContract.contract)
        })
        .getOrElse(List.empty)

      view.subviews.unblindedElements.foreach(go)
    }

    rootViews.foreach(go)
    coreInputs.result()
  }
}
