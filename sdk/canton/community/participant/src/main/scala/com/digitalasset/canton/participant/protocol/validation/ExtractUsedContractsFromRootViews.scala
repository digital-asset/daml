// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.data.TransactionView
import com.digitalasset.canton.protocol.{GenContractInstance, LfContractId}

object ExtractUsedContractsFromRootViews {

  /** Extract the input contracts for all unblinded
    * [[com.digitalasset.canton.data.ViewParticipantData]] nodes in the `rootViews`.
    */
  def apply(
      rootViews: List[TransactionView]
  ): List[(LfContractId, GenContractInstance)] = {
    val coreInputs = List.newBuilder[(LfContractId, GenContractInstance)]

    def go(view: TransactionView): Unit = {
      view.viewParticipantData.unwrap.foreach { vpd =>
        coreInputs ++= vpd.coreInputs.map { case (cid, inputContract) =>
          (cid, inputContract.contract)
        }
      }

      view.subviews.unblindedElements.foreach(go)
    }

    rootViews.foreach(go)
    coreInputs.result()
  }
}
