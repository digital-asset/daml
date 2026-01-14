// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements.dvp

import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.performance.model.java as M

object DvpPrettyInstances {
  import com.digitalasset.canton.participant.pretty.Implicits.*
  implicit val implicitPrettyString: Pretty[String] = Pretty.prettyString

  implicit val prettyAssetRequest: Pretty[M.dvp.asset.AssetRequest] = {
    import Pretty.*
    prettyOfClass(
      param("id", _.id.limit(10)),
      param("requester", _.requester),
      param("issuer", _.issuer),
      param("quantity", _.quantity.toLong),
      param("payload.length", _.payload.length),
    )
  }

  implicit val prettyAsset: Pretty[M.dvp.asset.Asset] = {
    import Pretty.*
    prettyOfClass(
      param("id", _.id.limit(10)),
      param("owner", _.owner),
      param("issuer", _.issuer),
      param("payload.length", _.payload.length),
    )
  }

  implicit val prettyTradePropose: Pretty[M.dvp.trade.Propose] = {
    import Pretty.*
    prettyOfClass(
      param("proposer", _.proposer),
      param("counterparty", _.counterparty),
      param("issuer", _.issuer),
      param("transferCid", _.transferCid),
    )
  }
}
