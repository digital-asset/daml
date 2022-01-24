// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

package object exporting {

  val header = new Header(version = "v2")

  type WriteItem = (Raw.Key, Raw.Envelope)

  type WriteSet = collection.Seq[WriteItem]

}
