// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.admin.gprc

import com.digitalasset.canton.mediator.admin.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class InitializeMediatorResponseX() {
  def toProtoV30: v30.InitializeMediatorResponse = v30.InitializeMediatorResponse()
}

object InitializeMediatorResponseX {

  def fromProtoV30(
      responseP: v30.InitializeMediatorResponse
  ): ParsingResult[InitializeMediatorResponseX] = {
    val v30.InitializeMediatorResponse() = responseP
    Right(InitializeMediatorResponseX())
  }
}
