// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator.admin.gprc

import com.digitalasset.canton.mediator.admin.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class InitializeMediatorResponse() {
  def toProtoV30: v30.InitializeMediatorResponse = v30.InitializeMediatorResponse()
}

object InitializeMediatorResponse {

  def fromProtoV30(
      responseP: v30.InitializeMediatorResponse
  ): ParsingResult[InitializeMediatorResponse] = {
    val v30.InitializeMediatorResponse() = responseP
    Right(InitializeMediatorResponse())
  }
}
