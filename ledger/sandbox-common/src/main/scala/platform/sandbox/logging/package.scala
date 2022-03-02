// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import com.daml.lf.data.Ref
import com.daml.logging.entries.LoggingEntry

package object logging {

  def participantId(id: Ref.ParticipantId): LoggingEntry = "participantId" -> id

}
