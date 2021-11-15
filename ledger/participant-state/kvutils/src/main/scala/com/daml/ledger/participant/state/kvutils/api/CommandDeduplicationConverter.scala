// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package ledger.participant.state.kvutils.api

import java.time.Duration

import scala.concurrent.Future

trait CommandDeduplicationConverter {

  def convertOffsetToDuration(offset: String): Future[Duration]

}
