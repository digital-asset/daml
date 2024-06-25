// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.services

import com.daml.ledger.api.v2.admin.command_inspection_service.CommandState
import com.digitalasset.canton.platform.apiserver.execution.CommandStatus

import scala.concurrent.Future

trait CommandInspectionService {
  def findCommandStatus(
      commandIdPrefix: String,
      state: CommandState,
      limit: Int,
  ): Future[Seq[CommandStatus]]

}
