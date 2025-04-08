// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer

import com.digitalasset.canton.integration.plugins.UseBftSequencer
import com.digitalasset.canton.logging.NamedLoggerFactory

package object bftordering {

  private[bftordering] def createPlugin(
      loggerFactory: NamedLoggerFactory
  ) = new UseBftSequencer(loggerFactory)
}
