// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer

import com.digitalasset.canton.config.StorageConfig
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.logging.NamedLoggerFactory

import scala.reflect.ClassTag

package object reference {

  private[reference] val DriverName: String = "community-reference"

  private[reference] def createPlugin[S <: StorageConfig](
      loggerFactory: NamedLoggerFactory
  )(implicit c: ClassTag[S]) =
    new UseCommunityReferenceBlockSequencer[S](loggerFactory)
}
