// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.bftordering

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.sequencer.SynchronizerBootstrapWithMultipleConsolesAndSequencersIntegrationTest

class BftOrderingSynchronizerBootstrapWithSeparateConsolesIntegrationTest
    extends SynchronizerBootstrapWithMultipleConsolesAndSequencersIntegrationTest {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
