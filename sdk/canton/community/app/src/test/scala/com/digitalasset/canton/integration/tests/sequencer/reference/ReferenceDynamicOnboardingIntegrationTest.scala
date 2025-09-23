// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.reference

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer

class ReferenceDynamicOnboardingIntegrationTest
    extends ReferenceDynamicOnboardingIntegrationTestBase(DriverName) {

  override protected lazy val plugin: UseReferenceBlockSequencer[DbConfig.Postgres] =
    createPlugin[DbConfig.Postgres](loggerFactory)
}
