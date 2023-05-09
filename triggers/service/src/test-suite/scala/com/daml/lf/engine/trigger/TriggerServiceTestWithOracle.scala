// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

class TriggerServiceTestWithOracle
    extends AbstractTriggerServiceTestWithCanton
    with AbstractTriggerServiceTestWithDatabaseAndCanton
    with TriggerDaoOracleCantonFixture
    with AbstractTriggerServiceTestNoAuthWithCanton {}
