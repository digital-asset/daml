// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

class TriggerServiceTestAuthWithPostgres
    extends AbstractTriggerServiceTest
    with AbstractTriggerServiceTestWithDatabase
    with TriggerDaoPostgresFixture
    with AbstractTriggerServiceTestAuthMiddleware {}
