// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.test

import com.daml.platform.services.time.TimeProviderType

final class FuncTestsWallClock extends AbstractFuncTests {

  final override protected lazy val timeProviderType: TimeProviderType = TimeProviderType.WallClock

}
