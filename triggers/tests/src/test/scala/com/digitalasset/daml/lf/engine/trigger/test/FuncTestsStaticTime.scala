// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.test

import com.daml.platform.services.time.TimeProviderType

final class FuncTestsStaticTime extends AbstractFuncTests {
  override def config = super.config.copy(timeProviderType = Some(TimeProviderType.Static))
}
