// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.config

import com.daml.resources.ProgramResource.StartupException

class InvalidConfigException(message: String)
    extends RuntimeException(message)
    with StartupException
