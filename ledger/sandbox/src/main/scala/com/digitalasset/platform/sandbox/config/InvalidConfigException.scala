// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.config

import com.digitalasset.resources.ProgramResource.StartupException

class InvalidConfigException(message: String)
    extends RuntimeException(message)
    with StartupException
