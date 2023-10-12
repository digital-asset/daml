// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

/** Thrown when the bootstrap script fails to execute */
class BootstrapScriptException(cause: Throwable)
    extends RuntimeException(s"Bootstrap script failed: ${cause.getMessage}", cause)
