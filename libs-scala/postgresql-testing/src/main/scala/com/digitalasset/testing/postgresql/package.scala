// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing

package object postgresql {
  private[postgresql] val isWindows =
    sys.props("os.name").toLowerCase.contains("windows")
}
