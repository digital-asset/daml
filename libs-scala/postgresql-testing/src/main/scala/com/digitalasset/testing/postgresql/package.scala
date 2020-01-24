// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.testing

package object postgresql {
  private[postgresql] val isWindows =
    sys.props("os.name").toLowerCase.contains("windows")
}
