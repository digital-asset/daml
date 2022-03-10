// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil.nonempty
package catsinstances

package object impl {
  val ImplicitPreference: ImplicitPreferenceModule.Module.type = ImplicitPreferenceModule.Module
  type ImplicitPreference[+A] = ImplicitPreference.T[A]
}
