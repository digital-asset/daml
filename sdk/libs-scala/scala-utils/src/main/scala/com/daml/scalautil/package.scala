// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

package object scalautil {
  val ImplicitPreference: ImplicitPreferenceModule = ImplicitPreferenceModule.Module
  type ImplicitPreference[+A] = ImplicitPreference.T[A]
}
