// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

package object data {

  private[data] def assertRight[X](either: Either[String, X]): X =
    either.fold(e => throw new IllegalArgumentException(e), identity)
}
