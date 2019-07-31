// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.jwt

import java.io.File

package object domain {
  final case class Keys(publicKey: File, privateKey: File)
  final case class Jwt(value: String)
}
