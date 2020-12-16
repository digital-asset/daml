// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.language

final case class LanguageMinorVersion(identifier: String) {
  def toProtoIdentifier: String = identifier
}
