// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

final case class AgreementText(v: String) extends AnyVal

object AgreementText {
  val empty = AgreementText("")
}
