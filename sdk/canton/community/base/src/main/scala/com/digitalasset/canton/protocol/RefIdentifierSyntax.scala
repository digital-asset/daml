// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Identifier
import com.digitalasset.canton.ProtoDeserializationError.ValueDeserializationError

object RefIdentifierSyntax {
  implicit class RefIdentifierSyntax(private val identifier: Ref.Identifier) extends AnyVal {
    def toProtoPrimitive: String = identifier.toString()
  }

  def fromProtoPrimitive(
      interfaceIdP: String
  ): Either[ValueDeserializationError, Identifier] = Ref.Identifier
    .fromString(interfaceIdP)
    .leftMap(err => ValueDeserializationError("identifier", err))
}
