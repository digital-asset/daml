// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import com.daml.lf.data.Ref.{Identifier, Party}
import com.daml.logging.entries.{LoggingKey, LoggingValue, ToLoggingKey, ToLoggingValue}

package object logging {

  implicit val `Identifier to LoggingValue`: ToLoggingValue[Identifier] =
    ToLoggingValue.ToStringToLoggingValue

  // The party name can grow quite long, so we offer ledger implementors the opportunity to truncate
  // it in structured log output.
  implicit val `Party to LoggingKey and LoggingValue`
      : ToLoggingKey[Party] with ToLoggingValue[Party] =
    new ToLoggingKey[Party] with ToLoggingValue[Party] {
      override def toLoggingKey(party: Party): LoggingKey =
        wrap(party).value

      override def toLoggingValue(party: Party): LoggingValue =
        wrap(party)

      private def wrap(party: Party): LoggingValue.OfString =
        LoggingConfiguration.current.maxPartyNameLength match {
          case None => LoggingValue.OfString(party)
          case Some(length) => LoggingValue.OfString(party).truncated(length)
        }
    }

}
