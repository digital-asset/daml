// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.model

sealed trait DatabaseError extends Throwable

final case class NoDatabaseUsed() extends DatabaseError {
  override def getMessage: String = "No database is used."
}

final case class DeserializationFailed(msg: String) extends DatabaseError {
  override def getMessage: String = s"Deserialization failed while reading from the db: $msg"
}

final case class RecordNotFound(msg: String) extends DatabaseError {
  override def getMessage: String = s"Record not found: $msg"
}
