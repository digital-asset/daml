// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.model.converter

import com.digitalasset.navigator.model.DamlLfDefRef

sealed trait ConversionError extends Throwable

/** Conversion failed because a type is missing. */
final case class TypeNotFoundError(id: DamlLfDefRef) extends ConversionError {
  override def getMessage: String = s"Type $id not found"
}

/** A required field is missing. */
final case class RequiredFieldDoesNotExistError(name: String) extends ConversionError {
  override def getMessage: String = s"Required field $name is missing"
}

/** Generic conversion error, with a human readable error string. */
final case class GenericConversionError(error: String) extends ConversionError {
  override def getMessage: String = error
}
