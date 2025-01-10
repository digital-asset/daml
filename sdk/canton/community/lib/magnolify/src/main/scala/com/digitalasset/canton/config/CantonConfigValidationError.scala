// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.config

/** A validation error for a canton configuration.
  *
  * @param error The error message
  * @param context The path in the configuration where the error occurred
  */
final case class CantonConfigValidationError(error: String, context: Seq[String] = Seq.empty) {
  def augmentContext(elem: String): CantonConfigValidationError = copy(context = elem +: context)

  override def toString: String =
    if (context.isEmpty) s"Config validation error: $error"
    else s"Config validation error in ${context.mkString(".")}: $error"
}
