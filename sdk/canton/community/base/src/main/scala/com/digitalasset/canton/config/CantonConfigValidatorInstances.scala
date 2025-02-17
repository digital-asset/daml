// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.daml.jwt.JwtTimestampLeeway
import com.daml.metrics.api.MetricQualification
import com.daml.metrics.{HistogramDefinition, MetricsFilterConfig}
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.config.RequireTypes.{
  ExistingFile,
  NonNegativeNumeric,
  Port,
  PositiveNumeric,
}
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.util.BytesUnit

/** Collects [[com.digitalasset.canton.config.CantonConfigValidator]] instances
  * that cannot be declared in the companion object of the classes due to project dependencies.
  */
object CantonConfigValidatorInstances {
  // RequireTypes
  implicit def portCantonConfigValidator: CantonConfigValidator[Port] =
    CantonConfigValidator.validateAll

  implicit def existingFileCantonConfigValidator: CantonConfigValidator[ExistingFile] =
    CantonConfigValidator.validateAll

  implicit def nonNegativeNumericCantonConfigValidator[A]
      : CantonConfigValidator[NonNegativeNumeric[A]] =
    CantonConfigValidator.validateAll

  implicit def positiveNumericCantonConfigValidator[A]: CantonConfigValidator[PositiveNumeric[A]] =
    CantonConfigValidator.validateAll

  implicit def nonEmptyStringCantonConfigValidator: CantonConfigValidator[NonEmptyString] =
    CantonConfigValidator.validateAll

  // util-external
  implicit def bytesUnitCantonConfigValidator: CantonConfigValidator[BytesUnit] =
    CantonConfigValidator.validateAll

  // util-logging
  implicit def tracingConfigCantonConfigValidator: CantonConfigValidator[TracingConfig] =
    CantonConfigValidator.validateAll

  // Daml repo
  implicit def jwtTimestampLeewayCantonConfigValidator: CantonConfigValidator[JwtTimestampLeeway] =
    CantonConfigValidator.validateAll

  implicit def histogramDefinitionCantonConfigValidator
      : CantonConfigValidator[HistogramDefinition] = CantonConfigValidator.validateAll

  implicit def metricsFilterConfigCantonConfigValidator
      : CantonConfigValidator[MetricsFilterConfig] = CantonConfigValidator.validateAll

  implicit def metricQualificationCantonConfigValidator
      : CantonConfigValidator[MetricQualification] =
    CantonConfigValidator.validateAll
}
