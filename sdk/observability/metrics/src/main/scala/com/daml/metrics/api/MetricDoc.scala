// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api

import scala.annotation.StaticAnnotation

object MetricDoc {
  // How to use the MetricDoc tags to provide documentation for metrics:
  // -- use Tag to annotate a unique metric located in a single place: a.b.c
  // -- use GroupTag for similar leaf metrics that are rooted at multiple places: a.*.c
  // -- use FanTag when a single root fans out into a collection of similar but distinctly named metrics: a.b.*

  // The Tag can be defined to document a single metric. Its summary, description and
  // qualification will be present as a separate documentation entry unless a GroupTag is defined
  // for the class that may belongs.
  // labelsWithDescription must contain the labels that can be attached to the metrics.
  //    It must be represented as literal strings, as the documentation is built based on static annotations.
  //    Example: Map("label" -> "description"). Even if the label is a constant, it cannot be referenced, therefore
  //    this is not an acceptable use: Map(SomeObject.SomeConstant -> "description")
  case class Tag(
      summary: String,
      description: String,
      qualification: MetricQualification,
      labelsWithDescription: Map[String, String] = Map.empty,
  ) extends StaticAnnotation

  // The GroupTag can be defined for metrics that belong in the same class, are used in multiple
  // places and can be grouped using a wildcard (the representative). The metrics of the class
  // should be annotated with a Tag.
  case class GroupTag(representative: String, groupableClass: Class[_]) extends StaticAnnotation

  // The FanTag is used to define a documentation entry that will fan out and represent all the
  // metrics that are tagged with a FanInstanceTag and whose name matches the given representative
  // wildcard.
  case class FanTag(
      representative: String,
      summary: String,
      description: String,
      qualification: MetricQualification,
  ) extends StaticAnnotation

  // This tag works in combination with the FanTag and declares a metric that can be represented in
  // the documentation by the info of the corresponding FanTag.
  case class FanInstanceTag() extends StaticAnnotation

}
