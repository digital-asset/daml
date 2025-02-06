// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.annotations;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotation for tagging whole test suites as unstable. Unstable tests will only run as part of
 * unstable_test jobs. Unstable tests are still periodically executed and failures are reported to
 * DataDog. But pull requests can still be merged, even if unstable tests fail.
 */
@org.scalatest.TagAnnotation
@Inherited
@Retention(RUNTIME)
@Target({METHOD, TYPE})
public @interface UnstableTest {}
