// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import java.util.*;
import org.checkerframework.checker.nullness.qual.NonNull;

// FIXME When removing this after the deprecation period is over, make DamlTextMap final
/** @deprecated Use {@link DamlTextMap} instead. */
@Deprecated
public final class DamlMap extends DamlTextMap {

  public DamlMap(Map<@NonNull String, @NonNull Value> value) {
    super(Collections.unmodifiableMap(new HashMap<>(value)));
  }
}
