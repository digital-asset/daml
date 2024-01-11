// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

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
