// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import java.util.*;
import org.checkerframework.checker.nullness.qual.NonNull;

@Deprecated // Use DamlTextMap
public class DamlMap extends DamlTextMap {

  public DamlMap(Map<@NonNull String, @NonNull Value> value) {
    super(Collections.unmodifiableMap(new HashMap<>(value)));
  }
}
