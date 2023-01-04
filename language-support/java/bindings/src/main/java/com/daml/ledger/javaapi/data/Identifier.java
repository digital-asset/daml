// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.ValueOuterClass;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class Identifier {

  private final String packageId;
  private final String moduleName;
  private final String entityName;

  /**
   * This constructor is deprecated in favor of {@link Identifier#Identifier(String, String,
   * String)}
   */
  @Deprecated
  public Identifier(@NonNull String packageId, @NonNull String name) {
    this.packageId = packageId;
    int lastDot = name.lastIndexOf('.');
    if (lastDot <= 0) {
      // The module component of the name must be at least 1 character long.
      // if no '.' is found or it is on the first position, then the name is not a valid identifier.
      throw new IllegalArgumentException(
          String.format(
              "Identifier name [%s] has wrong format. Dot-separated module and entity name"
                  + " expected (e.g.: Foo.Bar)",
              name));
    }
    this.moduleName = name.substring(0, lastDot);
    this.entityName = name.substring(lastDot + 1);
  }

  public Identifier(
      @NonNull String packageId, @NonNull String moduleName, @NonNull String entityName) {
    this.packageId = packageId;
    this.moduleName = moduleName;
    this.entityName = entityName;
  }

  @NonNull
  public static Identifier fromProto(ValueOuterClass.Identifier identifier) {
    if (!identifier.getModuleName().isEmpty() && !identifier.getEntityName().isEmpty()) {
      return new Identifier(
          identifier.getPackageId(), identifier.getModuleName(), identifier.getEntityName());
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Invalid identifier [%s]: both module_name and entity_name must be set.",
              identifier));
    }
  }

  public ValueOuterClass.Identifier toProto() {
    return ValueOuterClass.Identifier.newBuilder()
        .setPackageId(this.packageId)
        .setModuleName(this.moduleName)
        .setEntityName(this.entityName)
        .build();
  }

  @NonNull
  public String getPackageId() {
    return packageId;
  }

  @NonNull
  @Deprecated
  public String getName() {
    return moduleName.concat(".").concat(entityName);
  }

  @NonNull
  public String getModuleName() {
    return moduleName;
  }

  @NonNull
  public String getEntityName() {
    return entityName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Identifier that = (Identifier) o;
    return Objects.equals(packageId, that.packageId)
        && Objects.equals(moduleName, that.moduleName)
        && Objects.equals(entityName, that.entityName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(packageId, moduleName, entityName);
  }

  @Override
  public String toString() {
    return "Identifier{"
        + "packageId='"
        + packageId
        + '\''
        + ", moduleName='"
        + moduleName
        + '\''
        + ", entityName='"
        + entityName
        + '\''
        + '}';
  }
}
