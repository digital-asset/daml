// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.ValueOuterClass;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class Identifier {

  private final String packageId;
  private final String packageName;
  private final String moduleName;
  private final String entityName;

  public Identifier(
      @NonNull String packageId,
      @NonNull String packageName,
      @NonNull String moduleName,
      @NonNull String entityName) {
    this.moduleName = moduleName;
    this.entityName = entityName;
    if (packageId.startsWith("#")) {
      final String inferredPackageName = packageId.substring(1);
      if (packageName.isEmpty() || packageName.equals(inferredPackageName)) {
        this.packageId = "";
        this.packageName = inferredPackageName;
      } else {
        throw new IllegalArgumentException(
            "Package name defined by packageId ["
                + packageId
                + "] differs from packageName ["
                + packageName
                + "]");
      }
    } else {
      this.packageId = packageId;
      this.packageName = packageName;
    }
  }

  public Identifier(
      @NonNull String packageId, @NonNull String moduleName, @NonNull String entityName) {
    this(packageId, "", moduleName, entityName);
  }

  @NonNull
  public static Identifier fromProto(ValueOuterClass.Identifier identifier) {
    if (!identifier.getModuleName().isEmpty() && !identifier.getEntityName().isEmpty()) {
      return new Identifier(
          identifier.getPackageId(),
          identifier.getPackageName(),
          identifier.getModuleName(),
          identifier.getEntityName());
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
        .setPackageName(this.packageName)
        .setModuleName(this.moduleName)
        .setEntityName(this.entityName)
        .build();
  }

  @NonNull
  public String getPackageId() {
    return packageId;
  }

  @NonNull
  public String getPackageName() {
    return packageName;
  }

  @NonNull
  public String getModuleName() {
    return moduleName;
  }

  @NonNull
  public String getEntityName() {
    return entityName;
  }

  @NonNull
  public Identifier withoutPackageId() {
    return new Identifier("", packageName, moduleName, entityName);
  }

  @NonNull
  public Identifier withoutPackageName() {
    return new Identifier(packageId, "", moduleName, entityName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Identifier that = (Identifier) o;
    return Objects.equals(packageId, that.packageId)
        && Objects.equals(packageName, that.packageName)
        && Objects.equals(moduleName, that.moduleName)
        && Objects.equals(entityName, that.entityName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(packageId, packageName, moduleName, entityName);
  }

  @Override
  public String toString() {
    return "Identifier{"
        + "packageId='"
        + packageId
        + '\''
        + "packageName='"
        + packageName
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
