// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.admin.UserManagementServiceOuterClass;

public final class DeleteUserResponse {

  private static final DeleteUserResponse INSTANCE = new DeleteUserResponse();

  private DeleteUserResponse() {}

  @Override
  public String toString() {
    return "DeleteUserResponse{}";
  }

  public static DeleteUserResponse fromProto(
      UserManagementServiceOuterClass.DeleteUserResponse response) {
    // As this is so far a singleton, we just ignore the response
    return INSTANCE;
  }
}
