// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.*;
import io.reactivex.Single;
import org.checkerframework.checker.nullness.qual.NonNull;

/** An RxJava version of {@link com.daml.ledger.api.v1.admin.UserManagementServiceGrpc} */
public interface UserManagementClient {

  Single<CreateUserResponse> createUser(@NonNull CreateUserRequest request);

  Single<CreateUserResponse> createUser(@NonNull CreateUserRequest request, String accessToken);

  Single<GetUserResponse> getUser(@NonNull GetUserRequest request);

  Single<GetUserResponse> getUser(@NonNull GetUserRequest request, String accessToken);

  Single<DeleteUserResponse> deleteUser(@NonNull DeleteUserRequest request);

  Single<DeleteUserResponse> deleteUser(@NonNull DeleteUserRequest request, String accessToken);

  Single<ListUsersResponse> listUsers();

  Single<ListUsersResponse> listUsers(String accessToken);

  Single<GrantUserRightsResponse> grantUserRights(@NonNull GrantUserRightsRequest request);

  Single<GrantUserRightsResponse> grantUserRights(
      @NonNull GrantUserRightsRequest request, String accessToken);

  Single<RevokeUserRightsResponse> revokeUserRights(@NonNull RevokeUserRightsRequest request);

  Single<RevokeUserRightsResponse> revokeUserRights(
      @NonNull RevokeUserRightsRequest request, String accessToken);

  Single<ListUserRightsResponse> listUserRights(@NonNull ListUserRightsRequest request);

  Single<ListUserRightsResponse> listUserRights(
      @NonNull ListUserRightsRequest request, String accessToken);
}
