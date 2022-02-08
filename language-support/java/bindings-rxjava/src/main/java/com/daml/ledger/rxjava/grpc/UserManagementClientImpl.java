// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.grpc.adapter.ExecutionSequencerFactory;
import com.daml.ledger.api.v1.admin.UserManagementServiceGrpc;
import com.daml.ledger.api.v1.admin.UserManagementServiceGrpc.UserManagementServiceFutureStub;
import com.daml.ledger.api.v1.admin.UserManagementServiceOuterClass;
import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.rxjava.UserManagementClient;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import com.daml.ledger.rxjava.util.CreateSingle;
import io.grpc.Channel;
import io.reactivex.Single;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class UserManagementClientImpl implements UserManagementClient {

  private final UserManagementServiceFutureStub serviceFutureStub;
  private final ExecutionSequencerFactory sequencerFactory;

  public UserManagementClientImpl(
      @NonNull Channel channel,
      @NonNull ExecutionSequencerFactory sequencerFactory,
      @NonNull Optional<String> accessToken) {
    this.serviceFutureStub =
        StubHelper.authenticating(UserManagementServiceGrpc.newFutureStub(channel), accessToken);
    this.sequencerFactory = sequencerFactory;
  }

  private Single<User> createUser(
      @NonNull CreateUserRequest request, @NonNull Optional<String> maybeToken) {
    return CreateSingle.fromFuture(
            StubHelper.authenticating(this.serviceFutureStub, maybeToken)
                .createUser(request.toProto()),
            sequencerFactory)
        .map(res -> User.fromProto(res.getUser()));
  }

  @Override
  public Single<User> createUser(@NonNull CreateUserRequest request) {
    return createUser(request, Optional.empty());
  }

  @Override
  public Single<User> createUser(@NonNull CreateUserRequest request, @NonNull String accessToken) {
    return createUser(request, Optional.of(accessToken));
  }

  private Single<User> getUser(
      @NonNull GetUserRequest request, @NonNull Optional<String> maybeToken) {
    return CreateSingle.fromFuture(
            StubHelper.authenticating(this.serviceFutureStub, maybeToken)
                .getUser(request.toProto()),
            sequencerFactory)
        .map(res -> User.fromProto(res.getUser()));
  }

  @Override
  public Single<User> getUser(@NonNull GetUserRequest request) {
    return getUser(request, Optional.empty());
  }

  @Override
  public Single<User> getUser(@NonNull GetUserRequest request, @NonNull String accessToken) {
    return getUser(request, Optional.of(accessToken));
  }

  private Single<DeleteUserResponse> deleteUser(
      @NonNull DeleteUserRequest request, @NonNull Optional<String> maybeToken) {
    return CreateSingle.fromFuture(
            StubHelper.authenticating(this.serviceFutureStub, maybeToken)
                .deleteUser(request.toProto()),
            sequencerFactory)
        .map(DeleteUserResponse::fromProto);
  }

  @Override
  public Single<DeleteUserResponse> deleteUser(@NonNull DeleteUserRequest request) {
    return deleteUser(request, Optional.empty());
  }

  @Override
  public Single<DeleteUserResponse> deleteUser(
      @NonNull DeleteUserRequest request, @NonNull String accessToken) {
    return deleteUser(request, Optional.of(accessToken));
  }

  private Single<ListUsersResponse> listUsers(@NonNull Optional<String> maybeToken) {
    return CreateSingle.fromFuture(
            StubHelper.authenticating(this.serviceFutureStub, maybeToken)
                .listUsers(UserManagementServiceOuterClass.ListUsersRequest.getDefaultInstance()),
            sequencerFactory)
        .map(ListUsersResponse::fromProto);
  }

  @Override
  public Single<ListUsersResponse> listUsers() {
    return listUsers(Optional.empty());
  }

  @Override
  public Single<ListUsersResponse> listUsers(String accessToken) {
    return listUsers(Optional.of(accessToken));
  }

  private Single<GrantUserRightsResponse> grantUserRights(
      @NonNull GrantUserRightsRequest request, @NonNull Optional<String> maybeToken) {
    return CreateSingle.fromFuture(
            StubHelper.authenticating(this.serviceFutureStub, maybeToken)
                .grantUserRights(request.toProto()),
            sequencerFactory)
        .map(GrantUserRightsResponse::fromProto);
  }

  @Override
  public Single<GrantUserRightsResponse> grantUserRights(@NonNull GrantUserRightsRequest request) {
    return grantUserRights(request, Optional.empty());
  }

  @Override
  public Single<GrantUserRightsResponse> grantUserRights(
      @NonNull GrantUserRightsRequest request, String accessToken) {
    return grantUserRights(request, Optional.of(accessToken));
  }

  private Single<RevokeUserRightsResponse> revokeUserRights(
      @NonNull RevokeUserRightsRequest request, @NonNull Optional<String> maybeToken) {
    return CreateSingle.fromFuture(
            StubHelper.authenticating(this.serviceFutureStub, maybeToken)
                .revokeUserRights(request.toProto()),
            sequencerFactory)
        .map(RevokeUserRightsResponse::fromProto);
  }

  @Override
  public Single<RevokeUserRightsResponse> revokeUserRights(
      @NonNull RevokeUserRightsRequest request) {
    return revokeUserRights(request, Optional.empty());
  }

  @Override
  public Single<RevokeUserRightsResponse> revokeUserRights(
      @NonNull RevokeUserRightsRequest request, String accessToken) {
    return revokeUserRights(request, Optional.of(accessToken));
  }

  private Single<ListUserRightsResponse> listUserRights(
      @NonNull ListUserRightsRequest request, @NonNull Optional<String> maybeToken) {
    return CreateSingle.fromFuture(
            StubHelper.authenticating(this.serviceFutureStub, maybeToken)
                .listUserRights(request.toProto()),
            sequencerFactory)
        .map(ListUserRightsResponse::fromProto);
  }

  @Override
  public Single<ListUserRightsResponse> listUserRights(@NonNull ListUserRightsRequest request) {
    return listUserRights(request, Optional.empty());
  }

  @Override
  public Single<ListUserRightsResponse> listUserRights(
      @NonNull ListUserRightsRequest request, String accessToken) {
    return listUserRights(request, Optional.of(accessToken));
  }
}
