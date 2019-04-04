// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

export { TimeServiceService as TimeService, TimeServiceClient as TimeClient, ITimeServiceClient as ITimeClient } from '../com/digitalasset/ledger/api/v1/testing/time_service_grpc_pb';
export { GetTimeRequest, GetTimeResponse, SetTimeRequest } from '../com/digitalasset/ledger/api/v1/testing/time_service_pb';

export { ResetServiceService as ResetService, ResetServiceClient as ResetClient, IResetServiceClient as IResetClient } from '../com/digitalasset/ledger/api/v1/testing/reset_service_grpc_pb';
export { ResetRequest } from '../com/digitalasset/ledger/api/v1/testing/reset_service_pb';