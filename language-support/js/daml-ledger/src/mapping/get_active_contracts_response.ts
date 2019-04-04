// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const GetActiveContractsResponse: mapping.Mapping<grpc.GetActiveContractsResponse, ledger.GetActiveContractsResponse> = {
    toObject(response: grpc.GetActiveContractsResponse): ledger.GetActiveContractsResponse {
        const result: ledger.GetActiveContractsResponse = {
            offset: { absolute: response.getOffset() },
        };
        const workflowId = response.getWorkflowId();
        if (response.getWorkflowId() !== undefined && response.getWorkflowId() !== '') {
            result.workflowId = workflowId;
        }
        const activeContracts = response.getActiveContractsList();
        if (activeContracts) {
            result.activeContracts = activeContracts.map(event => mapping.CreatedEvent.toObject(event));
        }
        return result;
    },
    toMessage(response: ledger.GetActiveContractsResponse): grpc.GetActiveContractsResponse {
        const result = new grpc.GetActiveContractsResponse();
        result.setOffset(response.offset.absolute!);
        if (response.activeContracts) {
            result.setActiveContractsList(response.activeContracts.map(mapping.CreatedEvent.toMessage));
        }
        if (response.workflowId) {
            result.setWorkflowId(response.workflowId)
        }
        return result;
    }
}