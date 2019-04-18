// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.CommandServiceOuterClass;

import java.time.Instant;
import java.util.List;

public class SubmitAndWaitRequest {

    public static CommandServiceOuterClass.SubmitAndWaitRequest toProto(String ledgerId,
                                                                        String workflowId,
                                                                        String applicationId,
                                                                        String commandId,
                                                                        String party,
                                                                        Instant ledgerEffectiveTime,
                                                                        Instant maximumRecordTime,
                                                                        List<Command> commands) {
        return CommandServiceOuterClass.SubmitAndWaitRequest.newBuilder()
                .setCommands(SubmitCommandsRequest.toProto(ledgerId, workflowId, applicationId,
                        commandId, party, ledgerEffectiveTime, maximumRecordTime, commands))
                .build();
    }
}
