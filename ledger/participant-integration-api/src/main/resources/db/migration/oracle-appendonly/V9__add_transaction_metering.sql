--  Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

CREATE TABLE transaction_metering (
    application_id NVARCHAR2(1000) NOT NULL,
    action_count NUMBER NOT NULL,
    metering_timestamp NUMBER NOT NULL,
    ledger_offset VARCHAR2(4000) NOT NULL
);

CREATE INDEX transaction_metering_ledger_offset ON transaction_metering(ledger_offset);


