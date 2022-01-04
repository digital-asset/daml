--  Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

CREATE TABLE transaction_metering (
    application_id NVARCHAR2(1000) NOT NULL,
    action_count NUMBER NOT NULL,
    from_timestamp NUMBER NOT NULL,
    to_timestamp NUMBER NOT NULL,
    from_ledger_offset VARCHAR2(4000) NOT NULL,
    to_ledger_offset VARCHAR2(4000) NOT NULL
);

CREATE UNIQUE INDEX transaction_metering_from_timestamp_application ON transaction_metering(from_ledger_offset, application_id);


