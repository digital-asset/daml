--  Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

-- Transaction metering alterations
ALTER TABLE transaction_metering MODIFY application_id VARCHAR2(4000);

-- Create metering parameters
CREATE TABLE metering_parameters (
    ledger_metering_end VARCHAR2(4000),
    ledger_metering_timestamp NUMBER NOT NULL
);

-- Create participant metering
CREATE TABLE participant_metering (
    application_id VARCHAR2(4000) NOT NULL,
    from_timestamp NUMBER NOT NULL,
    to_timestamp NUMBER NOT NULL,
    action_count NUMBER NOT NULL,
    ledger_offset VARCHAR2(4000) NOT NULL
);

CREATE UNIQUE INDEX participant_metering_from_to_application ON participant_metering(from_timestamp, to_timestamp, application_id);


