// Copyright (c) 2020, Digital Asset (Switzerland) GmbH and/or its affiliates.
// All rights reserved.

/** Iso8601 encoded string */
export type Time = string;

export type TimeType = "static" | "wallclock" | "simulated";

export type CommandId = string;

export type Party = string;

export { DamlLfValue, DamlLfValueRecord } from "./DamlLfValue";

export { DamlLfType, DamlLfDataType } from "./DamlLfType";
