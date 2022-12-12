// Copyright (c) 2020, Digital Asset (Switzerland) GmbH and/or its affiliates.
// All rights reserved.

// --------------------------------------------------------------------------------------------------------------------
// Identifier shortening
// --------------------------------------------------------------------------------------------------------------------

// The following definitions replicate (to a certain extent) the way in which Canton shortens identifiers.
// Specific pointers to the code we are referring to is provided alongside each definition.
// The replication is slightly unfortunate but this should be good enough for Navigator.
//
// See: https://github.com/digital-asset/canton/blob/6f75f64c4fbe054f9e2ec665a5f864d087782144/community/common/src/main/scala/com/digitalasset/canton/logging/pretty/PrettyInstances.scala

// See: https://github.com/digital-asset/canton/blob/6f75f64c4fbe054f9e2ec665a5f864d087782144/community/common/src/main/scala/com/digitalasset/canton/util/ShowUtil.scala#L31
const hashMaxLength: number = 12;

// See: https://github.com/digital-asset/canton/blob/6f75f64c4fbe054f9e2ec665a5f864d087782144/community/common/src/main/scala/com/digitalasset/canton/util/ShowUtil.scala#L62-L63
function shortenHash(str: string): string {
  return str.length > hashMaxLength
    ? `${str.substr(0, hashMaxLength)}...`
    : str;
}

function shortenPrefixedId(separator: string): (id: string) => string {
  return id => {
    const parts = id.split(separator);
    return parts.length === 2
      ? `${parts[0]}::${shortenHash(parts[1])}`
      : shortenHash(id);
  };
}

// Contract identifiers are assumed to be either a hash or some sort of opaque string
export const shortenContractId = shortenHash;

// The assumption is that party identifiers are either:
// - a participant-unique, readable identifier and an opaque string representing the namespace
//     - this is what happens in Canton
//     - keep the first part whole and shorten the namespace to 12 characters
// - some other opaque string
//     - just shorten the entire identifier to 12 characters
export const shortenPartyId = shortenPrefixedId("::");

export const shortenContractTypeId = shortenPrefixedId("@");

export function removePkgIdFromContractTypeId(id: string): string {
  const separator = "@";
  const parts = id.split(separator);
  return parts.length === 2 ? parts[0] : id;
}
