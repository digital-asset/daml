#!/usr/bin/env python3
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import sys

# --- Helpers for raw version definitions

def to_scala_var(v):
    """Converts a version dict to a Scala variable definition name, e.g., v2_1."""
    # {"major": "2", "minor": "dev"} -> "v2_dev"
    return "v{}_{}".format(v["major"], v["minor"])

def to_scala_major(major_str):
    """Converts a major version string to a Scala MajorVersion constructor."""
    # This assumes Major.V2 is imported via `import ...Major._`
    if major_str == "1":
        return "V1"
    if major_str == "2":
        return "V2"
    raise ValueError("Unsupported Major Version: {}".format(major_str))

def to_scala_minor(v):
    """Converts a version dict to a Scala MinorVersion constructor."""
    # This assumes Minor.Stable, etc. are imported via `import ...Minor._`
    status = v["status"]
    minor = v["minor"]
    if status == "stable":
        return f"Stable({int(minor)})"
    elif status == "staging":
        return f"Staging({int(minor)})"
    elif status == "dev":
        return "Dev"
    raise ValueError(f"Unsupported Status: {status}")

# --- Generic generators ---

def generate_scala_list(name, version_dicts):
    """Generates a Scala List definition from raw version dicts."""
    refs = [to_scala_var(v) for v in version_dicts]
    return [
        f"  val {name}: List[LanguageVersion] = List({', '.join(refs)})",
    ]

def generate_scala_singleton(name, version_dict):
    """Generates a Scala singleton definition from a raw version dict."""
    ref = to_scala_var(version_dict)
    return [
        f"  val {name}: LanguageVersion = {ref}",
    ]

# --- Static File Parts ---

# This includes everything from the top of the file down to the
# end of the hardcoded legacy section.
STATIC_HEADER = """// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//
// THIS FILE IS-GENERATED FROM //daml-lf/language/daml-lf.bzl
// DO NOT EDIT MANUALLY
//

package com.digitalasset.daml.lf
package language

import com.digitalasset.daml.lf.language.LanguageVersion.Major._
import com.digitalasset.daml.lf.language.LanguageVersion.Minor._

import scala.annotation.nowarn

trait LanguageVersionGenerated {
  val allStableLegacyLfVersions: List[LanguageVersion] =
    List(6, 7, 8, 11, 12, 13, 14, 15, 17).map(i => LanguageVersion(V1, Stable(i)))
  val List(v1_6, v1_7, v1_8, v1_11, v1_12, v1_13, v1_14, v1_15, v1_17) = allStableLegacyLfVersions: @nowarn(
    "msg=match may not be exhaustive"
  )
  val v1_dev: LanguageVersion = LanguageVersion(V1, Dev)
  val allLegacyLfVersions: List[LanguageVersion] = allStableLegacyLfVersions.appended(v1_dev)
"""

# This includes the hardcoded ranges, Features objects, and the
# closing brace for the trait.
STATIC_FOOTER = """
  //ranges hardcoded (for now)
  val allLfVersionsRange = VersionRange(v2_1, v2_dev)
  val stableLfVersionsRange = VersionRange(v2_1, v2_2)
  val earlyAccessLfVersionsRange = VersionRange(v2_1, v2_2)

  //features hardcoded (for now)
  object Features {
    val default = v2_1
    val packageUpgrades = v2_1

    val flatArchive = v2_2
    val kindInterning = flatArchive
    val exprInterning = flatArchive

    val explicitPkgImports = v2_2

    val choiceFuncs = v2_dev
    val choiceAuthority = v2_dev

    /** TYPE_REP_TYCON_NAME builtin */
    val templateTypeRepToText = v2_dev

    /** Guards in interfaces */
    val extendedInterfaces = v2_dev

    /** BigNumeric */
    val bigNumeric = v2_dev

    val contractKeys = v2_dev

    val complexAnyType = v2_dev

    val cryptoUtility = v2_dev

    /** UNSAFE_FROM_INTERFACE is removed starting from 2.2, included */
    val unsafeFromInterfaceRemoved = v2_2

    /** Unstable, experimental features. This should stay in x.dev forever.
      * Features implemented with this flag should be moved to a separate
      * feature flag once the decision to add them permanently has been made.
      */
    val unstable = v2_dev
  }

  object LegacyFeatures {
    val default = v1_6
    val internedPackageId = v1_6
    val internedStrings = v1_7
    val internedDottedNames = v1_7
    val numeric = v1_7
    val anyType = v1_7
    val typeRep = v1_7
    val typeSynonyms = v1_8
    val packageMetadata = v1_8
    val genComparison = v1_11
    val genMap = v1_11
    val scenarioMustFailAtMsg = v1_11
    val contractIdTextConversions = v1_11
    val exerciseByKey = v1_11
    val internedTypes = v1_11
    val choiceObservers = v1_11
    val bigNumeric = v1_13
    val exceptions = v1_14
    val basicInterfaces = v1_15
    val choiceFuncs = v1_dev
    val choiceAuthority = v1_dev
    val natTypeErasure = v1_dev
    val packageUpgrades = v1_17
    val sharedKeys = v1_17
    val templateTypeRepToText = v1_dev
    val extendedInterfaces = v1_dev
    val unstable = v1_dev
  }
}
"""

def main(input_json_path, output_scala_path):
    with open(input_json_path, 'r') as f:
        data = json.load(f)

    output = [STATIC_HEADER]
    output.append("  // Start of code generated from //daml-lf/language/daml-lf.bzl\n")

    # 1. Handle the definitions from `allLfVersions`. This list contains
    # the full struct-like dicts.
    definitions = data.get("allLfVersions")
    if definitions:
        for v in definitions:
            var_name = to_scala_var(v)
            major = to_scala_major(v["major"])
            minor = to_scala_minor(v)
            output.append(f"  val {var_name}: LanguageVersion = LanguageVersion({major}, {minor})")
        output.append("") # Add a newline

    # 2. Loop over all other items, which are (lists of) raw version dicts.
    # The key is used directly as the variable name.
    for key, value in data.items():
        if key == "allLfVersions":
            # Sort the allLfVersions list to match Haskell
            value.sort(key=lambda v: (v['major'], v['status'] == 'dev', v['minor']))

        if isinstance(value, list):
            # This is for lists like stableLfVersions
            output.extend(generate_scala_list(key, value))
        elif isinstance(value, dict):
            # This is for singletons like defaultLfVersion
            output.extend(generate_scala_singleton(key, value))

    output.append(STATIC_FOOTER)

    with open(output_scala_path, 'w') as f:
        f.write("\n".join(output))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <input.json> <output.scala>", file=sys.stderr)
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
