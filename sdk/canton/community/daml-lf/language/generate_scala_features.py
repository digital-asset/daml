#!/usr/bin/env python3
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import sys

# --- Helpers for raw version definitions ---
# These are needed to parse the version_req structs
# inside features.json

def to_scala_var(v):
    """Converts a version dict to a Scala variable definition name, e.g., v2_1."""
    return "v{}_{}".format(v["major"], v["minor"])

def to_scala_major(major_str):
    """Converts a major version string to a Scala MajorVersion constructor."""
    if major_str == "1":
        return "V1"
    if major_str == "2":
        return "V2"
    raise ValueError("Unsupported Major Version: {}".format(major_str))

def to_scala_minor(v):
    """Converts a version dict to a Scala MinorVersion constructor."""
    status = v["status"]
    minor = v["minor"]
    if status == "stable":
        return f"Stable({int(minor)})"
    elif status == "staging":
        return f"Staging({int(minor)})"
    elif status == "dev":
        return "Dev"
    raise ValueError(f"Unsupported Status: {status}")

# --- Helpers for feature definitions

def to_scala_version_req(req_dict):
    """Converts a version_req dict to a Scala VersionRange constructor."""
    low = req_dict.get("low")
    high = req_dict.get("high")

    if low and high:
        return f"VersionRange.Inclusive({to_scala_var(low)}, {to_scala_var(high)})"
    elif low:
        return f"VersionRange.From({to_scala_var(low)})"
    elif high:
        return f"VersionRange.Until({to_scala_var(high)})"
    else:
        # Note: Empty is now a case class, so it needs ()
        return "VersionRange.Empty()"

# --- Static File Parts ---

STATIC_HEADER = """// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//
// THIS FILE IS-GENERATED FROM //canton/community/daml-lf/language/daml-lf.bzl
// (via generate_features.py)
// DO NOT EDIT MANUALLY
//

package com.digitalasset.daml.lf
package language

import com.digitalasset.daml.lf.language.LanguageVersion.Feature

trait LanguageFeaturesGenerated extends LanguageVersionGenerated {

  // TODO(#30144): FEATURE: Remove this hardcoded object once V1 features are also generated
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
"""

STATIC_FOOTER = """
}
"""

def main(input_json_path, output_scala_path):
    with open(input_json_path, 'r') as f:
        features_data = json.load(f)

    output = [STATIC_HEADER]
    output.append("\n  // --- Generated V2 Features --- \n")

    all_feature_vars = []
    for feature in features_data:
        var_name = feature["name"]
        pretty_name = feature["name_pretty"]
        version_req_dict = feature["version_req"]

        scala_req = to_scala_version_req(version_req_dict)

        # Use json.dumps to safely escape strings for Scala
        scala_pretty_name = json.dumps(pretty_name)

        output.append(f"  val {var_name}: Feature = Feature(")
        output.append(f"    name = {scala_pretty_name},")
        output.append(f"    versionRange = {scala_req},")
        output.append( "  )")
        output.append( "") # newline

        all_feature_vars.append(var_name)

    # 4. Generate the allFeatures list
    output.append(f"  val allFeatures: List[Feature] = List({', '.join(all_feature_vars)})")

    output.append(STATIC_FOOTER)

    with open(output_scala_path, 'w') as f:
        f.write("\n".join(output))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <features.json> <output.scala>", file=sys.stderr)
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
