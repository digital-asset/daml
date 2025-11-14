#!/usr/bin/env python3
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import yaml
import sys
import os

def to_scala_version(v):
    """
    Converts the YAML version dict to a Scala LanguageVersion constructor string.

    Input:
    {
        'versionMajor': 'V2',
        'versionMinor': {'contents': 1, 'tag': 'PointStable'}
    }

    Output:
    "LanguageVersion(Major.V2, Minor.Stable(1))"
    """
    major = v["versionMajor"]
    minor_tag = v["versionMinor"]["tag"]
    minor_contents = v["versionMinor"]["contents"]

    # e.g., "Major.V2"
    scala_major = f"Major.{major}"

    # e.g., "Minor.Stable(1)"
    if minor_tag == "PointStable":
        scala_minor = f"Minor.Stable({minor_contents})"
    # Add other cases like 'Dev' or 'Staging' if they ever appear in this file
    # elif minor_tag == "Dev":
    #     scala_minor = "Minor.Dev"
    else:
        raise ValueError(f"Unsupported minor tag in YAML: {minor_tag}")

    return f"LanguageVersion({scala_major}, {scala_minor})"

def generate_file_content(list_entries_str):
    """
    Wraps the generated list of tuples in the full Scala trait template.
    """
    return f"""// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//
// THIS FILE IS-GENERATED FROM stable-packages.yaml
// DO NOT EDIT MANUALLY
//

package com.digitalasset.daml.lf
package language

import com.digitalasset.daml.lf.language.LanguageVersion._
import com.digitalasset.daml.lf.language.LanguageVersion.Major._
import com.digitalasset.daml.lf.language.LanguageVersion.Minor._
import com.digitalasset.daml.lf.data.Ref // Assuming PackageId is Ref.PackageId

trait StablePackagesGenerated {{

  /**
   * A complete list of all stable package IDs and their associated language version.
   * This list is parsed directly from stable-packages.yaml at build time.
   */
  val allStablePackages: List[(LanguageVersion, Ref.PackageId)] = List(
{list_entries_str}
  )

  /** A list of all stable package IDs, regardless of version. */
  lazy val allStablePackageIds: List[Ref.PackageId] = allStablePackages.map(_._2)

  /** A map grouping stable package IDs by their language version. */
  lazy val stablePackagesByVersion: Map[LanguageVersion, List[Ref.PackageId]] =
    allStablePackages.groupBy(_._1).view.mapValues(_.map(_._2)).toMap

  /**
   * Returns a list of stable package IDs for a specific language version.
   * Mirrors the Haskell function `stablePackagesForVersion`.
   */
  def stablePackagesForVersion(v: LanguageVersion): List[Ref.PackageId] =
    stablePackagesByVersion.getOrElse(v, List.empty)

}}
"""

def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <input.yaml> <output.scala>", file=sys.stderr)
        sys.exit(1)

    input_yaml_path = sys.argv[1]
    output_scala_path = sys.argv[2]

    # 1. Read and Parse YAML
    print(f"Reading {input_yaml_path}...")
    with open(input_yaml_path, 'r') as f:
        # We use safe_load to parse the YAML data
        data = yaml.safe_load(f)

    # 2. Transform Data to Scala list entries
    scala_list_entries = []
    for entry in data:
        # Each entry is a 2-item list: [ {version_dict}, "package_id_hash" ]
        version_dict = entry[0]
        package_id = entry[1]

        # Convert to Scala code strings
        scala_version_str = to_scala_version(version_dict)
        scala_pkgid_str = f'Ref.PackageId.assertFromString("{package_id}")'

        # Format as a tuple entry in the list
        scala_list_entries.append(
            f"    ({scala_version_str}, {scala_pkgid_str})"
        )

    # Join all entries with commas and newlines
    list_content = ",\n".join(scala_list_entries)

    # 3. Generate final Scala file content
    output_scala_code = generate_file_content(list_content)

    # 4. Write output file
    # Ensure the directory exists
    os.makedirs(os.path.dirname(output_scala_path), exist_ok=True)
    with open(output_scala_path, 'w') as f:
        f.write(output_scala_code)
    print(f"Successfully generated {output_scala_path}")

if __name__ == "__main__":
    main()
