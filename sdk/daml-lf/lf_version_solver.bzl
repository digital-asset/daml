# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
LF Version Solver

Instead of hardcoding LF versions in test targets, tests declare their *needs*
along three dimensions. A solver then resolves these declarations into concrete
LF versions based on the CI context (PR, main, nightly).

## Dimensions

1. **Feature requirements** (`features`):
   A list of feature names (from daml-lf-features.json). The solver
   computes the intersection of compatible LF versions. If empty, all versions
   are compatible.

2. **Runtime** (`runtime`):
   One of: "short", "long"
   - "short": cheap to run, fine to test with many versions
   - "long": expensive, solver aggressively prunes versions

3. **Regression relevance** (`regression`):
   One of: "yes", "no", "always"
   - "yes": important to test across LF versions, profile/runtime determine how many
   - "no": only needs latest_stable / staging at most
   - "always": run ALL compatible versions regardless of profile/runtime

## Solver profiles

The profile is a global CI configuration, not a per-target setting.

- **pr**: pre-merge, balance coverage with speed (default)
- **main**: post-merge, broader coverage
- **nightly**: full matrix (currently unused)
- **quick**: minimal testing, latest staging only (for local dev iteration)

Each profile defines a strategy for how many versions to select from the
compatible set.
"""

load("//daml-lf:daml-lf.bzl", "ALL_LF_VERSIONS_STRUCT", "DEV_LF_VERSION_STRUCT", "LATEST_STABLE_LF_VERSION_STRUCT", "STAGING_LF_VERSION_STRUCT", "parse_version")
load("@daml_features_data//:data.bzl", _FEATURES_DATA = "DATA")

# ---------------------------------------------------------------------------
# Version ordering — works on parsed version structs (.major, .minor)
# ---------------------------------------------------------------------------

def _version_key(v):
    """
    Returns a sortable key for a parsed version struct.
    Replicates the Haskell Ord MinorVersion:
      Staging(3) < Stable(3) < Staging(4) < Stable(4) < ... < Dev
    We encode this as (major, minor_number, status_rank) where:
      staging(n) -> (major, n, 0)
      stable(n)  -> (major, n, 1)
      dev        -> (major, 9999, 2)
    """
    major = int(v.major)
    if v.minor == "dev":
        return (major, 9999, 2)
    minor = int(v.minor)
    if v.status == "staging":
        return (major, minor, 0)
    else:
        return (major, minor, 1)

def _version_ge(a, b):
    return _version_key(a) >= _version_key(b)

def _version_le(a, b):
    return _version_key(a) <= _version_key(b)

def _version_eq(a, b):
    return _version_key(a) == _version_key(b)

# ---------------------------------------------------------------------------
# Feature data (parsed once at load time from @daml_features_data)
# ---------------------------------------------------------------------------

def _parse_features():
    """
    Parse raw feature JSON data into a dict of:
      feature_name -> struct(empty=bool, low=version_struct|None, high=version_struct|None)
    """
    result = {}
    for feat_name, feat_data in _FEATURES_DATA.items():
        vr = feat_data["versionRange"]
        if vr.get("type") == "Empty":
            result[feat_name] = struct(empty = True, low = None, high = None)
        else:
            result[feat_name] = struct(
                empty = False,
                low = parse_version(vr["lowerBound"]) if "lowerBound" in vr else None,
                high = parse_version(vr["upperBound"]) if "upperBound" in vr else None,
            )
    return result

_FEATURES = _parse_features()

# ---------------------------------------------------------------------------
# Feature compatibility
# ---------------------------------------------------------------------------

def _compatible_versions(features):
    """
    Given a list of feature names, return the subset of ALL_LF_VERSIONS_STRUCT that
    satisfy ALL feature requirements (intersection of ranges).
    """
    compatible = list(ALL_LF_VERSIONS_STRUCT)

    for feat in features:
        if feat not in _FEATURES:
            fail("Unknown feature: {}. Available features: {}".format(
                feat,
                ", ".join(sorted(_FEATURES.keys())),
            ))
        f = _FEATURES[feat]

        if f.empty:
            return []

        compatible = [
            v
            for v in compatible
            if (not f.low or _version_ge(v, f.low)) and (not f.high or _version_le(v, f.high))
        ]

    return compatible

# ---------------------------------------------------------------------------
# Named version resolution with safe fallback
# ---------------------------------------------------------------------------

def _safe_resolve(named_version, compatible):
    """
    Resolve a named version against the compatible set.
    If the named version is compatible, return it.
    Otherwise, return the highest compatible version (fallback).
    """
    for v in compatible:
        if _version_eq(v, named_version):
            return v
    return compatible[-1]

def _dedup(versions):
    """Remove duplicates (by version key) while preserving order."""
    seen = {}
    result = []
    for v in versions:
        k = _version_key(v)
        if k not in seen:
            seen[k] = True
            result.append(v)
    return result

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

def _apply_strategy(strategy, compatible):
    """
    Given a strategy name and a list of compatible version dicts, return the
    selected subset. Guarantees at least one version is returned.
    """
    if not compatible:
        fail("No compatible LF versions found for the given feature requirements. " +
             "This likely means the feature range excludes all known versions.")

    if strategy == "all":
        return compatible

    if strategy == "stable_only":
        return _dedup([_safe_resolve(LATEST_STABLE_LF_VERSION_STRUCT, compatible)])

    if strategy == "staging_only":
        return _dedup([_safe_resolve(STAGING_LF_VERSION_STRUCT, compatible)])

    if strategy == "stable_and_staging":
        return _dedup([
            _safe_resolve(LATEST_STABLE_LF_VERSION_STRUCT, compatible),
            _safe_resolve(STAGING_LF_VERSION_STRUCT, compatible),
        ])

    if strategy == "stable_staging_dev":
        return _dedup([
            _safe_resolve(LATEST_STABLE_LF_VERSION_STRUCT, compatible),
            _safe_resolve(STAGING_LF_VERSION_STRUCT, compatible),
            _safe_resolve(DEV_LF_VERSION_STRUCT, compatible),
        ])

    if strategy == "bookends":
        if len(compatible) <= 2:
            return compatible
        return [compatible[0], compatible[-1]]

    fail("Unknown strategy: {}".format(strategy))

# Solver profiles: each maps (runtime, regression) → strategy
# The profile is a global CI configuration, not a per-target setting.
# - "pr": pre-merge, balance coverage with speed (default)
# - "main": post-merge, broader coverage
_PROFILES = {
    "quick": {
        ("short", "yes"): "staging_only",
        ("short", "no"): "staging_only",
        ("long", "yes"): "staging_only",
        ("long", "no"): "staging_only",
    },
    "pr": {
        ("short", "yes"): "stable_staging_dev",
        ("short", "no"): "stable_and_staging",
        ("long", "yes"): "stable_and_staging",
        ("long", "no"): "staging_only",
    },
    "main": {
        ("short", "yes"): "all",
        ("short", "no"): "stable_staging_dev",
        ("long", "yes"): "stable_staging_dev",
        ("long", "no"): "staging_only",
    },
    # Currently unused, but reserved for nightly CI runs
    "nightly": {
        ("short", "yes"): "all",
        ("short", "no"): "all",
        ("long", "yes"): "all",
        ("long", "no"): "all",
    },
}

def solve_lf_versions(
        features = [],
        runtime = "short",
        regression = "yes",
        override_versions = None):
    """
    Returns all compatible LF versions, each annotated with tags indicating
    which CI profiles should run them.

    The result is a list of structs, each with all fields from the parsed
    version (.dotted, .mangled_damlc, .major, .minor, .status) plus:
      .tags: list of strings to add to the target's tags

    Targets that should only run on main (not PRs) get tagged "main-only",
    which the existing CI tag filter already excludes on PR builds.

    Args:
        features: List of feature names required by the test.
        runtime: "short" or "long".
        regression: "yes", "no", or "always".
        override_versions: Explicit version struct list. Validated against features.

    Returns:
        List of version structs augmented with .tags field.
    """

    compatible = _compatible_versions(features)

    if override_versions:
        for ov in override_versions:
            found = False
            for cv in compatible:
                if _version_eq(ov, cv):
                    found = True
                    break
            if not found:
                fail("Override version {} is not compatible with declared features. " +
                     "Compatible versions: {}".format(
                         ov.dotted if hasattr(ov, "dotted") else str(ov),
                         ", ".join([c.dotted for c in compatible]),
                     ))
        return [_tag_version(v, []) for v in override_versions]

    if not compatible:
        fail("No compatible LF versions found for the given feature requirements.")

    # Compute which versions each profile would select
    pr_versions = _apply_strategy(
        _PROFILES["pr"].get((runtime, regression if regression != "always" else "yes"), "all"),
        compatible,
    )
    main_versions = _apply_strategy(
        _PROFILES["main"].get((runtime, regression if regression != "always" else "yes"), "all"),
        compatible,
    )

    # If regression="always", all compatible versions are included;
    # otherwise only main_versions are included
    if regression == "always":
        all_versions = compatible
    else:
        all_versions = main_versions

    result = []
    for v in all_versions:
        tags = []
        in_pr = _version_in_list(v, pr_versions)
        if not in_pr:
            tags.append("main-only")
        result.append(_tag_version(v, tags))

    return result

def _version_in_list(v, versions):
    """Check if version v is in the list of versions (by key equality)."""
    for other in versions:
        if _version_eq(v, other):
            return True
    return False

def _tag_version(v, tags):
    """Augment a version struct with a .tags field."""
    return struct(
        dotted = v.dotted,
        mangled_damlc = v.mangled_damlc,
        mangled_java = v.mangled_java,
        major = v.major,
        minor = v.minor,
        status = v.status,
        tags = tags,
    )
