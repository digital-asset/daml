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
   One of: "short", "long", "exceptional"
   - "short": cheap to run, fine to test with many versions
   - "long": expensive, solver aggressively prunes versions
   - "exceptional": test manages its own version list (escape hatch)

3. **Regression relevance** (`regression`):
   One of: "yes", "no", "override"
   - "yes": important to test across LF versions (e.g. serialization compat)
   - "no": only needs latest_stable / staging at most
   - "override": test provides an explicit version list (escape hatch)

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

# TODO: configure this via --define or a Bazel config setting in CI
_DEFAULT_PROFILE = "pr"

def solve_lf_versions(
        features = [],
        runtime = "short",
        regression = "yes",
        profile = _DEFAULT_PROFILE,
        override_versions = None):
    """
    Main entry point. Returns a list of LF version structs.

    Args:
        features: List of feature names required by the test.
        runtime: "short", "long", or "exceptional".
        regression: "yes", "no", or "override".
        profile: "quick", "pr", "main", or "nightly" (global CI config, not per-target).
        override_versions: Explicit version struct list, bypasses the solver.

    Returns:
        List of parsed version structs.
    """

    if override_versions:
        return override_versions

    compatible = _compatible_versions(features)

    profile_map = _PROFILES.get(profile)
    if not profile_map:
        fail("Unknown profile: {}".format(profile))

    key = (runtime, regression)
    strategy = profile_map.get(key)
    if not strategy:
        fail("No strategy for ({}, {}) in profile {}".format(runtime, regression, profile))

    return _apply_strategy(strategy, compatible)

# ---------------------------------------------------------------------------
# Public macro for declaring versioned test targets
# ---------------------------------------------------------------------------

def lf_versioned_test(
        target_fn,
        features = [],
        runtime = "short",
        regression = "yes",
        override_versions = None):
    """
    Creates one test target per solved LF version by calling target_fn for each.

    Args:
        target_fn: A function (version_struct) -> None that creates the rule.
            The version struct has fields like .dotted, .mangled_damlc, .major, .minor, .status.
        features: Feature requirements.
        runtime: "short" | "long" | "exceptional".
        regression: "yes" | "no" | "override".
        override_versions: Explicit version struct list, bypasses the solver.
    """

    versions = solve_lf_versions(
        features = features,
        runtime = runtime,
        regression = regression,
        override_versions = override_versions,
    )

    for version in versions:
        target_fn(version)
