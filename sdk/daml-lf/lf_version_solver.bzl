# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
LF Version Solver

Instead of hardcoding LF versions in test targets, tests declare their *needs*
along three dimensions. A solver then resolves these declarations into concrete
LF versions based on the CI context (local, PR, main, nightly).

## Dimensions

1. **Feature requirements** (`features`):
   A list of feature names (from daml-lf.bzl feature definitions). The solver
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
   - "no": only needs latest_stable / dev at most
   - "override": test provides an explicit version list (escape hatch)


## Solver profiles

The solver is parameterized by a *profile* that corresponds to the CI context:

- **local**: developer iteration, maximize speed
- **pr**: pre-merge, balance coverage with speed
- **main**: post-merge, broader coverage
- **nightly**: full matrix

Each profile defines a strategy for how many versions to select from the
compatible set.
"""

load("//daml-lf:daml-lf.bzl", "ALL_LF_VERSIONS", "DEV_LF_VERSION", "LATEST_STABLE_LF_VERSION", "STAGING_LF_VERSION")

# ---------------------------------------------------------------------------
# Feature registry helpers
# ---------------------------------------------------------------------------

def _load_feature_ranges():
    """
    Returns a dict: feature_name -> struct(low, high) of dotted version strings.
    Built from the features_definitions in daml-lf.bzl.

    NOTE: In a real implementation this would import from daml-lf.bzl directly.
    For now we maintain a parallel mapping. The eventual goal is to have
    daml-lf.bzl expose this as a public symbol.
    """

    # This is a placeholder; the real implementation would derive from
    # _data.features in daml-lf.bzl. For demonstration we hardcode the
    # current state:
    return {
        "featureExceptions": {"low": "2.1"},
        "featureFlatArchive": {"low": "2.2"},
        "featurePackageImports": {"low": "2.2"},
        "featureContractKeys": {"low": "2.3"},
        "featureFetchByKey": {"low": "2.3"},
        "featureExerciseByKey": {"low": "2.3"},
        "featureNUCK": {"low": "2.3"},
        "featureExtendedCryptoPrimitives": {"low": "2.3"},
        "featurePackageUpgrades": {"low": "2.1"},
        "featureUnsafeFromInterface": {"high": "2.1"},
        "featureUnstable": {"low": "2.dev", "high": "2.dev"},
        "featureTextMap": {"low": "2.dev", "high": "2.dev"},
        "featureBigNumeric": {"low": "2.dev", "high": "2.dev"},
        "featureExtendedInterfaces": {"low": "2.dev", "high": "2.dev"},
        "featureChoiceAuthority": {"low": "2.dev", "high": "2.dev"},
        "featureComplexAnyType": {"low": "2.dev", "high": "2.dev"},
        "featureExperimental": {"low": "2.dev", "high": "2.dev"},
    }

# ---------------------------------------------------------------------------
# Version ordering
# ---------------------------------------------------------------------------

def _version_key(v):
    """Returns a sortable key for an LF version string."""
    if v.endswith(".dev"):
        return (int(v.split(".")[0]), 9999)
    parts = v.split(".")
    return (int(parts[0]), int(parts[1]))

def _version_ge(a, b):
    return _version_key(a) >= _version_key(b)

def _version_le(a, b):
    return _version_key(a) <= _version_key(b)

# ---------------------------------------------------------------------------
# Core solver
# ---------------------------------------------------------------------------

def _compatible_versions(features, all_versions):
    """
    Given a list of feature names, return the subset of `all_versions` that
    satisfy ALL feature requirements (intersection of ranges).
    """
    feature_ranges = _load_feature_ranges()
    compatible = list(all_versions)

    for feat in features:
        if feat not in feature_ranges:
            fail("Unknown feature: {}".format(feat))
        r = feature_ranges[feat]
        low = r.get("low")
        high = r.get("high")
        compatible = [
            v
            for v in compatible
            if (not low or _version_ge(v, low)) and (not high or _version_le(v, high))
        ]

    return compatible

# ---------------------------------------------------------------------------
# Named version resolution with safe fallback
# ---------------------------------------------------------------------------

# The "named versions" that strategies reference. These may alias each other
# (e.g. STAGING == LATEST_STABLE when there is no staging version).
_NAMED_VERSIONS = {
    "LATEST_STABLE": LATEST_STABLE_LF_VERSION,
    "STAGING": STAGING_LF_VERSION,
    "DEV": DEV_LF_VERSION,
}

def _safe_resolve(name, compatible):
    """
    Resolve a named version against the compatible set.
    If the named version is compatible, return it.
    Otherwise, return the highest compatible version (fallback).
    This guarantees we always return *something* from the compatible set.
    """
    version = _NAMED_VERSIONS[name]
    if version in compatible:
        return version

    # Fallback: highest compatible version
    return compatible[-1]

def _dedup(versions):
    """Remove duplicates while preserving order."""
    seen = {}
    result = []
    for v in versions:
        if v not in seen:
            seen[v] = True
            result.append(v)
    return result

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------
# Each strategy returns a deduplicated list of versions from the compatible set.
# Strategies are defined in terms of named version variables, with safe fallback.

def _apply_strategy(strategy, compatible, all_versions):
    """
    Given a strategy name and a list of compatible versions, return the
    selected subset. Guarantees at least one version is returned.

    Strategies reference named version variables (LATEST_STABLE, STAGING, DEV).
    When a named version is not in the compatible set, the solver falls back
    to the highest compatible version. Results are always deduplicated — if
    STAGING == LATEST_STABLE, only one test target is produced.
    """
    if not compatible:
        fail("No compatible LF versions found for the given feature requirements. " +
             "This likely means the feature range excludes all known versions.")

    if strategy == "all":
        return compatible

    if strategy == "stable_only":
        # Just LATEST_STABLE (safe)
        return _dedup([_safe_resolve("LATEST_STABLE", compatible)])

    if strategy == "staging_only":
        # Just STAGING (safe) — collapses to stable_only when no staging exists
        return _dedup([_safe_resolve("STAGING", compatible)])

    if strategy == "stable_and_staging":
        # LATEST_STABLE + STAGING (safe). If they're the same version, deduped to one.
        return _dedup([
            _safe_resolve("LATEST_STABLE", compatible),
            _safe_resolve("STAGING", compatible),
        ])

    if strategy == "stable_staging_dev":
        # All three named versions (safe, deduped)
        return _dedup([
            _safe_resolve("LATEST_STABLE", compatible),
            _safe_resolve("STAGING", compatible),
            _safe_resolve("DEV", compatible),
        ])

    if strategy == "bookends":
        # First and last compatible version (covers range boundaries)
        if len(compatible) <= 2:
            return compatible
        return [compatible[0], compatible[-1]]

    fail("Unknown strategy: {}".format(strategy))

# Solver profiles: each maps (runtime, regression) → strategy
_PROFILES = {
    "local": {
        # Developers want speed
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
    "nightly": {
        ("short", "yes"): "all",
        ("short", "no"): "all",
        ("long", "yes"): "all",
        ("long", "no"): "stable_staging_dev",
    },
}

def solve_lf_versions(
        features = [],
        runtime = "short",
        regression = "yes",
        profile = "main",
        override_versions = None):
    """
    Main entry point. Returns a list of LF version strings to test with.

    Args:
        features: List of feature names required by the test.
        runtime: "short", "long", or "exceptional".
        regression: "yes", "no", or "override".
        profile: "local", "pr", "main", or "nightly".
        override_versions: Explicit version list (used when regression="override"
                           or runtime="exceptional").

    Returns:
        List of LF version dotted strings.
    """

    if regression == "override" or runtime == "exceptional":
        if not override_versions:
            fail("override_versions must be provided when regression='override' or runtime='exceptional'")
        return override_versions

    all_versions = ALL_LF_VERSIONS
    compatible = _compatible_versions(features, all_versions)

    profile_map = _PROFILES.get(profile)
    if not profile_map:
        fail("Unknown profile: {}".format(profile))

    key = (runtime, regression)
    strategy = profile_map.get(key)
    if not strategy:
        fail("No strategy for ({}, {}) in profile {}".format(runtime, regression, profile))

    return _apply_strategy(strategy, compatible, all_versions)

# ---------------------------------------------------------------------------
# Public macro for declaring versioned test targets
# ---------------------------------------------------------------------------

def lf_versioned_test(
        rule_fn,
        name,
        features = [],
        runtime = "short",
        regression = "yes",
        override_versions = None,
        profile = "main",
        **kwargs):
    """
    Wrapper macro that creates one test target per solved LF version.

    Args:
        rule_fn: The underlying rule function (e.g. da_haskell_test).
        name: Base name for the test targets (version will be appended).
        features: Feature requirements.
        runtime: "short" | "long" | "exceptional".
        regression: "yes" | "no" | "override".
        override_versions: Explicit version list, bypasses the solver entirely.
        profile: Solver profile.
        **kwargs: Passed through to rule_fn.
    """


    versions = solve_lf_versions(
        features = features,
        runtime = runtime,
        regression = regression,
        profile = profile,
        override_versions = override_versions,
    )

    for version in versions:
        version_suffix = version.replace(".", "").replace("-", "")
        rule_fn(
            name = "{}-{}".format(name, version_suffix),
            args = kwargs.pop("args", []) + ["--daml-lf-version", version],
            **kwargs
        )

