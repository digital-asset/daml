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

## Special modes

- `lf_version_self_managed`: test manages its own multi-version logic internally
  (e.g. comparing DARs of different LF versions). May still declare feature
  requirements for filtering, but produces a single target.

## Solver profiles

The solver is parameterized by a *profile* that corresponds to the CI context:

- **local**: developer iteration, maximize speed
- **pr**: pre-merge, balance coverage with speed
- **main**: post-merge, broader coverage
- **nightly**: full matrix

Each profile defines a strategy for how many versions to select from the
compatible set.
"""

load("//daml-lf:daml-lf.bzl", "ALL_LF_VERSIONS", "DEV_LF_VERSION", "LATEST_STABLE_LF_VERSION")

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

# Solver profiles: each maps (runtime, regression) → strategy function name
# Strategy: "all", "bookends", "latest_only", "dev_only", "latest_and_dev"

_PROFILES = {
    "local": {
        # Developers want speed
        ("short", "yes"): "latest_and_dev",
        ("short", "no"): "latest_only",
        ("long", "yes"): "latest_only",
        ("long", "no"): "latest_only",
    },
    "pr": {
        ("short", "yes"): "bookends",
        ("short", "no"): "latest_and_dev",
        ("long", "yes"): "latest_and_dev",
        ("long", "no"): "latest_only",
    },
    "main": {
        ("short", "yes"): "all",
        ("short", "no"): "latest_and_dev",
        ("long", "yes"): "bookends",
        ("long", "no"): "latest_only",
    },
    "nightly": {
        ("short", "yes"): "all",
        ("short", "no"): "all",
        ("long", "yes"): "all",
        ("long", "no"): "bookends",
    },
}

def _apply_strategy(strategy, compatible, all_versions):
    """
    Given a strategy name and a list of compatible versions, return the
    selected subset. Guarantees at least one version is returned.
    """
    if not compatible:
        fail("No compatible LF versions found for the given feature requirements. " +
             "This likely means the feature range excludes all known versions.")

    if strategy == "all":
        return compatible

    if strategy == "latest_only":
        # Pick the latest compatible version
        return [compatible[-1]]

    if strategy == "dev_only":
        dev = [v for v in compatible if v == DEV_LF_VERSION]
        return dev if dev else [compatible[-1]]

    if strategy == "latest_and_dev":
        # Latest compatible stable + dev (if compatible)
        result = []
        stable = [v for v in compatible if v != DEV_LF_VERSION]
        if stable:
            result.append(stable[-1])
        if DEV_LF_VERSION in compatible and DEV_LF_VERSION not in result:
            result.append(DEV_LF_VERSION)
        return result if result else [compatible[-1]]

    if strategy == "bookends":
        # First and last compatible version (covers range boundaries)
        if len(compatible) <= 2:
            return compatible
        return [compatible[0], compatible[-1]]

    fail("Unknown strategy: {}".format(strategy))

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
        lf_version_mode = "versioned",  # "versioned" or "self_managed"
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
        lf_version_mode: "versioned" | "self_managed".
        override_versions: Explicit versions for override/exceptional.
        profile: Solver profile.
        **kwargs: Passed through to rule_fn.
    """

    if lf_version_mode == "self_managed":
        # Single target; test handles versions internally
        rule_fn(name = name, **kwargs)
        return

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





