package(default_visibility = ["//visibility:public"])

# Alias toolchain_type to io_bazel_rules_scala for Bazel BSP compatibility.
# BSP aspects reference @rules_scala but this repo uses @io_bazel_rules_scala.
alias(
    name = "toolchain_type",
    actual = "@io_bazel_rules_scala//scala:toolchain_type",
)
