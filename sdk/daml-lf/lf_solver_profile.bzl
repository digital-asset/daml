# LF Version Solver profile configuration.
#
# To run in quick mode for local development, change this to "quick":
#   LF_SOLVER_PROFILE = "quick"
#
# Available profiles:
#   "quick"   - staging only, for fast local dev iteration
#   "pr"      - default, balance coverage with speed
#   "main"    - broader coverage for post-merge
#   "nightly" - full matrix
#
# CI overrides this file as needed for different pipeline stages.
LF_SOLVER_PROFILE = "pr"


