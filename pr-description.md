## Summary

Improves `daml test` output to be more readable and actionable:

- Buffer diagnostics so test summary prints before trace output
- Add pass/fail counts to summary header
- Hide passing tests when there are failures (focus on what needs fixing)
- Auto-detect TTY for colored output
- Only show coverage stats when `--show-coverage` is passed
- Show package name in header, relative paths in summary, absolute paths in diagnostics
- Group test results by file with cleaner formatting

Related to #23132 but takes a different approach for some aspects.

## Test plan

- [x] Build passes
- [ ] Manual testing with passing/failing test suites
- [ ] Verify colored output in TTY, plain in pipes
