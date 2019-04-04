
DAML Studio DA INTERNAL Changelog
---------------------------------

### 2017-08-29

- Slightly improved parser error messages

### 2017-08-16

- Improved automatic bracket matching and quotation mark insertion.
- Added support for "snippets" in VS Code. (PROD-7827)

### 2017-06-23

- Fixed crash in typechecker with an unannotated choice variable
- Fix obligables on hover with sugared (non-lambda) template declarations

### 2017-05-30 - Sprint 32

- minor documentation tweaks before first customer release

### 2017-05-23 - Sprint 32

- implemented obligable party computation for core DAML (PROD-5728)
- improved line-breaks when formatting record types (PROD-6258)
- `type` and `newtype` are now keywords and also properly highlighted in DAML Studio (PROD-6257)
- changed license text to the short form disclaimer

### 2017-05-03 - Sprint 30

- Refactored closure computation to fix an issue causing flood of
  "Module not found" errors on incomplete or invalid import statements
  (PROD-5477)

### 2017-04-28 - Sprint 30

- Minor bug fix for crashing DAML interpretation in numerical operations
  due to accidentally left-over debugging code.

### 2017-04-18 - Sprint 29

- DAML code formatting (⇧⌥F) - DAML code can now be formatted by the compiler.
- Scenario diagnostic computation was refactored and timeouts were added to
  recover from bugs causing long computations and unresponsiveness (PROD-5715).
- New DAML features such as lists and variants are now usable from DAML Studio,
  but may break the legacy obligables computation.

### 2017-03-31 - Sprint 28

- Upgraded language client libraries to latest version, fixing issues
  related to symbol renaming and issues arising from closing of files.
  (PROD-5206, PROD-3806).
- Fixed clearing of diagnostics when files are closed. Previously diagnostics
  were cleared when file was closed, regardless if it was referenced through
  other files (PROD-5236).
- DAML User Guide was updated, with many fixes to the tutorial section. The
  tutorial files are now also checked for correctness in CI.

### 2017-03-07 - Sprint 26

- Swapped out legacy type checker for bi-directional type checker.
  The types of top-level declarations are no longer annotated with
  their argument names. We will regain similar functionality in a future
  release of DAML once labelled record types have been added to the language.
  Also, for now, the hovers on constants and variables names are not available.
  These will be available in a future sprint.

### 2017-02-03 - Sprint 24

- Integrated DAML Core scenario interpreter.
- Fixed a memory leak in DAML Core translation.
- Fixed a bug in caching causing recomputation when changed content of
  a file resulted in same result.
- Temporarily disabled the DAML language server instrumentation due to
  high overhead on large projects.

### 2017-01-06 - Sprint 22

- Symbol Renaming - now supports: choices, record fields, modules
- Go to Symbol (⇧⌘O) - now supports: choices, record fields, modules
- Show all Symbols (⌘T) - now supports: choices, record fields, modules
- Added an item to the statusbar of DAML Studio to show the progress of the background computations
  needed for all the information DAML Studio provides.
- Fixed an issue with static assets not being loaded when using `damli demo` from the `damli`
  installed by the DAML Studio installation script.

### 2016-12-15 - Sprint 21

- Changed the polling for changed files on the file system to 5 seconds (up from 2 seconds).
- Go to Symbol (⇧⌘O) - symbols not supported yet: choices, record fields, modules
- Show all Symbols (⌘T) - symbols not supported yet: choices, record fields, modules
- Suggest alternatives on failed name resolution. If the compiler can't find a variable you are
  referencing, it will suggest an alternative from the names in scope that looks similar. This
  should help with typos in names.
- Fixed some general performance issues in the IDE.

### 2016-12-01 - Sprint 20

- Added warnings on unused variables in the IDE since those often point to
  bugs. There will be no warnings on top-level declarations since we don't have
  explicit exports yet and you can suppress of the warning by adding an
  underscore (`_`) in front of the variable.

- Added warnings for variable shadowing. Core translation disallows
  variable shadowing and these warnings will be turned into errors in
  future releases.

- Added support for symbol renaming. Press `F2` on a symbol to get
  a small input view where you can change the name of the symbol.
  There is a known bug at the moment, where if the renamed symbol is also
  referenced in another file it renames it, but there will be a warning
  on the file. You can fix the warning by manually changing the file in question.
  The VS Code team has already fixed the bug and we just need to wait for it
  to reach the stable VS Code version.

- Changed labels of 'obls' and 'ctrls' in the obligable computation to
  'obligable' and 'controlling' respectively.

- Added support for viewing the output of DAML Core translation.

- Fixed `[DAML Core]` command not working from the command palette.

### 2016-11-04 - Sprint 18

We implemented various performance improvements and support for performance
monitoring. More precisely:

- Added instrumentation for DAML language server.
  NOTE: You may need to accept incoming connections for the
  "damli" executable.
- Obligable computation now requires typechecking to pass.
- Added truncation of large diagnostic messages and log output
  to avoid adverse performance impact towards Visual Studio Code.
- DAML lexer was made lazy to allow for partial parsing and significantly
  improve performance of computing the compilation unit.

### 2016-10-05 - Sprint 15

- Added support for scenario interpretation results. A code lens
  is shown on scenarios and clicking on it will open a preview window
  showing scenario interpretation results.
- Added support for `TODO` and `FIXME`in comments.
- Added syntax highlighting in annotations (e.g.: `{@ DESC "hello" <> toText myVar @}`).
- Nested comments cannot be highlighted by the `RegEx` engine in VS Codes syntax highlighting
  Note: Line comments can easily be added by selecting a block of code and
  pressing `Ctrl-K+Ctrl-C`. To uncomment use `Ctrl-K+Ctrl-U`.
- Support for compilation units through a 'LibraryModules.daml'. See the DAML User's Guide
  for more information.
- Added support for monitoring changes on external files.
- Bundled the DAML User's Guide in the release and added a editor title button for
  opening it.
- Added the obligable computation on top-level declarations on hover.
- Much more precise locations on hover and when reporting errors.
- Fixed an issue with damli process shutdown, which may have left multiple
  language server processes running in the background.

### 2016-09-20 - Sprint 14 - Beta Release

- Added support for type on hover: hovering over a symbol shows its type
  information.
- Improved error messages
- Fixed stability issues with cyclic imports
- Added support for reporting scenario interpretation errors

### 2016-09-09 - Sprint 13 - Alpha Release

First release of DAML Studio for internal use at Digital Asset.
