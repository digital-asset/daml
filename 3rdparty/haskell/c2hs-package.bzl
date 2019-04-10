package = (
struct(
  specVersionRaw = None,
  package = struct( pkgName = "c2hs", pkgVersion = "0.28.6", ),
  licenseRaw = None,
  licenseFiles = [ "COPYING", ],
  copyright =
    "Copyright (c) 1999-2007 Manuel M T Chakravarty\n2005-2013 Duncan Coutts\n2008      Benedikt Huber",
  maintainer =
    "chak@cse.unsw.edu.au, duncan@community.haskell.org, ian@skybluetrades.net, aditya.siram@gmail.com",
  author = "Manuel M T Chakravarty",
  stability = "Stable",
  testedWith =
    [ ( "ghc", "==6.12.3", ),
      ( "ghc", "==7.0.4", ),
      ( "ghc", "==7.6.1", ),
      ( "ghc", "==7.6.3", ),
      ( "ghc", "==7.8.3", ),
      ( "ghc", "==7.10.1", ),
    ],
  homepage = "https://github.com/haskell/c2hs",
  pkgUrl = "",
  bugReports = "https://github.com/haskell/c2hs/issues",
  sourceRepos =
    [ struct(
        repoKind = "RepoHead",
        repoType = "Git",
        repoLocation = "git://github.com/haskell/c2hs.git",
        repoModule = None,
        repoBranch = None,
        repoTag = None,
        repoSubdir = None,
      ),
    ],
  synopsis =
    "C->Haskell FFI tool that gives some cross-language type safety",
  description =
    "C->Haskell assists in the development of Haskell bindings to C\nlibraries. It extracts interface information from C header\nfiles and generates Haskell code with foreign imports and\nmarshaling. Unlike writing foreign imports by hand (or using\nhsc2hs), this ensures that C functions are imported with the\ncorrect Haskell types.",
  category = "Development",
  customFieldsPD = [ ],
  buildTypeRaw = "Simple",
  setupBuildInfo = None,
  library = None,
  subLibraries = [ ],
  executables =
    [ struct(
        exeName = "c2hs",
        modulePath = "Main.hs",
        exeScope = "public",
        buildInfo =
          struct(
            buildable = True,
            buildTools = [ ],
            buildToolDepends = [ ],
            cppOptions = [ ],
            asmOptions = [ ],
            cmmOptions = [ ],
            ccOptions = [ ],
            cxxOptions = [ ],
            ldOptions = [ ],
            pkgconfigDepends = [ ],
            frameworks = [ ],
            extraFrameworkDirs = [ ],
            asmSources = [ ],
            cmmSources = [ ],
            cSources = [ "src/C2HS/config.c", ],
            cxxSources = [ ],
            jsSources = [ ],
            hsSourceDirs = [ "src", ],
            otherModules =
              [ "C2HS.C",
                "C2HS.C.Attrs",
                "C2HS.C.Builtin",
                "C2HS.C.Info",
                "C2HS.C.Names",
                "C2HS.C.Trav",
                "C2HS.CHS",
                "C2HS.CHS.Lexer",
                "C2HS.Gen.Monad",
                "C2HS.Gen.Bind",
                "C2HS.Gen.Header",
                "C2HS.Gen.Wrapper",
                "C2HS.State",
                "C2HS.Switches",
                "C2HS.Config",
                "C2HS.Version",
                "Control.StateBase",
                "Control.State",
                "Control.StateTrans",
                "Data.Attributes",
                "Data.Errors",
                "Data.NameSpaces",
                "System.CIO",
                "Text.Lexers",
              ],
            virtualModules = [ ],
            autogenModules = [ ],
            defaultLanguage = "Haskell2010",
            otherLanguages = [ ],
            defaultExtensions = [ "ForeignFunctionInterface", ],
            otherExtensions = [ ],
            oldExtensions = [ ],
            extraLibs = [ ],
            extraGHCiLibs = [ ],
            extraBundledLibs = [ ],
            extraLibFlavours = [ ],
            extraLibDirs = [ ],
            includeDirs = [ ],
            includes = [ ],
            installIncludes = [ ],
            options =
              [ ( "ghc",
                  [ "-Wall", "-fno-warn-incomplete-patterns", "-fwarn-tabs", "-XNoMonadFailDesugaring"],
                ),
              ],
            profOptions = [ ],
            sharedOptions = [ ],
            staticOptions = [ ],
            customFieldsBI = [ ],
            targetBuildDepends =
              [ struct( name = "base", version = ">=2 && <5", ),
                struct( name = "bytestring", version = "-any", ),
                struct( name = "language-c", version = ">=0.7.1 && <0.9", ),
                struct( name = "filepath", version = "-any", ),
                struct( name = "dlist", version = "-any", ),
                struct( name = "base", version = ">=3", ),
                struct( name = "process", version = "-any", ),
                struct( name = "directory", version = "-any", ),
                struct( name = "array", version = "-any", ),
                struct( name = "containers", version = "-any", ),
                struct( name = "pretty", version = "-any", ),
              ],
            mixins = [ ],
          ),
      ),
      struct(
        exeName = "regression-suite",
        modulePath = "regression-suite.hs",
        exeScope = "public",
        buildInfo =
          struct(
            buildable = False,
            buildTools = [ ],
            buildToolDepends = [ ],
            cppOptions = [ ],
            asmOptions = [ ],
            cmmOptions = [ ],
            ccOptions = [ ],
            cxxOptions = [ ],
            ldOptions = [ ],
            pkgconfigDepends = [ ],
            frameworks = [ ],
            extraFrameworkDirs = [ ],
            asmSources = [ ],
            cmmSources = [ ],
            cSources = [ ],
            cxxSources = [ ],
            jsSources = [ ],
            hsSourceDirs = [ "tests", ],
            otherModules = [ ],
            virtualModules = [ ],
            autogenModules = [ ],
            defaultLanguage = "Haskell2010",
            otherLanguages = [ ],
            defaultExtensions = [ ],
            otherExtensions = [ ],
            oldExtensions = [ ],
            extraLibs = [ ],
            extraGHCiLibs = [ ],
            extraBundledLibs = [ ],
            extraLibFlavours = [ ],
            extraLibDirs = [ ],
            includeDirs = [ ],
            includes = [ ],
            installIncludes = [ ],
            options = [ ],
            profOptions = [ ],
            sharedOptions = [ ],
            staticOptions = [ ],
            customFieldsBI = [ ],
            targetBuildDepends = [ ],
            mixins = [ ],
          ),
      ),
    ],
  foreignLibs = [ ],
  testSuites =
    [ struct(
        testName = "test-bugs",
        testInterface =
          struct(
            type = "exitcode-stdio-1.0",
            version = "1.0",
            mainIs = "test-bugs.hs",
          ),
        testBuildInfo =
          struct(
            buildable = True,
            buildTools = [ struct( name = "c2hs", version = "-any", ), ],
            buildToolDepends = [ ],
            cppOptions = [ ],
            asmOptions = [ ],
            cmmOptions = [ ],
            ccOptions = [ ],
            cxxOptions = [ ],
            ldOptions = [ ],
            pkgconfigDepends = [ ],
            frameworks = [ ],
            extraFrameworkDirs = [ ],
            asmSources = [ ],
            cmmSources = [ ],
            cSources = [ ],
            cxxSources = [ ],
            jsSources = [ ],
            hsSourceDirs = [ "tests", ],
            otherModules = [ ],
            virtualModules = [ ],
            autogenModules = [ ],
            defaultLanguage = "Haskell2010",
            otherLanguages = [ ],
            defaultExtensions = [ ],
            otherExtensions = [ ],
            oldExtensions = [ ],
            extraLibs = [ ],
            extraGHCiLibs = [ ],
            extraBundledLibs = [ ],
            extraLibFlavours = [ ],
            extraLibDirs = [ ],
            includeDirs = [ ],
            includes = [ ],
            installIncludes = [ ],
            options = [ ],
            profOptions = [ ],
            sharedOptions = [ ],
            staticOptions = [ ],
            customFieldsBI = [ ],
            targetBuildDepends =
              [ struct( name = "base", version = "-any", ),
                struct( name = "filepath", version = "-any", ),
                struct( name = "test-framework", version = "-any", ),
                struct( name = "test-framework-hunit", version = "-any", ),
                struct( name = "HUnit", version = "-any", ),
                struct( name = "shelly", version = ">=1.0", ),
                struct( name = "text", version = "-any", ),
                struct( name = "transformers", version = "-any", ),
              ],
            mixins = [ ],
          ),
      ),
      struct(
        testName = "test-system",
        testInterface =
          struct(
            type = "exitcode-stdio-1.0",
            version = "1.0",
            mainIs = "test-system.hs",
          ),
        testBuildInfo =
          struct(
            buildable = True,
            buildTools = [ struct( name = "c2hs", version = "-any", ), ],
            buildToolDepends = [ ],
            cppOptions = [ ],
            asmOptions = [ ],
            cmmOptions = [ ],
            ccOptions = [ ],
            cxxOptions = [ ],
            ldOptions = [ ],
            pkgconfigDepends = [ ],
            frameworks = [ ],
            extraFrameworkDirs = [ ],
            asmSources = [ ],
            cmmSources = [ ],
            cSources = [ ],
            cxxSources = [ ],
            jsSources = [ ],
            hsSourceDirs = [ "tests", ],
            otherModules = [ ],
            virtualModules = [ ],
            autogenModules = [ ],
            defaultLanguage = "Haskell2010",
            otherLanguages = [ ],
            defaultExtensions = [ ],
            otherExtensions = [ ],
            oldExtensions = [ ],
            extraLibs = [ ],
            extraGHCiLibs = [ ],
            extraBundledLibs = [ ],
            extraLibFlavours = [ ],
            extraLibDirs = [ ],
            includeDirs = [ ],
            includes = [ ],
            installIncludes = [ ],
            options = [ ],
            profOptions = [ ],
            sharedOptions = [ ],
            staticOptions = [ ],
            customFieldsBI = [ ],
            targetBuildDepends =
              [ struct( name = "base", version = "-any", ),
                struct( name = "test-framework", version = "-any", ),
                struct( name = "test-framework-hunit", version = "-any", ),
                struct( name = "HUnit", version = "-any", ),
                struct( name = "shelly", version = ">=1.0", ),
                struct( name = "text", version = "-any", ),
                struct( name = "transformers", version = "-any", ),
              ],
            mixins = [ ],
          ),
      ),
    ],
  benchmarks = [ ],
  dataFiles = [ "C2HS.hs", ],
  dataDir = "",
  extraSrcFiles =
    [ "src/C2HS/config.h",
      "AUTHORS",
      "INSTALL",
      "README",
      "ChangeLog",
      "ChangeLog.old",
      "doc/c2hs.xml",
      "doc/c2hs.css",
      "doc/man1/c2hs.1",
      "doc/Makefile",
      "tests/system/calls/*.chs",
      "tests/system/calls/*.h",
      "tests/system/cpp/*.chs",
      "tests/system/enums/*.chs",
      "tests/system/enums/*.h",
      "tests/system/enums/*.c",
      "tests/system/marsh/*.chs",
      "tests/system/marsh/*.h",
      "tests/system/pointer/*.chs",
      "tests/system/pointer/*.h",
      "tests/system/pointer/*.c",
      "tests/system/simple/*.chs",
      "tests/system/simple/*.h",
      "tests/system/simple/*.c",
      "tests/system/sizeof/*.chs",
      "tests/system/sizeof/*.h",
      "tests/system/sizeof/*.c",
      "tests/system/structs/*.chs",
      "tests/system/structs/*.h",
      "tests/system/structs/*.c",
      "tests/system/Makefile",
      "tests/bugs/call_capital/*.chs",
      "tests/bugs/call_capital/*.h",
      "tests/bugs/call_capital/*.c",
      "tests/bugs/issue-7/*.chs",
      "tests/bugs/issue-7/*.h",
      "tests/bugs/issue-9/*.chs",
      "tests/bugs/issue-9/*.h",
      "tests/bugs/issue-9/*.c",
      "tests/bugs/issue-10/*.chs",
      "tests/bugs/issue-10/*.h",
      "tests/bugs/issue-10/*.c",
      "tests/bugs/issue-15/*.chs",
      "tests/bugs/issue-15/*.h",
      "tests/bugs/issue-15/*.c",
      "tests/bugs/issue-16/*.chs",
      "tests/bugs/issue-16/*.h",
      "tests/bugs/issue-16/*.c",
      "tests/bugs/issue-19/*.chs",
      "tests/bugs/issue-19/*.h",
      "tests/bugs/issue-19/*.c",
      "tests/bugs/issue-20/*.chs",
      "tests/bugs/issue-20/*.h",
      "tests/bugs/issue-20/*.c",
      "tests/bugs/issue-22/*.chs",
      "tests/bugs/issue-22/*.h",
      "tests/bugs/issue-22/*.c",
      "tests/bugs/issue-23/*.chs",
      "tests/bugs/issue-23/*.h",
      "tests/bugs/issue-23/*.c",
      "tests/bugs/issue-25/*.chs",
      "tests/bugs/issue-29/*.chs",
      "tests/bugs/issue-29/*.h",
      "tests/bugs/issue-30/*.chs",
      "tests/bugs/issue-30/*.h",
      "tests/bugs/issue-30/*.c",
      "tests/bugs/issue-31/*.chs",
      "tests/bugs/issue-31/*.h",
      "tests/bugs/issue-31/*.c",
      "tests/bugs/issue-32/*.chs",
      "tests/bugs/issue-32/*.h",
      "tests/bugs/issue-32/*.c",
      "tests/bugs/issue-36/*.chs",
      "tests/bugs/issue-36/*.h",
      "tests/bugs/issue-38/*.chs",
      "tests/bugs/issue-38/*.h",
      "tests/bugs/issue-38/*.c",
      "tests/bugs/issue-43/*.chs",
      "tests/bugs/issue-43/*.h",
      "tests/bugs/issue-43/*.c",
      "tests/bugs/issue-44/*.chs",
      "tests/bugs/issue-44/*.h",
      "tests/bugs/issue-44/*.c",
      "tests/bugs/issue-45/*.chs",
      "tests/bugs/issue-45/*.h",
      "tests/bugs/issue-45/*.c",
      "tests/bugs/issue-46/*.chs",
      "tests/bugs/issue-46/*.h",
      "tests/bugs/issue-46/*.c",
      "tests/bugs/issue-47/*.chs",
      "tests/bugs/issue-47/*.h",
      "tests/bugs/issue-47/*.c",
      "tests/bugs/issue-48/*.chs",
      "tests/bugs/issue-48/*.h",
      "tests/bugs/issue-48/*.c",
      "tests/bugs/issue-51/*.chs",
      "tests/bugs/issue-51/*.h",
      "tests/bugs/issue-51/*.c",
      "tests/bugs/issue-54/*.chs",
      "tests/bugs/issue-54/*.h",
      "tests/bugs/issue-54/*.c",
      "tests/bugs/issue-60/*.chs",
      "tests/bugs/issue-60/*.h",
      "tests/bugs/issue-60/*.c",
      "tests/bugs/issue-62/*.chs",
      "tests/bugs/issue-62/*.h",
      "tests/bugs/issue-62/*.c",
      "tests/bugs/issue-65/*.chs",
      "tests/bugs/issue-65/*.h",
      "tests/bugs/issue-65/*.c",
      "tests/bugs/issue-69/*.chs",
      "tests/bugs/issue-69/*.h",
      "tests/bugs/issue-69/*.c",
      "tests/bugs/issue-70/*.chs",
      "tests/bugs/issue-70/*.h",
      "tests/bugs/issue-70/*.c",
      "tests/bugs/issue-73/*.chs",
      "tests/bugs/issue-73/*.h",
      "tests/bugs/issue-73/*.c",
      "tests/bugs/issue-75/*.chs",
      "tests/bugs/issue-75/*.h",
      "tests/bugs/issue-75/*.c",
      "tests/bugs/issue-79/*.chs",
      "tests/bugs/issue-79/*.h",
      "tests/bugs/issue-79/*.c",
      "tests/bugs/issue-80/*.chs",
      "tests/bugs/issue-80/*.h",
      "tests/bugs/issue-80/*.c",
      "tests/bugs/issue-82/*.chs",
      "tests/bugs/issue-83/*.chs",
      "tests/bugs/issue-93/*.chs",
      "tests/bugs/issue-93/*.h",
      "tests/bugs/issue-93/*.c",
      "tests/bugs/issue-95/*.chs",
      "tests/bugs/issue-95/*.h",
      "tests/bugs/issue-95/*.c",
      "tests/bugs/issue-96/*.chs",
      "tests/bugs/issue-96/*.h",
      "tests/bugs/issue-96/*.c",
      "tests/bugs/issue-97/*.chs",
      "tests/bugs/issue-97/*.h",
      "tests/bugs/issue-97/*.c",
      "tests/bugs/issue-98/*.chs",
      "tests/bugs/issue-98/*.h",
      "tests/bugs/issue-98/*.c",
      "tests/bugs/issue-102/*.chs",
      "tests/bugs/issue-103/*.chs",
      "tests/bugs/issue-103/*.h",
      "tests/bugs/issue-103/*.c",
      "tests/bugs/issue-107/*.chs",
      "tests/bugs/issue-113/*.chs",
      "tests/bugs/issue-113/*.h",
      "tests/bugs/issue-113/*.c",
      "tests/bugs/issue-115/*.chs",
      "tests/bugs/issue-115/*.h",
      "tests/bugs/issue-115/*.c",
      "tests/bugs/issue-116/*.chs",
      "tests/bugs/issue-116/*.h",
      "tests/bugs/issue-116/*.c",
      "tests/bugs/issue-117/*.chs",
      "tests/bugs/issue-117/*.h",
      "tests/bugs/issue-117/*.c",
      "tests/bugs/issue-123/*.chs",
      "tests/bugs/issue-123/*.h",
      "tests/bugs/issue-123/*.c",
      "tests/bugs/issue-127/*.chs",
      "tests/bugs/issue-127/*.h",
      "tests/bugs/issue-127/*.c",
      "tests/bugs/issue-128/*.chs",
      "tests/bugs/issue-128/*.h",
      "tests/bugs/issue-128/*.c",
      "tests/bugs/issue-130/*.chs",
      "tests/bugs/issue-130/*.h",
      "tests/bugs/issue-130/*.c",
      "tests/bugs/issue-131/*.chs",
      "tests/bugs/issue-131/*.h",
      "tests/bugs/issue-131/*.c",
      "tests/bugs/issue-133/*.chs",
      "tests/bugs/issue-133/*.h",
      "tests/bugs/issue-134/*.chs",
      "tests/bugs/issue-134/*.h",
      "tests/bugs/issue-136/*.chs",
      "tests/bugs/issue-136/*.h",
      "tests/bugs/issue-136/*.c",
      "tests/bugs/issue-140/*.chs",
      "tests/bugs/issue-140/*.h",
      "tests/bugs/issue-140/*.c",
      "tests/bugs/issue-141/*.chs",
      "tests/bugs/issue-141/*.h",
      "tests/bugs/issue-149/*.chs",
      "tests/bugs/issue-149/*.h",
      "tests/bugs/issue-149/*.c",
      "tests/bugs/issue-151/*.chs",
      "tests/bugs/issue-151/*.h",
      "tests/bugs/issue-152/*.chs",
      "tests/bugs/issue-152/*.h",
      "tests/bugs/issue-155/*.chs",
      "tests/bugs/issue-155/*.h",
      "tests/bugs/issue-180/*.chs",
      "tests/bugs/issue-180/*.h",
      "tests/bugs/issue-192/*.chs",
      "tests/bugs/issue-192/*.h",
    ],
  extraTmpFiles = [ ],
  extraDocFiles = [ ],
)
)
