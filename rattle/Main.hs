-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ViewPatterns, RecordWildCards, LambdaCase #-}
{-# OPTIONS_GHC "-Wall" #-}
{-# OPTIONS_GHC "-fno-warn-name-shadowing" #-}
{-# OPTIONS_GHC "-fno-warn-missing-signatures" #-}

module Main(main) where

import Rattle
import Metadata
import Util
import Data.Char
import Data.Maybe
import System.Info.Extra
import System.Process.Extra
import System.FilePattern.Directory
import System.FilePath
import System.Directory
import System.Environment
import System.IO.Unsafe
import Control.Monad.Extra
import Data.List.Extra

targets =
    ["libs-haskell/prettyprinter-syntax"
    ,"daml-assistant"
    ,"daml-foundations/daml-tools/da-hs-damlc-app"
    ,"daml-foundations/daml-tools/da-hs-daml-cli"
    ,"compiler/haskell-ide-core"
    ,"libs-haskell/da-hs-base"
    ,"libs-haskell/da-hs-pretty"
    ,"libs-haskell/da-hs-language-server"
    ,"libs-haskell/da-hs-json-rpc"
    ,"compiler/daml-lf-ast"
    ,"compiler/daml-lf-tools"
    ,"compiler/daml-lf-proto"
    ,"daml-lf/archive"
    ,"daml-foundations/daml-ghc"
    ,"libs-haskell/bazel-runfiles"
    ,"compiler/scenario-service/client"
    ,"compiler/scenario-service/protos"
    ]

haskellExts =
    ["BangPatterns"
    ,"DeriveDataTypeable"
    ,"DeriveFoldable"
    ,"DeriveFunctor"
    ,"DeriveGeneric"
    ,"DeriveTraversable"
    ,"FlexibleContexts"
    ,"GeneralizedNewtypeDeriving"
    ,"LambdaCase"
    ,"NamedFieldPuns"
    ,"NumericUnderscores"
    ,"PackageImports"
    ,"RecordWildCards"
    ,"ScopedTypeVariables"
    ,"StandaloneDeriving"
    ,"TupleSections"
    ,"TypeApplications"
    ,"ViewPatterns"
    ]

haskellFlags =
    ["-Wall", "-Werror", "-Wincomplete-uni-patterns", "-Wno-name-shadowing"
    ,"-fno-omit-yields"
    ,"-threaded", "-rtsopts"
    -- run on two cores, disable idle & parallel GC
    ,"-with-rtsopts=-N2 -qg -I0"
    ]

main = rattle $ do
    putStrLn "  ---- Starting rattle build ----"
    metadata <- concatMapM (\x -> readMetadata $ x </> "BUILD.bazel") targets
    metadata <- return [x{dhl_deps = dhl_deps x `intersect` map dhl_name metadata} | x <- metadata]
    metadata <- return $ topSort [(dhl_name, dhl_deps, x) | x@Metadata{..} <- metadata]

    -- Figure out what libraries we need
    neededDeps <- pure $ (++ ["grpc-haskell" | True]) $ ("proto3-suite":) $ nubSort $ concatMap dhl_hazel_deps metadata

    installDependencies neededDeps

    -- generate the LF protobuf output
    let lfMajorVersions = ["0", "1", "dev"]
    forM_ ("":lfMajorVersions) $ \v -> do
        cmd_ (Cwd "daml-lf/archive")
            "compile-proto-file --proto" ["da/daml_lf" ++ ['_' | v /= ""] ++ v ++ ".proto"]
            "--out ../../.rattle/generated/daml-lf/haskell"

    -- generate the scenario protobuf output
    cmd_ (Cwd "compiler/scenario-service/protos")
            "compile-proto-file --proto scenario_service.proto"
            "--out ../../../.rattle/generated/scenario-service/haskell"

    -- build all the Haskell projects
    let trans = transitive [(dhl_name, dhl_deps) | Metadata{..} <- metadata]
    let patch x | dhl_name x == "daml_lf_haskell_proto" = x{dhl_dir = ".rattle/generated/daml-lf/haskell", dhl_srcs = ["**/*.hs"]}
                | dhl_name x == "scenario_service_haskell_proto" = x{dhl_dir = ".rattle/generated/scenario-service/haskell", dhl_srcs = ["**/*.hs"]}
                | otherwise = x

    putStrLn $ intercalate "\n - " $ "Building targets:" : (prettyName <$> metadata)

    forM_ metadata $ \m -> buildHaskellPackage $ patch m{dhl_deps = nubSort $ concatMap trans $ dhl_deps m}

    putStrLn $ intercalate "\n - " $ "Successfully built targets:" : (prettyName <$> metadata)

-- | Builds a haskell package.
--
-- If the package is an executable, builds all modules and links them to an
-- executable.
--
-- If the package is a library, builds all modules and creates a package DB
-- with both a static and a dynamic lib.
buildHaskellPackage :: Metadata -> IO ()
buildHaskellPackage (fixNames -> o@Metadata{..}) = do
    putStrLn "-----------"
    putStrLn $ "  " <> prettyName o
    putStrLn "-----------"
    print ("buildHaskellPackage",o)

    files <- map (drop $ if null dhl_src_strip_prefix then 0 else length dhl_src_strip_prefix + 1) <$>
             getDirectoryFiles dhl_dir dhl_srcs

    -- Work around the fact that SdkVersion is bazel-generated
    files <- pure $ files <> (case dhl_name of
      "da-hs-daml-cli" -> ["SdkVersion.hs"]
      _ -> [])

    let modules = map (intercalate "." . splitDirectories . dropExtension) files

    dirs <- pure $ [dhl_dir </> dhl_src_strip_prefix] <> (case dhl_name of
      -- Work around the fact that SdkVersion is bazel-generated
      "da-hs-daml-cli" -> ["rattle" </> "hacks"]
      _ -> [])

    commonFlags <- pure $
        -- set the correct output directories for objects, headers, etc
        [ flag ++ "=.rattle/haskell" </> dhl_name
        | flag <- ["-outputdir","-odir","-hidir","-stubdir"]
        ] <>

        -- specify which directories to look into for modules
        ["-i" ++ dir | dir <- dirs] <>

        -- makes sure GHC uses shared objects in RTS linker
        -- https://github.com/ghc/ghc/blob/cf9e1837adc647c90cfa176669d14e0d413c043d/compiler/main/DynFlags.hs#L2087
        ["-fexternal-interpreter"] <>

        -- Ensure GHC finds the packages
        (join [["-package", d] | d <- dhl_deps ]) <>
        (join [["-package", d] | d <- dhl_hazel_deps ]) <>
        ["-package-db=.rattle/haskell" </> d </> "pkg.db" | d <- dhl_deps] <>

        -- All extra extensions and flags
        (map ("-X"++) haskellExts) <> haskellFlags

    case dhl_main_is of
      Just main_ -> do

        mainMod <- pure $
          -- get the module name by dropping the function name
          -- Foo.Bar.main -> Foo.Bar
          intercalate "." .
          (reverse . drop 1 . reverse . wordsBy (== '.')) $ main_

        -- Include the main module name, if it's missing
        modules <- pure $ nubSort $ [mainMod] <> modules

        rpath <- ghcLibPath

        -- XXX: is this going to work on windows?
        cmd_ "mkdir" "-p" (".rattle/haskell" </> dhl_name)

        cmd_ "ghc" commonFlags
            modules
            "-o" [".rattle/haskell" </> dhl_name </> dhl_name]
            ["-main-is", main_]
            -- Set the rpath so that the executable uses the proper libffi so
            ["-optl-Wl,-rpath," <> rpath]

      Nothing -> do
        cmd_ "ghc" commonFlags
            "-dynosuf=dyn_o -dynhisuf=dyn_hi"
            ["-static"] ["-dynamic-too" | not isWindows]
            modules
            ["-this-unit-id=" ++ dhl_name]

        cmd_ "ar -r -s" [".rattle/haskell" </> dhl_name </> "libHS" ++ dhl_name ++ ".a"]
            [".rattle/haskell" </> dhl_name </> x -<.> "o" | x <- files]
        if isWindows then
            cmd_ "ld -x -r -o" [".rattle/haskell" </> dhl_name </> "HS" ++ dhl_name ++ ".o"]
                [".rattle/haskell" </> dhl_name </> x -<.> "o" | x <- files]
        else
            cmd_ "ghc -shared -dynamic" ["-dynload deploy"] "-o" [".rattle/haskell" </> dhl_name </> "libHS" ++ dhl_name ++ ".dylib"]
                [".rattle/haskell" </> dhl_name </> x -<.> "dyn_o" | x <- files]
        unlessM (doesDirectoryExist $ ".rattle/haskell" </> dhl_name </> "pkg.db") $
            cmd_ "ghc-pkg init" [".rattle/haskell" </> dhl_name </> "pkg.db"]
        keys <- map (drop 4) . filter (not . isInfixOf "haskeline") . lines <$>
            systemOutput_ "ghc-pkg field \"*\" key"
        writeFile (".rattle/haskell" </> dhl_name </> "pkg.db" </> dhl_name <.> "conf") $ unlines $
            ["name: " ++ dhl_name
            ,"version: 0"
            ,"id: " ++ dhl_name
            ,"key: " ++ dhl_name
            ,"hs-libraries: HS" ++ dhl_name
            ,"import-dirs: ${pkgroot}"
            ,"library-dirs: ${pkgroot}"
            ,"dynamic-library-dirs: ${pkgroot}"
            ,"exposed: True"
            ,"exposed-modules:"] ++
            map (" "++) modules ++
            ["depends:"] ++
            keys ++ map (" " ++) dhl_deps
        cmd_ "ghc-pkg recache" ["--package-db=" ++ ".rattle/haskell" </> dhl_name </> "pkg.db"]

-- | Installs all the specified packages in the stack database.
installDependencies :: [String] -> IO ()
installDependencies neededDeps = do

    putStrLn $ "Found " <> show (length neededDeps) <> " needed dependencies"

    -- Diff the installed dependencies to figure out what's missing
    Stdout sout <- cmd stack "exec" "--" ["ghc-pkg", "list"]
    installedDeps <- pure $ nubSort $ parseLibs $ lines sout
    missingDeps <- pure $ neededDeps \\ installedDeps

    unless (null missingDeps) $
      putStrLn $ intercalate " " $ "Installing missing deps:" : missingDeps

    -- 'stack build' sometimes gets confused if we ask for several libs to be
    -- installed at once, so we install them one by one
    forM_ missingDeps $ \dep -> do
      putStrLn $ "Installing " <> dep
      cmd_ stack "build" dep

    -- Double check that everything's available, otherwise fail
    Stdout sout <- cmd stack "exec" " --" ["ghc-pkg", "list"]
    installedDeps <- pure $ nubSort $ parseLibs $ lines sout
    missingDeps <- pure $ neededDeps \\ installedDeps
    unless (null missingDeps) $ do
        error $ intercalate " " $ "Could not find missing deps, try adding to extra-deps? :" : missingDeps

stack :: String
stack = unsafePerformIO $ do -- assume LD_LIBRARY_PATH won't change during a run
    -- XXX: We may have to use ';' on Windows
    dirs <- maybe [] (wordsBy (== ':')) <$> lookupEnv "LD_LIBRARY_PATH"
    pure $ intercalate " " $
      [ "stack" ] <>
      ((\d -> ["--extra-lib-dirs", d]) `concatMap` dirs) <>
      ["--stack-yaml=rattle/stack.yaml"]

-- | Ask stack for the path of GHC's bundled libs
-- XXX: Path gymnastics happening here, let's hope stack is somewhat consistent
ghcLibPath :: IO String
ghcLibPath = do
    Stdout str <- cmd stack "path" "--compiler-bin"
    case (reverse . splitPath) <$> lines str of
      ["bin" : ver : rest] -> do
        pure $ joinPath $ reverse $ "rts" : ver : "lib": ver : rest
      xs -> error $ "Couldn't parse ghc path: " <> show xs

-- | Parse the output of ghc-pkg list into a list of package names, e.g.
--  [ "lens", "conduit", ... ]
parseLibs :: [String] -> [String]
parseLibs = mapMaybe $ fmap parseLib . parseLine
  where
    parseLib :: String -> String
    parseLib = reverse . drop 1 . dropWhile (\c -> isDigit c || (c == '.')) . reverse
    parseLine :: String -> Maybe String
    parseLine = stripPrefix "    "

prettyName :: Metadata -> String
prettyName Metadata{..} =
    dhl_name <> maybe " (lib)" (const " (bin)") dhl_main_is

-- some packages (e.g. daml_lf_haskell_proto) use names which are unsuitable for GHC
-- so we switch them and the dependencies here
fixNames :: Metadata -> Metadata
fixNames o = o
    { dhl_name = fixName (dhl_name o)
    , dhl_deps = map fixName (dhl_deps o)
    }
  where
    fixName = replace "_" "-"
