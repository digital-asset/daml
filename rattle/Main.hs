{-# LANGUAGE ViewPatterns, RecordWildCards #-}

module Main(main) where

import Rattle
import Metadata
import Util
import System.IO.Extra
import System.Info.Extra
import System.Process.Extra
import System.FilePattern.Directory
import System.FilePath
import System.Directory
import Control.Monad.Extra
import Data.List.Extra


main = rattle $ do
    print "Starting rattle build"
    metadata <- concatMapM (\x -> readMetadata $ x </> "BUILD.bazel")
        ["libs-haskell/prettyprinter-syntax"
        ,"daml-assistant"
        ,"compiler/haskell-ide-core"
        ,"libs-haskell/da-hs-base"
        ,"libs-haskell/da-hs-pretty"
        ,"compiler/daml-lf-ast"
        ,"compiler/daml-lf-tools"
        --,"compiler/daml-lf-proto"
        ]
    metadata <- return [x{dhl_deps = dhl_deps x `intersect` map dhl_name metadata} | x <- metadata]
    metadata <- return $ topSort [(dhl_name, dhl_deps, x) | x@Da_haskell_library{..} <- metadata]
    cmd_ "stack build --stack-yaml=rattle/stack.yaml" $ ("proto3-suite":) $ nubSort (concatMap dhl_hazel_deps metadata) \\ ["ghc-lib","ghc-lib-parser"]

    let trans = transitive [(dhl_name, dhl_deps) | Da_haskell_library{..} <- metadata]
    forM_ metadata $ \m -> buildHaskellLibrary m{dhl_deps = nubSort $ concatMap trans $ dhl_deps m}


buildHaskellLibrary :: Metadata -> IO ()
buildHaskellLibrary o@Da_haskell_library{..} = do
    print ("buildHaskellPackage",o)
    files <- map (drop $ if null dhl_src_strip_prefix then 0 else length dhl_src_strip_prefix + 1) <$>
             getDirectoryFiles dhl_dir dhl_srcs
    let modules = map (intercalate "." . splitDirectories . dropExtension) files
    cmd_ "ghc"
        [flag ++ "=.rattle/haskell" </> dhl_name | flag <- ["-outputdir","-odir","-hidir","-stubdir"]]
        ["-i" ++ dhl_dir </> dhl_src_strip_prefix]
        "-dynosuf=dyn_o -dynhisuf=dyn_hi"
        ["-static"] ["-dynamic-too" | not isWindows]
        ["-package-db=.rattle/haskell" </> d </> "pkg.db" | d <- dhl_deps]
        modules ["-this-unit-id=" ++ dhl_name]
        (map ("-X"++) haskellExts) haskellFlags
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
    keys <- map (drop 4) . filter (not . isInfixOf "haskeline") . filter (not . isInfixOf "ghc-lib") . lines <$>
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
