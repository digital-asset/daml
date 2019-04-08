{-# LANGUAGE ViewPatterns #-}

module Main(main) where

import Rattle
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
    src <- lines <$> readFile' "rattle/haskell-dependencies.txt"
    cmd_ "stack build --stack-yaml=rattle/stack.yaml" $ delete "" $ nubOrd src
    pp_syntax <- buildHaskellLibrary True [] "libs-haskell/prettyprinter-syntax"
    config <- buildHaskellLibrary False [] "daml-assistant/daml-project-config"
    buildHaskellLibrary True pp_syntax "compiler/haskell-ide-core"
    base <- buildHaskellLibrary True config "libs-haskell/da-hs-base"
    pretty <- buildHaskellLibrary True (base++pp_syntax) "libs-haskell/da-hs-pretty"
    buildHaskellLibrary True (pretty++base) "compiler/daml-lf-ast"
    return ()


buildHaskellLibrary :: Bool -> [String] -> FilePath -> IO [String]
buildHaskellLibrary useSrc (nubOrd -> deps) source = do
    print ("buildHaskellPackage",useSrc,deps,source)
    let addSrc x = if useSrc then x </> "src" else x
    let name = takeFileName source
    files <- getDirectoryFiles (addSrc source) ["**/*.hs"]
    let modules = map (intercalate "." . splitDirectories . dropExtension) files
    cmd_ "ghc"
        [flag ++ "=.rattle/haskell" </> name | flag <- ["-outputdir","-odir","-hidir","-stubdir"]]
        ["-i" ++ addSrc source]
        "-dynosuf=dyn_o -dynhisuf=dyn_hi"
        ["-static"] ["-dynamic-too" | not isWindows]
        ["-package-db=.rattle/haskell" </> d </> "pkg.db" | d <- deps]
        modules ["-this-unit-id=" ++ name]
        (map ("-X"++) haskellExts) haskellFlags
    cmd_ "ar -r -s" [".rattle/haskell" </> name </> "libHS" ++ name ++ ".a"]
        [".rattle/haskell" </> name </> x -<.> "o" | x <- files]
    if isWindows then
        cmd_ "ld -x -r -o" [".rattle/haskell" </> name </> "HS" ++ name ++ ".o"]
            [".rattle/haskell" </> name </> x -<.> "o" | x <- files]
    else
        cmd_ "ghc -shared -dynamic" ["-dynload deploy"] "-o" [".rattle/haskell" </> name </> "libHS" ++ name ++ ".dylib"]
            [".rattle/haskell" </> name </> x -<.> "dyn_o" | x <- files]
    unlessM (doesDirectoryExist $ ".rattle/haskell" </> name </> "pkg.db") $
        cmd_ "ghc-pkg init" [".rattle/haskell" </> name </> "pkg.db"]
    keys <- map (drop 4) . filter (not . isInfixOf "haskeline") . filter (not . isInfixOf "ghc-lib") . lines <$>
        systemOutput_ "ghc-pkg field \"*\" key"
    writeFile (".rattle/haskell" </> name </> "pkg.db" </> name <.> "conf") $ unlines $
        ["name: " ++ name
        ,"version: 0"
        ,"id: " ++ name
        ,"key: " ++ name
        ,"hs-libraries: HS" ++ name
        ,"import-dirs: ${pkgroot}"
        ,"library-dirs: ${pkgroot}"
        ,"dynamic-library-dirs: ${pkgroot}"
        ,"exposed: True"
        ,"exposed-modules:"] ++
        map (" "++) modules ++
        ["depends:"] ++
        keys ++ map (" "++) deps
    cmd_ "ghc-pkg recache" ["--package-db=" ++ ".rattle/haskell" </> name </> "pkg.db"]
    return $ name : deps


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
