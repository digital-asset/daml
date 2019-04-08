
module Main(main) where

import Rattle
import System.IO.Extra
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
    buildHaskellLibrary True [pp_syntax] "compiler/haskell-ide-core"
    base <- buildHaskellLibrary True [config] "libs-haskell/da-hs-base"
    pretty <- buildHaskellLibrary True [base,pp_syntax] "libs-haskell/da-hs-pretty"
    buildHaskellLibrary True [pretty,base] "compiler/daml-lf-ast"
    return ()


buildHaskellLibrary :: Bool -> [String] -> FilePath -> IO String
buildHaskellLibrary useSrc deps source = do
    print ("buildHaskellPackage",useSrc,deps,source)
    let addSrc x = if useSrc then x </> "src" else x
    let name = takeFileName source
    files <- getDirectoryFiles (addSrc source) ["**/*.hs"]
    let modules = map (intercalate "." . splitDirectories . dropExtension) files
    cmd_ "ghc" ["-hidir=.rattle/haskell" </> name, "-odir=.rattle/haskell" </> name]
        ["-i" ++ addSrc source]
        ["-package-db=.rattle/haskell" </> d </> "pkg.db" | d <- deps]
        modules ["-this-unit-id=" ++ name]
        (map ("-X"++) haskellExts) haskellFlags
    cmd_ "ar -r" [".rattle/haskell" </> name </> "libHS" ++ name ++ ".a"]
        [".rattle/haskell" </> name </> x -<.> "o" | x <- files]
    unlessM (doesDirectoryExist $ ".rattle/haskell" </> name </> "pkg.db") $
        cmd_ "ghc-pkg init" [".rattle/haskell" </> name </> "pkg.db"]
    writeFile (".rattle/haskell" </> name </> "pkg.db" </> name <.> "conf") $ unlines $
        ["name: " ++ name
        ,"version: 0"
        ,"id: " ++ name
        ,"key: " ++ name
        ,"hs-libraries: HS" ++ name
        ,"import-dirs: ${pkgroot}"
        ,"exposed: True"
        ,"exposed-modules:"] ++
        map (" "++) modules
    cmd_ "ghc-pkg recache" ["--package-db=" ++ ".rattle/haskell" </> name </> "pkg.db"]
    return name


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
    ,"-with-rtsopts=-N2 -qg -I0"]
