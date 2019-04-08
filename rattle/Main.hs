
module Main(main) where

import Rattle
import System.IO.Extra
import System.FilePattern.Directory
import System.FilePath
import System.Directory
import Control.Monad.Extra
import Data.List


main = rattle $ do
    src <- lines <$> readFile' "rattle/haskell-dependencies.txt"
    cmd_ "stack build --stack-yaml=rattle/stack.yaml" src
    buildHaskellPackage [] "libs-haskell/prettyprinter-syntax"
    buildHaskellPackage ["prettyprinter-syntax"] "compiler/haskell-ide-core"


buildHaskellPackage :: [String] -> FilePath -> IO ()
buildHaskellPackage deps src = do
    print ("buildHaskellPackage",deps,src)
    let name = takeFileName src
    files <- getDirectoryFiles (src </> "src") ["**/*.hs"]
    let modules = map (intercalate "." . splitDirectories . dropExtension) files
    cmd_ "ghc" ["-hidir=.rattle/haskell" </> name, "-odir=.rattle/haskell" </> name]
        ["-i" ++ src </> "src"]
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
    cmd "ghc-pkg recache" ["--package-db=" ++ ".rattle/haskell" </> name </> "pkg.db"]


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
