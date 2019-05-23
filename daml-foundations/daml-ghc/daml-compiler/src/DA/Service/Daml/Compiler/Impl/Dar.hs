-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings   #-}

module DA.Service.Daml.Compiler.Impl.Dar
  ( buildDar
  ) where

import           Control.Monad.Extra
import           Data.List.Extra
import qualified Data.Text                  as T
import qualified Data.ByteString            as BS
import qualified Data.ByteString.Lazy       as BSL
import qualified Data.ByteString.Lazy.Char8 as BSC
import           System.FilePath
import System.Directory
import qualified Codec.Archive.Zip          as Zip
import DA.Daml.GHC.Compiler.Options

------------------------------------------------------------------------------
{- | Builds a dar file.

A (fat) dar file is a zip file containing

* a dalf of a DAML library <name>.dalf
* a MANIFEST.MF file that describes the package
* all source files to that library
     - a dependency tree of imports
     - starting from the given top-level DAML 'file'
     - all these files _must_ reside in the same directory 'topdir'
     - the 'topdir' in the absolute path is replaced by 'name'
* all dalf dependencies
* additional data files under the data/ directory.

'topdir' is the path prefix of the top module that is _not_ part of the
qualified module name.
Example:  'file' = "/home/dude/work/solution-xy/daml/XY/Main/LibraryModules.daml"
contains "daml-1.2 module XY.Main.LibraryModules"
so 'topdir' is "/home/dude/work/solution-xy/daml"

The dar archive should stay independent of the dependency resolution tool. Therefore the pom file is
gernerated separately.

-}

buildDar ::
  BSL.ByteString
  -> FilePath
  -> [(T.Text, BS.ByteString)]
  -> [FilePath]
  -> [(String, BS.ByteString)]
  -> String
  -> String
  -> IO BS.ByteString
buildDar dalf modRoot dalfDependencies fileDependencies dataFiles name sdkVersion = do
    -- Take all source file dependencies and produced interface files. Only the new package command
    -- produces interface files per default, hence we filter for existent files.
    ifaces <-
        filterM doesFileExist $
        concat [[ifaceDir </> dep -<.> "hi", ifaceDir </> dep -<.> "hie"] | dep <- fileDependencies]

    -- Reads all module source files, and pairs paths (with changed prefix)
    -- with contents as BS. The path must be within the module root path, and
    -- is modified to have prefix <name> instead of the original root path.
    srcFiles <- forM fileDependencies $ \mPath -> do
      contents <- BSL.readFile mPath
      return (name </> makeRelative' modRoot mPath, contents)

    ifaceFaceFiles <- forM ifaces $ \mPath -> do
      contents <- BSL.readFile mPath
      return (name </> makeRelative' (ifaceDir </> modRoot) mPath, contents)

    let dalfName = name <> ".dalf"
    let dependencies = [(T.unpack pkgName <> ".dalf", BSC.fromStrict bs)
                          | (pkgName, bs) <- dalfDependencies]
    let dataFiles' = [("data" </> n, BSC.fromStrict bs) | (n, bs) <- dataFiles]

    -- construct a zip file from all required files
    let allFiles = ("META-INF/MANIFEST.MF", manifestHeader dalfName $ dalfName:map fst dependencies)
                    : (dalfName, dalf)
                    : srcFiles
                    ++ ifaceFaceFiles
                    ++ dependencies
                    ++ dataFiles'

        mkEntry (filePath, content) = Zip.toEntry filePath 0 content
        zipArchive = foldr (Zip.addEntryToArchive . mkEntry) Zip.emptyArchive allFiles

    pure $ BSL.toStrict $ Zip.fromArchive zipArchive
      where
        manifestHeader :: FilePath -> [String] -> BSL.ByteString
        manifestHeader location dalfs = BSC.pack $ unlines
          [ "Manifest-Version: 1.0"
          , "Created-By: Digital Asset packager (DAML-GHC)"
          , "Sdk-Version: " <> sdkVersion
          , breakAt72Chars $ "Main-Dalf: " <> location
          , breakAt72Chars $ "Dalfs: " <> intercalate ", " dalfs
          , "Format: daml-lf"
          , "Encryption: non-encrypted"
          ]

        breakAt72Chars :: String -> String
        breakAt72Chars s = case splitAt 72 s of
          (s0, []) -> s0
          (s0, rest) -> s0 ++ "\n" ++ breakAt72Chars (" " ++ rest)

-- | Like `makeRelative` but also takes care of normalising filepaths so
--
-- > makeRelative' "./a" "a/b" == "b"
--
-- instead of
--
-- > makeRelative "./a" "a/b" == "a/b"
makeRelative' :: FilePath -> FilePath -> FilePath
makeRelative' a b = makeRelative (normalise a) (normalise b)
