
-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- TODO: There is a lot of copying going on here from TsCodeGenMain.hs.
-- A nicer alternative would be to just change the exports from this module.

-- | Reading dar files for DAML LF verification.
module DA.Daml.LF.Verify.Read
  ( readPackages
  , optionsParserInfo
  , Options(..)
  ) where

import qualified DA.Daml.LF.Proto3.Archive as Archive
import qualified DA.Daml.LF.Reader as DAR
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Text.Extended as T
import qualified "zip-archive" Codec.Archive.Zip as Zip
import Control.Monad.Extra
import Options.Applicative

import DA.Daml.LF.Ast

data Options = Options
    { optInputDar :: FilePath
    , optChoiceTmpl :: String
    , optChoiceName :: String
    , optFieldTmpl :: String
    , optFieldName :: String
    }

optionsParser :: Parser Options
optionsParser = Options
    <$> argument str
        (  metavar "DAR-FILE"
        <> help "DAR file to analyse"
        )
    <*> argument str
        (  metavar "CHOICE-TEMPLATE"
        <> help "Template of the choice to analyse"
        )
    <*> argument str
        (  metavar "CHOICE-NAME"
        <> help "Name of the choice to analyse"
        )
    <*> argument str
        (  metavar "FIELD-TEMPLATE"
        <> help "Template of the field to verify"
        )
    <*> argument str
        (  metavar "FIELD-NAME"
        <> help "Name of the field to verify"
        )

optionsParserInfo :: ParserInfo Options
optionsParserInfo = info (optionsParser <**> helper)
    (  fullDesc
    <> progDesc "Perform static analysis on a DAR"
    )

-- Build a list of packages from a list of DAR file paths.
readPackages :: [FilePath] -> IO [(PackageId, (Package, Maybe PackageName))]
readPackages dars = concatMapM darToPackages dars
  where
    darToPackages :: FilePath -> IO [(PackageId, (Package, Maybe PackageName))]
    darToPackages dar = do
      dar <- B.readFile dar
      let archive = Zip.toArchive $ BSL.fromStrict dar
      dalfs <- either fail pure $ DAR.readDalfs archive
      DAR.DalfManifest{packageName} <- either fail pure $ DAR.readDalfManifest archive
      packageName <- pure (PackageName . T.pack <$> packageName)
      forM ((DAR.mainDalf dalfs, packageName) : map (, Nothing) (DAR.dalfs dalfs)) $
        \(dalf, mbPkgName) -> do
          (pkgId, pkg) <- either (fail . show)  pure $ Archive.decodeArchive Archive.DecodeAsMain (BSL.toStrict dalf)
          pure (pkgId, (pkg, mbPkgName))
