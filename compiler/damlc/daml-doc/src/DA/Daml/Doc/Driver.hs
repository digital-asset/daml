-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Doc.Driver
    ( DamldocOptions(..)
    , InputFormat(..)
    , OutputFormat(..)
    , RenderFormat(..)
    , TransformOptions(..)
    , runDamlDoc
    ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Render
import DA.Daml.Doc.Extract
import DA.Daml.Doc.Transform

import DA.Daml.Options.Types

import Development.IDE.Types.Location
import qualified Language.Haskell.LSP.Messages as LSP

import Control.Monad.Extra
import Control.Monad.Trans.Maybe
import Data.Maybe
import System.IO
import System.Exit
import System.Directory
import System.FilePath

import qualified Data.Aeson as AE
import qualified Data.Aeson.Encode.Pretty as AP
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Text.Extended as T
import qualified Data.Text.Encoding as T

data DamldocOptions = DamldocOptions
    { do_inputFormat :: InputFormat
    , do_compileOptions :: Options
    , do_diagsLogger :: LSP.FromServerMessage -> IO ()
    , do_outputPath :: FilePath
    , do_outputFormat :: OutputFormat
    , do_docTemplate :: Maybe FilePath
    , do_docIndexTemplate :: Maybe FilePath
    , do_docHoogleTemplate :: Maybe FilePath
    , do_transformOptions :: TransformOptions
    , do_inputFiles :: [NormalizedFilePath]
    , do_docTitle :: Maybe T.Text
    , do_combine :: Bool
    , do_extractOptions :: ExtractOptions
    , do_baseURL :: Maybe T.Text -- ^ base URL for generated documentation
    , do_hooglePath :: Maybe FilePath -- ^ hoogle database output path
    , do_anchorPath :: Maybe FilePath -- ^ anchor table output path
    }

data InputFormat = InputJson | InputDaml
    deriving (Eq, Show, Read)

data OutputFormat = OutputJson | OutputDocs RenderFormat
    deriving (Eq, Show, Read)

-- | Run damldocs!
runDamlDoc :: DamldocOptions -> IO ()
runDamlDoc options@DamldocOptions{..} = do
    docData <- inputDocData options
    renderDocData options (applyTransform do_transformOptions docData)

-- | Load doc data, either via the DAML typechecker or via JSON files.
inputDocData :: DamldocOptions -> IO [ModuleDoc]
inputDocData DamldocOptions{..} = do

    let printAndExit errMsg = do
            hPutStr stderr . unlines $
                [ unwords
                    $ "Error processing input from"
                    : map fromNormalizedFilePath do_inputFiles
                , errMsg
                ]
            exitFailure

        onErrorExit act = act >>= maybe (printAndExit "") pure

    case do_inputFormat of
        InputJson -> do
            input <- mapM (BS.readFile . fromNormalizedFilePath) do_inputFiles
            let mbData = map (AE.eitherDecode . LBS.fromStrict) input
            concatMapM (either printAndExit pure) mbData

        InputDaml -> onErrorExit . runMaybeT $
            extractDocs do_extractOptions do_diagsLogger do_compileOptions do_inputFiles

-- | Output doc data.
renderDocData :: DamldocOptions -> [ModuleDoc] -> IO ()
renderDocData DamldocOptions{..} docData = do
    templateM <- mapM T.readFileUtf8 do_docTemplate
    indexTemplateM <- mapM T.readFileUtf8 do_docIndexTemplate
    hoogleTemplateM <- mapM T.readFileUtf8 do_docHoogleTemplate

    let prefix = fromMaybe "" templateM
        write file contents = do
            createDirectoryIfMissing True $ takeDirectory file
            putStrLn $ "Writing " ++ file ++ "..."
            T.writeFileUtf8 file $ prefix <> contents

    case do_outputFormat of
            OutputJson ->
                write do_outputPath $ T.decodeUtf8 . LBS.toStrict $ AP.encodePretty' jsonConf docData
            OutputDocs format -> do
                let renderOptions = RenderOptions
                        { ro_mode =
                            if do_combine
                                then RenderToFile do_outputPath
                                else RenderToFolder do_outputPath
                        , ro_format = format
                        , ro_title = do_docTitle
                        , ro_template = templateM
                        , ro_indexTemplate = indexTemplateM
                        , ro_hoogleTemplate = hoogleTemplateM
                        , ro_baseURL = do_baseURL
                        , ro_hooglePath = do_hooglePath
                        , ro_anchorPath = do_anchorPath
                        }
                renderDocs renderOptions docData
