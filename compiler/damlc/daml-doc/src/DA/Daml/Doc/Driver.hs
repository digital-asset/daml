-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Doc.Driver
    ( DamldocOptions(..)
    , InputFormat(..)
    , OutputFormat(..)
    , RenderFormat(..)
    , TransformOptions(..)
    , ExternalAnchorPath(..)
    , runDamlDoc
    , loadExternalAnchors
    ) where

import DA.Daml.Doc.Types
import DA.Daml.Doc.Render
import DA.Daml.Doc.Extract
import DA.Daml.Doc.Transform

import DA.Daml.Options.Types

import DA.Bazel.Runfiles
import Development.IDE.Core.Shake (NotificationHandler)
import Development.IDE.Types.Location

import Control.Monad.Extra
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.Except
import Control.Exception

import Data.Maybe
import Data.Bifunctor
import System.IO
import System.Exit
import System.Directory
import System.FilePath

import qualified Data.Aeson as AE
import qualified Data.Aeson.Encode.Pretty as AP
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.HashMap.Strict as HMS
import qualified Data.Text.Encoding as T
import qualified Data.Text.Extended as T

import SdkVersion.Class (SdkVersioned)

data DamldocOptions = DamldocOptions
    { do_inputFormat :: InputFormat
    , do_compileOptions :: Options
    , do_diagsLogger ::  NotificationHandler
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
    , do_externalAnchorPath :: ExternalAnchorPath -- ^ external anchor table input path
    , do_globalInternalExt :: String -- ^ File extension for internal links
    }

data InputFormat = InputJson | InputDaml
    deriving (Eq, Show, Read)

data OutputFormat = OutputJson | OutputDocs RenderFormat
    deriving (Eq, Show, Read)

data ExternalAnchorPath
  = NoExternalAnchorPath
      -- ^ Use an empty AnchorMap
  | DefaultExternalAnchorPath
      -- ^ Use the AnchorMap for daml-prim and daml-stdlib defined by
      -- //compiler/damlc:daml-base-anchors.json
  | ExplicitExternalAnchorPath FilePath
      -- ^ Read the AnchorMap from the given file
  deriving (Eq, Show, Read)

-- | Run damldocs!
runDamlDoc :: SdkVersioned => DamldocOptions -> IO ()
runDamlDoc options@DamldocOptions{..} = do
    docData <- inputDocData options
    renderDocData options (applyTransform do_transformOptions docData)

-- | Load doc data, either via the Daml typechecker or via JSON files.
inputDocData :: SdkVersioned => DamldocOptions -> IO [ModuleDoc]
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

-- | Load a database of external anchors from a file. Will abnormally
-- terminate the program if there's an IOError or json decoding
-- failure.
loadExternalAnchors :: ExternalAnchorPath -> IO AnchorMap
loadExternalAnchors eapath = do
    let printAndExit err = do
            hPutStr stderr err
            exitFailure
    anchors <- case eapath of
        NoExternalAnchorPath -> pure . Right $ AnchorMap HMS.empty
        DefaultExternalAnchorPath -> getDamlBaseAnchorsPath >>= tryLoadAnchors
        ExplicitExternalAnchorPath path -> tryLoadAnchors path
    either printAndExit pure anchors
    where
        tryLoadAnchors path = runExceptT $ do
            bytes <- ExceptT $ first readErr <$> try @IOError (LBS.fromStrict <$> BS.readFile path)
            ExceptT $ pure (first decodeErr (AE.eitherDecode @AnchorMap bytes))
            where
                readErr = const $ "Failed to read anchor table '" ++ path ++ "'"
                decodeErr err = unlines ["Failed to decode anchor table '" ++ path ++ "':", err]
        getDamlBaseAnchorsPath = locateResource Resource
            -- //compiler/damlc:daml-base-anchors.json
            { resourcesPath = "daml-base-anchors.json"
              -- In a packaged application, this is stored directly underneath
              -- the resources directory because it's a single file.
              -- See @bazel_tools/packaging/packaging.bzl@.
            , runfilesPathPrefix = mainWorkspace </> "compiler" </> "damlc"
            }

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
                externalAnchors <- loadExternalAnchors do_externalAnchorPath
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
                        , ro_externalAnchors = externalAnchors
                        , ro_globalInternalExt = do_globalInternalExt
                        }
                renderDocs renderOptions docData
