-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Doc.Render
  ( RenderFormat(..)
  , RenderOptions(..)
  , RenderMode(..)
  , renderDocs
  , renderPage
  , renderFolder
  , renderRst
  , renderMd
  , renderModule
  , renderSimpleHoogle
  , jsonConf
  ) where

import DA.Daml.Doc.Render.Types
import DA.Daml.Doc.Render.Monoid
import DA.Daml.Doc.Render.Rst
import DA.Daml.Doc.Render.Markdown
import DA.Daml.Doc.Render.Hoogle
import DA.Daml.Doc.Render.Output
import DA.Daml.Doc.Types

import Control.Monad.Extra
import Data.Maybe
import Data.Foldable
import System.Directory
import System.FilePath
import System.IO
import System.Exit

import CMarkGFM qualified as GFM
import Data.Aeson qualified as A
import Data.Aeson.Encode.Pretty qualified as AP
import Data.Text qualified as T
import Data.Text.Lazy qualified as TL
import Data.Text.Encoding qualified as T
import Data.ByteString qualified as BS
import Data.Map.Strict qualified as Map
import Text.Mustache qualified as M

-- | centralised JSON configuration for pretty-printing
jsonConf :: AP.Config
jsonConf = AP.Config (AP.Spaces 2) (AP.keyOrder ["id"]) AP.Generic True

renderDocs :: RenderOptions -> [ModuleDoc] -> IO ()
renderDocs ro@RenderOptions{..} mods = do
    let (formatter, postProcessing) =
            case ro_format of
                Rst -> (renderRst, id)
                Markdown -> (renderMd, id)
                Html -> (renderMd, GFM.commonmarkToHtml [GFM.optUnsafe] [GFM.extTable])
        templateText = fromMaybe (defaultTemplate ro_format) ro_template
        renderMap = Map.fromList [(md_name mod, renderModule mod) | mod <- mods]
        externalAnchors = ro_externalAnchors

    template <- compileTemplate "template" templateText

    case ro_mode of
        RenderToFile path -> do
            BS.writeFile path
                . T.encodeUtf8
                . renderTemplate ro template
                . postProcessing
                . renderPage formatter externalAnchors
                $ fold renderMap

        RenderToFolder path -> do
            let (outputIndex, outputMap) = renderFolder formatter externalAnchors ro_globalInternalExt renderMap
                extension =
                    case ro_format of
                        Markdown -> "md"
                        Rst -> "rst"
                        Html -> "html"

                outputPath mod = path </> moduleNameToFileName mod <.> extension

            createDirectoryIfMissing True path
            for_ (Map.toList outputMap) $ \ (mod, renderedOutput) -> do
                BS.writeFile (outputPath mod)
                    . T.encodeUtf8
                    . renderTemplate ro template
                    . postProcessing
                    $ renderedOutput

            let indexTemplateText = fromMaybe (defaultTemplate ro_format) ro_indexTemplate
            indexTemplate <- compileTemplate "index template" indexTemplateText

            BS.writeFile (path </> "index" <.> extension)
                . T.encodeUtf8
                . renderTemplate ro indexTemplate
                . postProcessing
                $ outputIndex

    let anchorTable = buildAnchorTable ro renderMap
    whenJust ro_anchorPath $ \anchorPath -> do
        A.encodeFile anchorPath anchorTable
    whenJust ro_hooglePath $ \hooglePath -> do
        let he = HoogleEnv { he_anchorTable = anchorTable }
        hoogleTemplate <- compileTemplate "hoogle template"
            (fromMaybe defaultHoogleTemplate ro_hoogleTemplate)
        BS.writeFile hooglePath
            . T.encodeUtf8
            . renderTemplate ro hoogleTemplate
            . T.concat
            $ map (renderSimpleHoogle he) mods

compileTemplate :: T.Text -> T.Text -> IO M.Template
compileTemplate templateName templateText =
    case M.compileMustacheText "daml docs template" templateText of
        Right t -> pure t
        Left e -> do
            hPutStrLn stderr ("Error with daml docs " <> T.unpack templateName <> ": " <> show e)
            exitFailure

renderTemplate ::
    RenderOptions
    -> M.Template -- ^ template
    -> T.Text -- ^ page body
    -> T.Text
renderTemplate RenderOptions{..} template pageBody =
    TL.toStrict . M.renderMustache template . A.object $
        [ "base-url" A..= fromMaybe "" ro_baseURL
        , "title" A..= fromMaybe "" ro_title
        , "body" A..= pageBody
        ]

defaultHoogleTemplate :: T.Text
defaultHoogleTemplate = T.unlines
    [ "-- Hoogle database generated by damlc."
    , "-- See Hoogle, http://www.haskell.org/hoogle/"
    , ""
    , "@url {{{base-url}}}"
    , "@package {{package-name}}" -- TODO
    , "@version {{package-version}}" -- TODO
    , ""
    , "{{{body}}}"
    ]

defaultTemplate :: RenderFormat -> T.Text
defaultTemplate = \case
    Html -> defaultTemplateHtml
    _ -> "{{{body}}}"

defaultTemplateHtml :: T.Text
defaultTemplateHtml = T.unlines
    [ "<html>"
    , "<head><title>{{title}}</title><meta charset=\"utf-8\"></head>"
    , "<body>{{{body}}}</body>"
    , "</html>"
    ]
