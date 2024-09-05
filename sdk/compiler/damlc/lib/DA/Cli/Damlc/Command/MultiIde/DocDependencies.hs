-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.Command.MultiIde.DocDependencies (unpackDoc) where

import DA.Cli.Damlc.Command.MultiIde.Types (MultiIdeState (..))
import DA.Cli.Damlc.Packaging (baseImports, getDepImports, getExposedModules)
import DA.Daml.Compiler.Dar (getDamlRootFiles)
import DA.Daml.Compiler.Output (diagnosticsLogger)
import DA.Daml.Doc.Driver (runDamlDoc, DamldocOptions (..), InputFormat (..), OutputFormat (..), ExternalAnchorPath (..))
import DA.Daml.Doc.Extract (defaultExtractOptions)
import DA.Daml.Doc.Render.Types (RenderFormat (..))
import DA.Daml.Doc.Transform (defaultTransformOptions)
import DA.Daml.LF.Ast.Version (parseVersion)
import DA.Daml.Options.Types (Options (..), IgnorePackageMetadata (..), defaultOptions)
import Data.List.Extra (dropEnd, intercalate)
import Data.List.Split (splitWhen)
import Development.IDE.Core.Rules.Daml (generatePackageMap)
import SdkVersion.Class (SdkVersioned)
import System.Directory (createDirectoryIfMissing)
import System.FilePath (splitDirectories, joinPath, (</>))

-- Use the original given location (which will be to generated doc page)
-- unpackDar should fail if daml files are missing, which it can compare against the LF modules in the dalf
--   fail in the same way that "no dar" would, so we recover in the same way
-- run runDamlDoc on it, to generate something we can show, might need to be html, as we can't assume an `md` viewer

-- unpack to .daml/unpacked-docs

-- we expect the path to be in the package-database, therefore of the form **/package-database/<lf-version>/<name>-<version>-<hash>/**/*.daml
-- we can extract the lf version and unit id from these for convenience.
unpackDoc :: SdkVersioned => MultiIdeState -> FilePath -> String -> IO String
unpackDoc miState modulePath moduleName = do
  let packageRootDirList = dropEnd (length $ splitWhen (=='.') moduleName) $ splitDirectories modulePath
      dropLastIfNotAlone xs = if length xs > 1 then init xs else xs
      -- Drops the package-id if there is a unit id before it
      unitIdOrPackageId = intercalate "-" $ dropLastIfNotAlone $ splitWhen (=='-') $ last packageRootDirList
      packageDb = joinPath $ dropEnd 2 packageRootDirList
      unpackDir = unpackedDocsLocation miState </> unitIdOrPackageId

  damlFiles <- getDamlRootFiles $ joinPath packageRootDirList
  lfVersion <- maybe (fail "Failed to parse LF Version in package database") pure $ parseVersion $ last $ init packageRootDirList
  let initialOpts = (defaultOptions $ Just lfVersion)
        { optImportPath = [joinPath packageRootDirList]
        , optPackageDbs = [packageDb]
        }

  -- TODO Consider if this should be called via Shake
  -- Also consider if this should somehow defer to a given SDK version
  (_, pkgMap) <- generatePackageMap lfVersion Nothing [packageDb]
  exposedModules <- getExposedModules initialOpts (head damlFiles)
  createDirectoryIfMissing True unpackDir

  runDamlDoc DamldocOptions
    { do_inputFormat = InputDaml
    , do_compileOptions = initialOpts
        { optPackageImports =
            baseImports ++
            getDepImports pkgMap exposedModules
        -- (From Packaging.hs)
        -- When compiling dummy interface files for a data-dependency,
        -- we know all package flags so we don’t need to consult metadata.
        , optIgnorePackageMetadata = IgnorePackageMetadata True
        }
    , do_diagsLogger = diagnosticsLogger
    , do_outputPath = unpackDir </> "doc.html"
    , do_outputFormat = OutputDocs Html
    , do_docTemplate = Nothing
    , do_docIndexTemplate = Nothing
    , do_docHoogleTemplate = Nothing
    , do_transformOptions = defaultTransformOptions
    , do_inputFiles = damlFiles
    , do_docTitle = Nothing -- Consider renaming later
    , do_combine = True -- For now, we'll combine to make it easier
    , do_extractOptions = defaultExtractOptions
    , do_baseURL = Nothing
    , do_hooglePath = Nothing
    , do_anchorPath = Nothing
    , do_externalAnchorPath = DefaultExternalAnchorPath -- May need to read from external file, to setup links for dependent packages?
    , do_globalInternalExt = ""
    }
  pure $ "daml:open-docs?path=" <> (unpackDir </> "doc.html") <> "&name=" <> unitIdOrPackageId

unpackedDocsLocation :: MultiIdeState -> FilePath
unpackedDocsLocation miState = misMultiPackageHome miState </> ".daml" </> "unpacked-docs"
