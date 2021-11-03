module Main (main) where

import Control.Monad
import qualified Data.Map.Strict as MS
import qualified Data.Set as Set
import qualified Data.Text as T
import Language.LSP.Types
import "ghc-lib-parser" Module (stringToUnitId, UnitId, unitIdString)
import System.Directory
import System.Environment
import System.FilePath
import System.IO.Extra

import DA.Daml.Compiler.DataDependencies
import DA.Daml.Compiler.DecodeDar
import DA.Daml.Compiler.ExtractDar
import DA.Daml.LF.Ast
import qualified DA.Service.Logger as Logger
import qualified DA.Service.Logger.Impl.IO as Logger

ref :: PackageRef -> UnitId
ref PRSelf = stringToUnitId "Self"
ref (PRImport pkgId) = stringToUnitId $ "Pkg_" <> T.unpack (unPackageId pkgId)

main :: IO ()
main = do
    [file] <- getArgs
    logger <- Logger.newStderrLogger Logger.Debug "data-deps"
    Logger.logInfo logger "Extracting dar"
    dar <- extractDar file
    Logger.logInfo logger "Decoding dar"
    DecodedDar{..} <- either fail pure $ decodeDar Set.empty dar
    let conf pkgId = Config
          { configPackages = MS.fromList
              [ ( dalfPackageId $ decodedDalfPkg dalf
                , extPackagePkg $ dalfPackagePkg $ decodedDalfPkg dalf
                )
              | dalf <- dalfs
              ]
          , configGetUnitId = ref
          , configSelfPkgId = pkgId
          , configStablePackages = MS.empty
          , configDependencyPackages = Set.empty
          , configSdkPrefix = []
          }
    Logger.logInfo logger "Starting source generation"
    withTempDir $ \dir ->
      forM_ dalfs $ \dalf -> do
          let pkgId = dalfPackageId $ decodedDalfPkg dalf
          let pkgIdStr = T.unpack (unPackageId pkgId)
          let context = T.pack (pkgIdStr <> " (" <> unitIdString (decodedUnitId dalf) <> ")")
          Logger.logInfo logger $ "Generating src for " <> context
          createDirectory (dir </> pkgIdStr)
          let src =
                  generateSrcPkgFromLf
                    (conf $ dalfPackageId $ decodedDalfPkg dalf)
                    (extPackagePkg $ dalfPackagePkg $ decodedDalfPkg dalf)
          forM_ src $ \(path, content) -> do
              createDirectoryIfMissing True (dir </> pkgIdStr </> takeDirectory (fromNormalizedFilePath path))
              writeFile (dir </> pkgIdStr </> fromNormalizedFilePath path) content
    Logger.logInfo logger "Finished"
