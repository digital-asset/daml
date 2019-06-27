-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE CPP #-}

-- | Based on https://ghc.haskell.org/trac/ghc/wiki/Commentary/Compiler/API.
--   Given a list of paths to find libraries, and a file to compile, produce a list of 'CoreModule' values.
module Development.IDE.Core.Compile
  ( TcModuleResult(..)
  , getGhcDynFlags
  , compileModule
  , getSrcSpanInfos
  , parseModule
  , parseFileContents
  , typecheckModule
  , computePackageDeps
  ) where

import           Development.IDE.GHC.Warnings
import           Development.IDE.GHC.CPP
import           Development.IDE.Types.Diagnostics
import qualified Development.IDE.Import.FindImports as FindImports
import           Development.IDE.GHC.Error
import           Development.IDE.Spans.Calculate
import Development.IDE.GHC.Orphans()
import Development.IDE.GHC.Util
import Development.IDE.GHC.Compat
import Development.IDE.Types.Options
import Development.IDE.Types.Location

import           GHC hiding (parseModule, typecheckModule)
import qualified Parser
import           Lexer
import ErrUtils

import qualified GHC
import           Panic
import           GhcMonad
import           GhcPlugins                     as GHC hiding (fst3, (<>))
import qualified HeaderInfo                     as Hdr
import           MkIface
import           StringBuffer                   as SB
import           TidyPgm
import qualified GHC.LanguageExtensions as LangExt

import Control.DeepSeq
import           Control.Monad
import Control.Monad.Trans.Except
import qualified Data.Text as T
import           Data.IORef
import           Data.List.Extra
import           Data.Maybe
import           Data.Tuple.Extra
import qualified Data.Map.Strict                          as Map
import           Development.IDE.Spans.Type
import           System.FilePath
import           System.Directory
import System.IO.Extra
import Data.Char


-- | Contains the typechecked module and the OrigNameCache entry for
-- that module.
data TcModuleResult = TcModuleResult
    { tmrModule     :: TypecheckedModule
    , tmrModInfo    :: HomeModInfo
    }
instance Show TcModuleResult where
    show = show . pm_mod_summary . tm_parsed_module . tmrModule

instance NFData TcModuleResult where
    rnf = rwhnf


-- | Get source span info, used for e.g. AtPoint and Goto Definition.
getSrcSpanInfos
    :: ParsedModule
    -> HscEnv
    -> [(Located ModuleName, Maybe NormalizedFilePath)]
    -> TcModuleResult
    -> IO [SpanInfo]
getSrcSpanInfos mod env imports tc =
    runGhcSession (Just mod) env
        . getSpanInfo imports
        $ tmrModule tc


-- | Given a string buffer, return a pre-processed @ParsedModule@.
parseModule
    :: IdeOptions
    -> HscEnv
    -> FilePath
    -> Maybe SB.StringBuffer
    -> IO ([FileDiagnostic], Maybe ParsedModule)
parseModule IdeOptions{..} env file =
    fmap (either (, Nothing) (second Just)) .
    -- We need packages since imports fail to resolve otherwise.
    runGhcSession Nothing env . runExceptT . parseFileContents optPreprocessor file


-- | Given a package identifier, what packages does it depend on
computePackageDeps
    :: HscEnv
    -> InstalledUnitId
    -> IO (Either [FileDiagnostic] [InstalledUnitId])
computePackageDeps env pkg = do
    let dflags = hsc_dflags env
    case lookupInstalledPackage dflags pkg of
        Nothing -> return $ Left [ideErrorText (toNormalizedFilePath noFilePath) $
            T.pack $ "unknown package: " ++ show pkg]
        Just pkgInfo -> return $ Right $ depends pkgInfo


-- | Typecheck a single module using the supplied dependencies and packages.
typecheckModule
    :: IdeOptions
    -> HscEnv
    -> [TcModuleResult]
    -> ParsedModule
    -> IO ([FileDiagnostic], Maybe TcModuleResult)
typecheckModule opt packageState deps pm =
    fmap (either (, Nothing) (second Just)) $
    runGhcSession (Just pm) packageState $
        catchSrcErrors $ do
            setupEnv deps
            (warnings, tcm) <- withWarnings $ \tweak ->
                GHC.typecheckModule pm{pm_mod_summary = tweak $ pm_mod_summary pm}
            tcm2 <- mkTcModuleResult (WriteInterface $ optWriteIface opt) tcm
            return (warnings, tcm2)

-- | Compile a single type-checked module to a 'CoreModule' value, or
-- provide errors.
compileModule
    :: ParsedModule
    -> HscEnv
    -> [TcModuleResult]
    -> TcModuleResult
    -> IO ([FileDiagnostic], Maybe CoreModule)
compileModule mod packageState deps tmr =
    fmap (either (, Nothing) (second Just)) $
    runGhcSession (Just mod) packageState $
        catchSrcErrors $ do
            setupEnv (deps ++ [tmr])

            let tm = tmrModule tmr
            session <- getSession
            (warnings,desugar) <- withWarnings $ \tweak -> do
                let pm = tm_parsed_module tm
                let pm' = pm{pm_mod_summary = tweak $ pm_mod_summary pm}
                let tm' = tm{tm_parsed_module  = pm'}
                GHC.dm_core_module <$> GHC.desugarModule tm'

            -- give variables unique OccNames
            (tidy, details) <- liftIO $ tidyProgram session desugar

            let core = CoreModule
                         (cg_module tidy)
                         (md_types details)
                         (cg_binds tidy)
                         (mg_safe_haskell desugar)

            return (warnings, core)


getGhcDynFlags :: ParsedModule -> HscEnv -> IO DynFlags
getGhcDynFlags mod pkg = runGhcSession (Just mod) pkg getSessionDynFlags

-- | Evaluate a GHC session using a new environment constructed with
-- the supplied options.
runGhcSession
    :: Maybe ParsedModule
    -> HscEnv
    -> Ghc a
    -> IO a
runGhcSession modu env act = runGhcEnv env $ do
    modifyDynFlags $ \x -> x
        {importPaths = nubOrd $ maybeToList (moduleImportPaths =<< modu) ++ importPaths x}
    act


moduleImportPaths :: GHC.ParsedModule -> Maybe FilePath
moduleImportPaths pm
  | rootModDir == "." = Just rootPathDir
  | otherwise =
    -- TODO (MK) stripSuffix (normalise rootModDir) (normalise rootPathDir)
    -- would be a better choice but at the moment we do not consistently
    -- normalize file paths in the Shake graph so we can end up with the
    -- same module being represented twice in the Shake graph.
    Just $ dropTrailingPathSeparator $ dropEnd (length rootModDir) rootPathDir
  where
    ms   = GHC.pm_mod_summary pm
    file = GHC.ms_hspp_file ms
    mod'  = GHC.ms_mod ms
    rootPathDir  = takeDirectory file
    rootModDir   = takeDirectory . moduleNameSlashes . GHC.moduleName $ mod'


newtype WriteInterface = WriteInterface Bool

mkTcModuleResult
    :: GhcMonad m
    => WriteInterface
    -> TypecheckedModule
    -> m TcModuleResult
mkTcModuleResult (WriteInterface writeIface) tcm = do
    session   <- getSession
    (iface,_) <- liftIO $ mkIfaceTc session Nothing Sf_None details tcGblEnv
    liftIO $ when writeIface $ do
        let path = ".interfaces" </> file tcm
        createDirectoryIfMissing True (takeDirectory path)
        writeIfaceFile (hsc_dflags session) (replaceExtension path ".hi") iface
        -- For now, we write .hie files whenever we write .hi files which roughly corresponds to
        -- when we are building a package. It should be easily decoupable if that turns out to be
        -- useful.
        hieFile <- runHsc session $ mkHieFile (tcModSummary tcm) tcGblEnv (fromJust $ renamedSource tcm)
        writeHieFile (replaceExtension path ".hie") hieFile
    let mod_info = HomeModInfo iface details Nothing
    return $ TcModuleResult tcm mod_info
  where
    file = ms_hspp_file . tcModSummary
    (tcGblEnv, details) = tm_internals_ tcm

tcModSummary :: TypecheckedModule -> ModSummary
tcModSummary = pm_mod_summary . tm_parsed_module

-- | Setup the environment that GHC needs according to our
-- best understanding (!)
setupEnv :: GhcMonad m => [TcModuleResult] -> m ()
setupEnv tms = do
    session <- getSession

    let mss = map (pm_mod_summary . tm_parsed_module . tmrModule) tms

    -- set the target and module graph in the session
    let graph = mkModuleGraph mss
    setSession session { hsc_mod_graph = graph }

    -- Make modules available for others that import them,
    -- by putting them in the finder cache.
    let ims  = map (InstalledModule (thisInstalledUnitId $ hsc_dflags session) . moduleName . ms_mod) mss
        ifrs = zipWith (\ms -> InstalledFound (ms_location ms)) mss ims
    liftIO $ modifyIORef (hsc_FC session) $ \fc ->
        foldl' (\fc (im, ifr) -> GHC.extendInstalledModuleEnv fc im ifr) fc
            $ zip ims ifrs

    -- load dependent modules, which must be in topological order.
    mapM_ loadModuleHome tms


-- | Load a module, quickly. Input doesn't need to be desugared.
-- A module must be loaded before dependent modules can be typechecked.
-- This variant of loadModuleHome will *never* cause recompilation, it just
-- modifies the session.
loadModuleHome
    :: (GhcMonad m)
    => TcModuleResult
    -> m ()
loadModuleHome tmr = modifySession $ \e ->
    e { hsc_HPT = addToHpt (hsc_HPT e) mod mod_info }
  where
    ms       = pm_mod_summary . tm_parsed_module . tmrModule $ tmr
    mod_info = tmrModInfo tmr
    mod      = ms_mod_name ms

-- | Produce a module summary from a StringBuffer.
getModSummaryFromBuffer
    :: GhcMonad m
    => FilePath
    -> SB.StringBuffer
    -> DynFlags
    -> GHC.ParsedSource
    -> ExceptT [FileDiagnostic] m ModSummary
getModSummaryFromBuffer fp contents dflags parsed = do
  (modName, imports) <- FindImports.getImportsParsed dflags parsed

  let modLoc = ModLocation
          { ml_hs_file  = Just fp
          , ml_hi_file  = replaceExtension fp "hi"
          , ml_obj_file = replaceExtension fp "o"
#ifndef GHC_STABLE
          , ml_hie_file = replaceExtension fp "hie"
#endif
          -- This does not consider the dflags configuration
          -- (-osuf and -hisuf, object and hi dir.s).
          -- However, we anyway don't want to generate them.
          }
      InstalledUnitId unitId = thisInstalledUnitId dflags
  return $ ModSummary
    { ms_mod          = mkModule (fsToUnitId unitId) modName
    , ms_location     = modLoc
    , ms_hs_date      = error "Rules should not depend on ms_hs_date"
    -- ^ When we are working with a virtual file we do not have a file date.
    -- To avoid silent issues where something is not processed because the date
    -- has not changed, we make sure that things blow up if they depend on the
    -- date.
    , ms_textual_imps = imports
    , ms_hspp_file    = fp
    , ms_hspp_opts    = dflags
    , ms_hspp_buf     = Just contents

    -- defaults:
    , ms_hsc_src      = HsSrcFile
    , ms_obj_date     = Nothing
    , ms_iface_date   = Nothing
#ifndef GHC_STABLE
    , ms_hie_date     = Nothing
#endif
    , ms_srcimps      = []        -- source imports are not allowed
    , ms_parsed_mod   = Nothing
    }

-- | Run CPP on a file
runCpp :: DynFlags -> FilePath -> Maybe SB.StringBuffer -> IO SB.StringBuffer
runCpp dflags filename contents = withTempDir $ \dir -> do
    let out = dir </> takeFileName filename <.> "out"
    case contents of
        Nothing -> do
            -- Happy case, file is not modified, so run CPP on it in-place
            -- which also makes things like relative #include files work
            -- and means location information is correct
            doCpp dflags True filename out
            liftIO $ SB.hGetStringBuffer out

        Just contents -> do
            -- Sad path, we have to create a version of the path in a temp dir
            -- __FILE__ macro is wrong, ignoring that for now (likely not a real issue)

            -- Relative includes aren't going to work, so we fix that by adding to the include path.
            let addSelf (IncludeSpecs quote global) = IncludeSpecs (takeDirectory filename : quote) global
            dflags <- return dflags{includePaths = addSelf $ includePaths dflags}

            -- Location information is wrong, so we fix that by patching it afterwards.
            let inp = dir </> "___HIE_CORE_MAGIC___"
            withBinaryFile inp WriteMode $ \h ->
                hPutStringBuffer h contents
            doCpp dflags True inp out

            -- Fix up the filename in lines like:
            -- # 1 "C:/Temp/extra-dir-914611385186/___HIE_CORE_MAGIC___"
            let tweak x
                    | Just x <- stripPrefix "# " x
                    , "___HIE_CORE_MAGIC___" `isInfixOf` x
                    , let num = takeWhile (not . isSpace) x
                    -- important to use /, and never \ for paths, even on Windows, since then C escapes them
                    -- and GHC gets all confused
                        = "# " <> num <> " \"" <> map (\x -> if isPathSeparator x then '/' else x) filename <> "\""
                    | otherwise = x
            stringToStringBuffer . unlines . map tweak . lines <$> readFileUTF8' out


-- | Given a buffer, flags, file path and module summary, produce a
-- parsed module (or errors) and any parse warnings.
parseFileContents
       :: GhcMonad m
       => (GHC.ParsedSource -> ([(GHC.SrcSpan, String)], GHC.ParsedSource))
       -> FilePath  -- ^ the filename (for source locations)
       -> Maybe SB.StringBuffer -- ^ Haskell module source text (full Unicode is supported)
       -> ExceptT [FileDiagnostic] m ([FileDiagnostic], ParsedModule)
parseFileContents preprocessor filename mbContents = do
   contents <- liftIO $ maybe (hGetStringBuffer filename) return mbContents
   let loc  = mkRealSrcLoc (mkFastString filename) 1 1
   dflags  <- ExceptT $ parsePragmasIntoDynFlags filename contents

   (contents, dflags) <-
      if not $ xopt LangExt.Cpp dflags then
          return (contents, dflags)
      else do
          contents <- liftIO $ runCpp dflags filename mbContents
          dflags <- ExceptT $ parsePragmasIntoDynFlags filename contents
          return (contents, dflags)

   case unP Parser.parseModule (mkPState dflags contents loc) of
     PFailed _ locErr msgErr ->
      throwE $ diagFromErrMsg dflags $ mkPlainErrMsg dflags locErr msgErr
     POk pst rdr_module ->
         let hpm_annotations =
               (Map.fromListWith (++) $ annotations pst,
                 Map.fromList ((noSrcSpan,comment_q pst)
                                  :annotations_comments pst))
             (warns, errs) = getMessages pst dflags
         in
           do
               -- Just because we got a `POk`, it doesn't mean there
               -- weren't errors! To clarify, the GHC parser
               -- distinguishes between fatal and non-fatal
               -- errors. Non-fatal errors are the sort that don't
               -- prevent parsing from continuing (that is, a parse
               -- tree can still be produced despite the error so that
               -- further errors/warnings can be collected). Fatal
               -- errors are those from which a parse tree just can't
               -- be produced.
               unless (null errs) $
                 throwE $ diagFromErrMsgs dflags $ snd $ getMessages pst dflags

               -- Ok, we got here. It's safe to continue.
               let (errs, parsed) = preprocessor rdr_module
               unless (null errs) $ throwE $ diagFromStrings errs
               ms <- getModSummaryFromBuffer filename contents dflags parsed
               let pm =
                     ParsedModule {
                         pm_mod_summary = ms
                       , pm_parsed_source = parsed
                       , pm_extra_src_files=[] -- src imports not allowed
                       , pm_annotations = hpm_annotations
                      }
                   warnings = diagFromErrMsgs dflags warns
               pure (warnings, pm)


-- | This reads the pragma information directly from the provided buffer.
parsePragmasIntoDynFlags
    :: GhcMonad m
    => FilePath
    -> SB.StringBuffer
    -> m (Either [FileDiagnostic] DynFlags)
parsePragmasIntoDynFlags fp contents = catchSrcErrors $ do
    dflags0  <- getSessionDynFlags
    let opts = Hdr.getOptions dflags0 contents fp
    (dflags, _, _) <- parseDynamicFilePragma dflags0 opts
    return dflags

-- | Run something in a Ghc monad and catch the errors (SourceErrors and
-- compiler-internal exceptions like Panic or InstallationError).
catchSrcErrors :: GhcMonad m => m a -> m (Either [FileDiagnostic] a)
catchSrcErrors ghcM = do
      dflags <- getDynFlags
      handleGhcException (ghcExceptionToDiagnostics dflags) $
        handleSourceError (sourceErrorToDiagnostics dflags) $
        Right <$> ghcM
    where
        ghcExceptionToDiagnostics dflags = return . Left . diagFromGhcException dflags
        sourceErrorToDiagnostics dflags = return . Left . diagFromErrMsgs dflags . srcErrorMessages
