-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Compiler.Repl (runRepl) where

import qualified "zip-archive" Codec.Archive.Zip as Zip
import Control.Exception hiding (TypeError)
import Control.Monad
import Control.Monad.Except
import Control.Monad.Trans.Maybe
import qualified               DA.Daml.LF.Ast as LF
import qualified               DA.Daml.LF.Proto3.Archive as LFArchive
import DA.Daml.LF.Reader (readDalfs, Dalfs(..))
import qualified               DA.Daml.LF.ReplClient as ReplClient
import DA.Daml.LFConversion.UtilGHC
import DA.Daml.Options.Types
import Data.Bifunctor (first)
import qualified               Data.ByteString.Lazy as BSL
import Data.Foldable
import Data.Maybe
import qualified               Data.NameMap as NM
import Data.Text (Text)
import qualified               Data.Text as T
import Development.IDE.Core.API
import Development.IDE.Core.Rules
import Development.IDE.Core.RuleTypes
import Development.IDE.Core.RuleTypes.Daml
import Development.IDE.Core.Shake
import Development.IDE.GHC.Util
import Development.IDE.Types.Location
import ErrUtils
import GHC
import HsExpr (Stmt, StmtLR(..), LHsExpr)
import HsExtension (GhcPs, GhcTc)
import HsPat (Pat(..))
import HscTypes (HscEnv(..))
import Language.Haskell.GhclibParserEx.Parse
import Lexer (ParseResult(..))
import OccName (occName, occNameFS)
import Outputable (ppr, showSDoc)
import SrcLoc (unLoc)
import System.Exit
import System.IO.Error
import System.IO.Extra
import Type

data Error
    = ParseError MsgDoc
    | UnsupportedStatement String -- ^ E.g., pattern on the LHS
    | TypeError -- ^ The actual error will be in the diagnostics
    | ScriptError ReplClient.BackendError

renderError :: DynFlags -> Error -> IO ()
renderError dflags err = case err of
    ParseError err ->
        putStrLn (showSDoc dflags err)
    (UnsupportedStatement str) ->
        putStrLn ("Unsupported statement: " <> str)
    TypeError ->
        -- ^ The error will be displayed via diagnostics.
        pure ()
    (ScriptError _err) ->
        -- ^ The error will be displayed by the script runner.
        pure ()

-- | Split a statement into the name of the binder (patterns are not supported)
-- and the body. For unsupported statements we return `Nothing`.
splitStmt :: Stmt GhcPs (LHsExpr GhcPs) -> Maybe (Maybe Text, LHsExpr GhcPs)
splitStmt (BodyStmt _ expr _ _) = Just (Nothing, expr)
splitStmt (BindStmt _ pat expr _ _)
  -- TODO Support more complex patterns
  | VarPat _ (unLoc -> id) <- unLoc pat =
        let bind = (fsToText . occNameFS . occName) id
        in Just (Just bind, expr)
splitStmt _ = Nothing

runRepl :: Options -> FilePath -> ReplClient.Handle -> IdeState -> IO ()
runRepl opts mainDar replClient ideState = do
    Right Dalfs{..} <- readDalfs . Zip.toArchive <$> BSL.readFile mainDar
    (_, pkg) <- either (fail . show) pure (LFArchive.decodeArchive LFArchive.DecodeAsMain (BSL.toStrict mainDalf))
    let moduleNames = map LF.moduleName (NM.elems (LF.packageModules pkg))
    Just pkgs <- runAction ideState (use GeneratePackageMap "Dummy.daml")
    Just stablePkgs <- runAction ideState (use GenerateStablePackages "Dummy.daml")
    for_ (toList pkgs <> toList stablePkgs) $ \pkg -> do
        r <- ReplClient.loadPackage replClient (LF.dalfPackageBytes pkg)
        case r of
            Left err -> do
                hPutStrLn stderr ("Package could not be loaded: " <> show err)
                exitFailure
            Right _ -> pure ()
    go moduleNames 0 []
  where
    handleLine
        :: [LF.ModuleName]
        -> [(Text, Type)]
        -> DynFlags
        -> String
        -> Int
        -> IO (Either Error (Maybe Text, Type))
    handleLine moduleNames binds dflags l i = runExceptT $ do
        stmt <- case parseStatement l dflags of
            POk _ lStmt -> pure (unLoc lStmt)
            PFailed _ _ errMsg -> throwError (ParseError errMsg)
        (mbBind, expr) <- maybe (throwError (UnsupportedStatement l)) pure (splitStmt stmt)
        liftIO $ writeFileUTF8 (fromNormalizedFilePath $ lineFilePath i)
            (renderModule dflags moduleNames i binds expr)
        -- Useful for debugging, probably best to put it behind a --debug flag
        -- rendered <- liftIO  $readFileUTF8 (fromNormalizedFilePath $ lineFilePath i)
        -- liftIO $ for_ (lines rendered) $ \line ->
        --     hPutStrLn stderr ("> " <> line)
        (lfMod, tmrModule -> tcMod) <-
            maybe (throwError TypeError) pure =<< liftIO (runAction ideState $ runMaybeT $
            (,) <$> useE GenerateDalf (lineFilePath i)
                <*> useE TypeCheck (lineFilePath i))
        -- Type of the statement so we can give it a type annotation
        -- and avoid incurring a typeclass constraint.
        stmtTy <- maybe (throwError TypeError) pure (exprTy $ tm_typechecked_source tcMod)
        scriptRes <- liftIO $ ReplClient.runScript replClient (optDamlLfVersion opts) lfMod
        case scriptRes of
            Right _ -> pure (mbBind, stmtTy)
            Left err -> throwError (ScriptError err)
    go :: [LF.ModuleName] -> Int -> [(T.Text, Type)] -> IO ()
    go moduleNames !i !binds = do
         putStr "daml> "
         hFlush stdout
         l <- catchJust (guard . isEOFError) getLine (const exitSuccess)
         dflags <-
             hsc_dflags . hscEnv <$>
             runAction ideState (use_ GhcSession $ lineFilePath i)
         r <- handleLine moduleNames binds dflags l i
         case r of
             Left err -> do
                 renderError dflags err
                 -- If we get an error we don’t increment i and we
                 -- do not get a new binding
                 go moduleNames i binds
             Right (mbBind, ty) -> do
                 let shadow bind
                       | Just newBind  <- mbBind, bind == newBind = "_"
                       | otherwise = bind
                 go moduleNames (i + 1 :: Int) (map (first shadow) binds <> [(fromMaybe "_" mbBind, ty)])

exprTy :: LHsBinds GhcTc -> Maybe Type
exprTy binds = listToMaybe
    [ argTy
    | FunBind{..} <- map unLoc (concatMap expand $ toList binds)
    , getOccText fun_id == "expr"
    , (_, [argTy]) <- [(splitTyConApp . mg_res_ty . mg_ext) fun_matches]
    ]

expand :: LHsBindLR id id -> [LHsBindLR id id]
expand (unLoc -> AbsBinds{..}) = toList abs_binds
expand bind = [bind]

lineFilePath :: Int -> NormalizedFilePath
lineFilePath i = toNormalizedFilePath $ "Line" <> show i <> ".daml"

lineModuleName :: Int -> String
lineModuleName i = "Line" <> show i

renderModule :: DynFlags -> [LF.ModuleName] -> Int -> [(Text, Type)] -> LHsExpr GhcPs -> String
renderModule dflags imports line binds expr = unlines $
     [ "{-# OPTIONS_GHC -Wno-unused-imports -Wno-partial-type-signatures #-}"
     , "{-# LANGUAGE PartialTypeSignatures #-}"
     , "daml 1.2"
     , "module " <> lineModuleName line <> " where"
     , "import Prelude hiding (submit)"
     , "import Daml.Script"
     ] <>
     map (\moduleName -> T.unpack $ "import " <> LF.moduleNameString moduleName) imports <>
     [ "expr : " <> concatMap (renderTy . snd) binds <> "Script _"
     , "expr " <> unwords (map renderBind binds) <> " = " <> prettyPrint expr
     ]
  where renderBind (name, ty) = "(" <> T.unpack name <> " : " <> showSDoc dflags (ppr ty) <> ")"
        renderTy ty = showSDoc dflags (ppr ty) <> " -> "

