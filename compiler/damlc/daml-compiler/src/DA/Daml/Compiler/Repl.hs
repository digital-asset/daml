-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Compiler.Repl (runRepl) where

import qualified "zip-archive" Codec.Archive.Zip as Zip
import Control.Exception
import Control.Monad
import qualified               DA.Daml.LF.Ast as LF
import qualified               DA.Daml.LF.Proto3.Archive as LFArchive
import DA.Daml.LF.Reader (readDalfs, Dalfs(..))
import qualified               DA.Daml.LF.ReplClient as ReplClient
import DA.Daml.LFConversion.UtilGHC
import DA.Daml.Options.Types
import qualified               Data.ByteString.Lazy as BSL
import Data.Data (toConstr)
import Data.Foldable
import Data.Maybe
import qualified               Data.NameMap as NM
import Data.Text (Text)
import qualified               Data.Text as T
import Development.IDE.Core.API
import Development.IDE.Core.RuleTypes
import Development.IDE.Core.RuleTypes.Daml
import Development.IDE.Core.Shake
import Development.IDE.GHC.Util
import Development.IDE.Types.Location
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
    go :: [LF.ModuleName] -> Int -> [(T.Text, Type)] -> IO ()
    go moduleNames !i !binds = do
         putStr "daml> "
         hFlush stdout
         l <- catchJust (guard . isEOFError) getLine (const exitSuccess)
         dflags <-
             hsc_dflags . hscEnv <$>
             runAction ideState (use_ GhcSession $ lineFilePath i)
         POk _ (unLoc -> stmt) <- pure (parseStatement l dflags)
         let !(mbBind, expr) = fromMaybe (fail ("Unsupported statement type: " <> show (toConstr stmt))) (splitStmt stmt)
         writeFileUTF8 (fromNormalizedFilePath $ lineFilePath i)
             (renderModule dflags moduleNames i binds expr)
         -- Useful for debugging, probably best to put it behind a --debug flag
         -- rendered <- readFileUTF8 (fromNormalizedFilePath $ lineFilePath i)
         -- for_ (lines rendered) $ \line ->
         --     hPutStrLn stderr ("> " <> line)

         -- TODO Handle failures here cracefully instead of
         -- tearing down the whole process.
         Just lfMod <- runAction ideState $ use GenerateDalf (lineFilePath i)
         Just (tmrModule -> tcMod) <- runAction ideState $ use TypeCheck (lineFilePath i)
         -- We need type annotations to avoid things becoming polymorphic.
         -- If we end up with a typeclass constraint on `expr` things
         -- will go wrong.
         Just ty <- pure $ exprTy $ tm_typechecked_source tcMod

         r <- ReplClient.runScript replClient (optDamlLfVersion opts) lfMod
         case r of
             Right _ -> pure ()
             Left err -> do
                 hPutStrLn stderr ("Script produced an error: " <> show err)
                 -- TODO don’t kill the whole process
                 exitFailure

         let shadow bind
               | Just newBind  <- mbBind, bind == newBind = "_"
               | otherwise = bind
         go moduleNames (i + 1 :: Int) (map (\(bind, ty) -> (shadow bind, ty)) binds <> [(fromMaybe "_" mbBind, ty)])

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

