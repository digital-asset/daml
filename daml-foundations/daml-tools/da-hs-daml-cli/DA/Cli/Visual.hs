-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ApplicativeDo       #-}

-- | Main entry-point of the DAML compiler
module DA.Cli.Visual
  ( execVisual
  ) where


import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Ast.World as AST 
import DA.Daml.LF.Ast.Version
import qualified Data.NameMap as NM
-- import Debug.Trace
import qualified Data.Set as Set
import qualified DA.Pretty as DAP
import qualified DA.Daml.LF.Proto3.Archive as Archive
import qualified Data.ByteString.Lazy as BSL
import Text.Dot
import qualified Data.ByteString as B
import Control.Monad.Except
import Codec.Archive.Zip
import System.FilePath
import qualified Data.List as DL

data Action = ACreate (LF.Qualified LF.TypeConName) 
            | AExercise (LF.Qualified LF.TypeConName) LF.ChoiceName deriving (Eq, Ord, Show )


startFromUpdate :: Set.Set (LF.Qualified LF.ExprValName) -> LF.World -> LF.Update -> Set.Set Action
startFromUpdate seen world update = case update of 
    LF.UPure _ e -> startFromExpr seen world e
    LF.UBind (LF.Binding _ e1) e2 -> startFromExpr seen world e1 `Set.union` startFromExpr seen world e2
    LF.UCreate tpl e -> Set.singleton (ACreate tpl) `Set.union` startFromExpr seen world e
    LF.UExercise tpl chc e1 e2 e3 -> Set.singleton (AExercise tpl chc) `Set.union` startFromExpr seen world e1 `Set.union` maybe Set.empty (startFromExpr seen world) e2 `Set.union` startFromExpr seen world e3
    LF.UFetch _ ctIdEx -> startFromExpr seen world ctIdEx
    LF.UGetTime -> Set.empty
    LF.UEmbedExpr _ upEx -> startFromExpr seen world upEx
    LF.ULookupByKey _ -> Set.empty
    LF.UFetchByKey _ -> Set.empty

startFromExpr :: Set.Set (LF.Qualified LF.ExprValName) -> LF.World  -> LF.Expr -> Set.Set Action
startFromExpr seen world e = case e of
    LF.EVar _ -> Set.empty
    LF.EVal ref->  case LF.lookupValue ref world of 
        Right LF.DefValue{..}  
            | ref `Set.member` seen  -> Set.empty
            | otherwise -> startFromExpr (Set.insert ref seen)  world dvalBody
        Left _ -> error "This should not happen"
    LF.EBuiltin _ -> Set.empty
    LF.ERecCon _ flds -> Set.unions $ map (\(_, exp) -> startFromExpr seen world exp) flds
    LF.ERecProj _ _ recEx -> startFromExpr seen world recEx
    LF.ETupleUpd _ recExpr recUpdate -> startFromExpr seen world recExpr `Set.union` startFromExpr seen world recUpdate
    LF.EVariantCon _ _ varg -> startFromExpr seen world varg
    LF.ETupleCon tcon -> Set.unions $ map (\(_, exp) -> startFromExpr seen world exp) tcon
    LF.ETupleProj _ tupExpr -> startFromExpr seen world tupExpr
    LF.ERecUpd _ _ recExpr recUpdate -> startFromExpr seen world recExpr `Set.union` startFromExpr seen world recUpdate
    LF.ETmApp tmExpr tmpArg -> startFromExpr seen world tmExpr `Set.union` startFromExpr seen world tmpArg
    LF.ETyApp tAppExpr _ -> startFromExpr seen world tAppExpr
    LF.ETmLam _ tmlB -> startFromExpr seen world tmlB
    LF.ETyLam _ lambdy -> startFromExpr seen world lambdy
    LF.ECase cas casel -> startFromExpr seen world cas `Set.union` Set.unions ( map ( startFromExpr seen world . LF.altExpr ) casel)
    LF.ELet (LF.Binding _ e1) e2 -> startFromExpr seen  world e1 `Set.union` startFromExpr seen world e2
    LF.ENil _ -> Set.empty
    LF.ECons _ consH consT -> startFromExpr seen world consH `Set.union` startFromExpr seen world consT
    LF.ESome _ smBdy -> startFromExpr seen world smBdy
    LF.ENone _ -> Set.empty
    LF.EUpdate upd -> startFromUpdate seen world upd
    LF.EScenario _ -> Set.empty
    LF.ELocation _ e1 -> startFromExpr seen world e1
    -- x -> Set.unions $ map startFromExpr $ children x

startFromChoice :: LF.World -> LF.TemplateChoice -> Set.Set Action
startFromChoice world chc = startFromExpr Set.empty world (LF.chcUpdate chc)

templatePossibleUpdates :: LF.World -> LF.Template -> Set.Set Action
templatePossibleUpdates world tpl = Set.unions $ map (startFromChoice world) (NM.toList (LF.tplChoices tpl))

moduleAndTemplates :: LF.World -> LF.Module -> [(LF.TypeConName, Set.Set Action)]
moduleAndTemplates world mod = retTypess
    where 
        templates = templatesFromModule mod
        retTypess = map (\t-> (LF.tplTypeCon t, templatePossibleUpdates world t )) templates

listOfModules :: NM.NameMap LF.Module -> [LF.Module]
listOfModules modules = NM.toList modules

templatesFromModule :: LF.Module -> [LF.Template]
templatesFromModule mod = NM.toList $ LF.moduleTemplates mod

dalfsInDar :: Archive -> [BSL.ByteString]
dalfsInDar dar = [fromEntry e | e <- zEntries dar, ".dalf" `isExtensionOf` eRelativePath e]

dalfBytesToPakage :: BSL.ByteString -> (LF.PackageId, LF.Package)
dalfBytesToPakage bytes = case Archive.decodeArchive $ BSL.toStrict bytes of
    Right a -> a
    Left err -> error (show err)

dalfsToWorld :: [BSL.ByteString] -> LF.Package -> LF.World
dalfsToWorld dalfs pkg = AST.initWorldSelf pkgs version1_4 pkg
    where 
        pkgs = map dalfBytesToPakage dalfs

darToWorld :: FilePath -> LF.Package -> IO LF.World
darToWorld darFilePath pkg = do
    bytes <- B.readFile darFilePath
    let dalfs = dalfsInDar (toArchive $ BSL.fromStrict bytes)
    return (dalfsToWorld dalfs pkg )

prettyAction :: Action -> String
prettyAction (ACreate  (LF.Qualified _ _ tpl) )  = DAP.renderPretty tpl 
prettyAction (AExercise  (LF.Qualified _ _ tpl) _ ) = DAP.renderPretty tpl

-- prettyTemplateWithAction :: (LF.TypeConName ,DS.Set Action) -> String
-- prettyTemplateWithAction (tplCon, actions) =  DAP.renderPretty tplCon ++ "->" ++ show(DS.map prettyAction actions)

src :: String -> Dot NodeId
src label = node [ ("shape","none"),("label",label) ]

dotGraphTemplateAndActionHelper :: (LF.TypeConName, Set.Set Action) -> (String, [String])
dotGraphTemplateAndActionHelper (tplCon, actions) = (tplStr, actionsStrs)
    where 
        tplStr =  DAP.renderPretty tplCon
        actionsStrs = Set.elems $ Set.map prettyAction actions

dotTemplateNodes :: LF.TypeConName -> (String, Dot NodeId)
dotTemplateNodes tplCon = (DAP.renderPretty tplCon , src $ DAP.renderPretty tplCon)

nodeToDot :: NodeId -> String -> Dot ()
nodeToDot a b = do
    n2 <- src b
    a .->. n2

errorOnLeft :: Show a => String -> Either a b -> IO b
errorOnLeft desc = \case
  Left err -> ioError $ userError $ unlines [ desc, show err ]
  Right x  -> return x

execVisual :: FilePath -> FilePath -> IO ()
execVisual darFilePath dalfFile = do
    bytes <- B.readFile dalfFile
    (_, lfPkg) <- errorOnLeft "Cannot decode package" $ Archive.decodeArchive bytes 
    world <- darToWorld darFilePath lfPkg
    putStrLn "done"
    let modules = listOfModules $ LF.packageModules lfPkg
        res = concatMap (moduleAndTemplates world) modules
        -- ppString = map prettyTemplateWithAction res
        tpls =  map (dotTemplateNodes . fst) res
        dotThing = map dotGraphTemplateAndActionHelper res

    putStrLn $ showDot $ do
        attribute ("rankdir","LR")
        forM_ dotThing $ \(tplName, actions) -> do
            case DL.find (\t ->  fst t == tplName ) tpls of 
                Just (_, tplNode) -> do 
                    tNode <- tplNode
                    mapM (\a -> nodeToDot tNode a ) actions
                Nothing -> error "Unknow teplate referenced"



