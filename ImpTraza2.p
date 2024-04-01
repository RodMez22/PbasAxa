session:date-format = "dmy".

DEFINE TEMP-TABLE TmpDeltas
    FIELD TmpSerie   AS CHARACTER FORMAT "x(5)"
    FIELD TmpFolEdi  AS decimal   
    FIELD TmpNoPoli  AS CHARACTER FORMAT "x(14)"
    FIELD TmpNoEndo  AS CHARACTER FORMAT "x(10)"
    FIELD TmpFolio   AS CHARACTER FORMAT "x(10)"
    FIELD TmpUnico   AS CHARACTER FORMAT "x(10)"
    FIELD FolPoliza  AS CHARACTER FORMAT "x(12)"   
    FIELD FolEndoso  AS CHARACTER FORMAT "x(12)" 
    FIELD FolTipFac  AS CHARACTER FORMAT "x(3)"
    FIELD FolStaFol  AS CHARACTER FORMAT "x(20)"
    FIELD FolStaPag  AS LOGICAL   FORMAT "Si/No"
    FIELD FolStaEnv  AS INTEGER   FORMAT "9"
    FIELD FolFecCre  AS DATE
    FIELD FolFecEnv  AS DATE
    FIELD FolIntern  AS INTEGER   FORMAT ">>>>>>9"
    FIELD FolNumFac  AS INTEGER   FORMAT ">>>>>>>9"
    FIELD FolNumErr  AS INTEGER   FORMAT ">9"        
    FIELD TraSerie   AS CHARACTER FORMAT "x(5)"
    FIELD TraFolio   AS decimal   
    FIELD TraStaFol  AS CHARACTER FORMAT "x(20)"
    FIELD TraFecCre  AS DATE
    FIELD TraFecEnv  AS DATE
    FIELD TraStaPag  AS LOGICAL   FORMAT "Si/No" 
    FIELD TraPoliza  AS CHARACTER FORMAT "x(12)"   
    FIELD TraEndoso  AS CHARACTER FORMAT "x(12)" 
    FIELD TraNumFac  AS INTEGER   FORMAT ">>>>>>>9"
    FIELD TraIntern  AS INTEGER   FORMAT ">>>>>>9"
    FIELD PNeta      AS DECIMAL   FORMAT "->>>,>>>,>>9.99"
    FIELD Imp-Reduc  AS DECIMAL   FORMAT "->>>,>>>,>>9.99"
    FIELD Recargo    AS DECIMAL   FORMAT "->>>,>>>,>>9.99"
    FIELD Derechos   AS DECIMAL   FORMAT "->>>,>>>,>>9.99"
    FIELD Impuesto   AS DECIMAL   FORMAT "->>>,>>>,>>9.99"
    FIELD PTotal     AS DECIMAL   FORMAT "->>>,>>>,>>9.99"
    FIELD Comision   AS DECIMAL   FORMAT "->>>,>>>,>>9.99" 
    FIELD TraTipo    AS CHARACTER FORMAT "x(5)"
    FIELD OutVig     AS CHARACTER FORMAT "x(2)"
    FIELD Brinco     AS CHARACTER FORMAT "x(2)"
    FIELD TraFecIni  AS DATE
    FIELD TraFecFin  AS DATE
    .
 
DEFINE BUFFER BufFoliosSat for FoliosSat.    
DEFINE temp-table BufTmpDeltas like TmpDeltas.

DEFINE VARIABLE wconta     AS INTEGER                       no-undo.
DEFINE VARIABLE wtotal     AS INTEGER                       no-undo.
DEFINE VARIABLE vchrStCFD1 AS CHARACTER FORMAT "X(16)"      no-undo.
DEFINE VARIABLE wFolioSat  LIKE TmpDeltas.TraFolio          no-undo.
DEFINE VARIABLE wFolioSat2 LIKE TmpDeltas.TraFolio          no-undo.
DEFINE VARIABLE wdelta     AS CHARACTER FORMAT "X(5)"       no-undo.
DEFINE VARIABLE wSinVi     AS CHARACTER FORMAT "X(2)"       no-undo.
DEFINE VARIABLE wBrinco    AS CHARACTER FORMAT "X(2)"       no-undo.


STATUS DEFAULT "Importando.....".    
INPUT FROM "v:\Deltas_Trazabilidad_20230807.csv".
/*do wconta = 1 to 7000 :*/ 
REPEAT: 
  CREATE TmpDeltas.
  IMPORT DELIMITER "," TmpDeltas no-error.
  wtotal = wtotal + 1.
END.
/*wconta = 0. */ 
INPUT CLOSE. 

OUTPUT TO v:\ResulDeltas2.txt.
EXPORT DELIMITER "|" 
        "Delta Serie" "Delta Folio Edi" "Delta Poliza" "Delta Endoso" "Delta Folio" "Delta Unico"
        "CFD Poliza" "CFD Endoso" "CFD TipFact" "CFD Stat Folio" "CFD Stat Pagado" "CFD Stat Enviado" "CFD Fec Crea" 
            "CFD Fec Envio" "CFD Folio Interno" "CFD No. Factura" "CFD No.Error"
        "Traza Serie" "Traza Folio" "Traza Stat Folio" "Traza Fec Crea" "Traza Fec Env" "Traza Stat Pagado"
            "Traza Poliza" "Traza Endoso" "Traza No Factura" "Traza Folio Interno"
        "Fact PNeta" "Fact Imp-Reducción" "Fact Recargo" "Fact Derechos" "Fact Impuesto" "Fact PTotal" "Fact Comision"
        "Delta" "Sin Vigencia" "Brinco A¤o" "Fec Ini Vig" "Fec Fin Vig".
            
FOR EACH TmpDeltas :
    wconta = wconta + 1.
    STATUS DEFAULT "Procesando Reg " + string(wconta,">,>>>,>>>") + " de " + string(wtotal,">,>>>,>>>"). 
    if TmpDeltas.TmpFolEdi = 0 THEN next.

    FIND FIRST foliosSat WHERE foliosSat.SerieSAT = TmpDeltas.TmpSerie 
                           AND foliosSat.FolioSAT = TmpDeltas.TmpFolEdi NO-LOCK NO-ERROR.
    
    IF AVAILABLE foliosSat THEN DO :
       FIND FIRST poliza where poliza.poliza = FoliosSAT.Poliza no-lock no-error.
       FIND FIRST claves WHERE claves.ramo     =      "10" 
                           AND claves.cvecampo = "EstFolioSat"  
                           AND claves.statcve  = FoliosSat.EstFolioSat NO-LOCK NO-ERROR.
       vchrStCFD1 = string(FoliosSat.EstFolioSat,">9 ") + 
                    IF AVAILABLE claves THEN ENTRY(2,Claves.Descrip,"|") ELSE "".
              
       ASSIGN TmpDeltas.FolPoliza    = FoliosSAT.Poliza  
              TmpDeltas.FolEndoso    = FoliosSAT.Endoso
              TmpDeltas.FolTipFac    = FoliosSAT.tipfact
              TmpDeltas.FolStaFol    = vchrStCFD1 
              TmpDeltas.FolStaPag    = FoliosSAT.EstFolioPagado
              TmpDeltas.FolStaEnv    = FoliosSAT.EstFolioEnvio
              TmpDeltas.FolFecCre    = FoliosSAT.Fec_Creacion
              TmpDeltas.FolFecEnv    = FoliosSAT.Fec_Envio 
              TmpDeltas.FolIntern    = FoliosSAT.FolioInt
              TmpDeltas.FolNumFac    = FoliosSAT.numfact
              TmpDeltas.FolNumErr    = FoliosSAT.NumError.
       if available poliza then assign TmpDeltas.TraFecIni = poliza.fec-ini
                                       TmpDeltas.TraFecFin = poliza.fec-ter.       
              
       FIND FIRST BufFoliosSat WHERE BufFoliosSat.Poliza    = FoliosSAT.Poliza   AND
                                     BufFoliosSat.NumFact   = FoliosSAT.NumFact  /* AND
                                     BufFoliosSat.FolioSat <> FoliosSat.FolioSat */ NO-LOCK NO-ERROR.
       IF AVAILABLE BufFoliosSat then DO :
          FIND FIRST claves WHERE claves.ramo     =      "10" 
                              AND claves.cvecampo = "EstFolioSat"  
                              AND claves.statcve  = BufFoliosSat.EstFolioSat NO-LOCK NO-ERROR.
          vchrStCFD1 = string(BufFoliosSat.EstFolioSat,">9 ") +
                       IF AVAILABLE claves THEN ENTRY(2,Claves.Descrip,"|") ELSE "".
     
          ASSIGN TmpDeltas.TraSerie  = BufFoliosSat.SerieSat
                 TmpDeltas.TraFolio  = BufFoliosSat.FolioSAT 
                 TmpDeltas.TraStaFol = vchrStCFD1
                 TmpDeltas.TraFecCre = BufFoliosSat.Fec_Creacion
                 TmpDeltas.TraFecEnv = BufFoliosSat.Fec_Envio
                 TmpDeltas.TraStaPag = BufFoliosSat.EstFolioPagado
                 TmpDeltas.TraPoliza = BufFoliosSAT.Poliza   
                 TmpDeltas.TraEndoso = BufFoliosSAT.Endoso
                 TmpDeltas.TraIntern = BufFoliosSAT.FolioInt
                 TmpDeltas.TraNumFac = BufFoliosSAT.numfact.
                 wFolioSat2 = BufFoliosSat.FolioSAT.                                 
          Run Facturas (1).
       END.
       
    END.          
    EXPORT DELIMITER "|"  TmpDeltas.
    
    IF not AVAILABLE foliosSat THEN next.
    
    wFolioSat = TmpDeltas.TraFolio.
    empty temp-table BufTmpDeltas.
    CREATE BufTmpDeltas.
    BUFFER-COPY TmpDeltas EXCEPT TmpDeltas.FolPoliza TmpDeltas.FolEndoso TmpDeltas.FolTipFac
                                 TmpDeltas.FolStaFol TmpDeltas.FolStaPag TmpDeltas.FolStaEnv
                                 TmpDeltas.FolFecCre TmpDeltas.FolFecEnv TmpDeltas.FolIntern
                                 TmpDeltas.FolNumFac TmpDeltas.FolNumErr TmpDeltas.TraTipo 
                          TO BufTmpDeltas.

    IF TmpDeltas.TmpSerie = "GMD" OR TmpDeltas.TmpSerie = "GMC" THEN DO :
        if TmpDeltas.TmpSerie = "GMD" 
        then FIND FIRST BufFoliosSat WHERE ROWID(BufFoliosSat) = ROWID(foliosSat) NO-LOCK NO-ERROR.
        else do :
            if wFolioSat2 = FoliosSat.FolioSat 
            then FIND FIRST BufFoliosSat WHERE BufFoliosSat.Poliza    = FoliosSAT.Poliza   AND
                                               BufFoliosSat.NumFact   = FoliosSAT.NumFact  AND
                                               BufFoliosSat.FolioSat <> FoliosSat.FolioSat NO-LOCK no-error.
            else FIND FIRST BufFoliosSat WHERE ROWID(BufFoliosSat) = ROWID(foliosSat) NO-LOCK NO-ERROR.
        end.
        if available BufFoliosSat then Run GeneraDetalle.
    END.
    ELSE DO :
       FOR EACH BufFoliosSat WHERE BufFoliosSat.Poliza    = FoliosSAT.Poliza   AND
                                   BufFoliosSat.NumFact   = FoliosSAT.NumFact  AND
                                   BufFoliosSat.FolioSat <> wFolioSat NO-LOCK :
           Run GeneraDetalle.
       END.
    END.

END.
OUTPUT CLOSE.

MESSAGE wconta skip VIEW-AS ALERT-BOX.


PROCEDURE Facturas:
    define input parameter piproceso AS INTEGER.
    
    assign wdelta  = (if BufFoliosSat.FolioSAT = TmpDeltas.TmpFolEdi then "Delta" else "")     
           wSinVi  = ""
           wBrinco = "".

    if available poliza then do :
        if (BufFoliosSat.Fec_Creacion > poliza.fec-ter or
            BufFoliosSat.Fec_Envio    > poliza.fec-ter)
        then wSinVi = "Si".
        if year(BufFoliosSat.Fec_Creacion) > year(poliza.fec-ter)  or
           year(BufFoliosSat.Fec_Envio)    > year(poliza.fec-ter)  
        then wBrinco = "Si".
    end.        
   
    if piproceso = 1 then assign TmpDeltas.TraTipo    = wdelta
                                 TmpDeltas.OutVig     = wSinVi
                                 TmpDeltas.Brinco     = wBrinco.
                     else assign BufTmpDeltas.TraTipo = wdelta
                                 BufTmpDeltas.OutVig  = wSinVi
                                 BufTmpDeltas.Brinco  = wBrinco.
        
    FIND FIRST factura WHERE Factura.poliza  = BufFoliosSAT.Poliza
                         AND Factura.endoso  = BufFoliosSAT.Endoso
                         AND factura.numfact = BufFoliosSAT.numfact NO-LOCK NO-ERROR.
    IF NOT AVAILABLE factura 
    then FIND FIRST factura WHERE factura.numfact = BufFoliosSAT.numfact NO-LOCK NO-ERROR.               
    IF AVAILABLE factura THEN DO: 
       if piproceso = 1 then ASSIGN TmpDeltas.PNeta     = Factura.PNeta[1]
                                    TmpDeltas.Imp-Reduc = Factura.Impte-Reduc
                                    TmpDeltas.Recargo   = Factura.Recargo
                                    TmpDeltas.Derechos  = Factura.Derecho
                                    TmpDeltas.Impuesto  = Factura.Impuesto
                                    TmpDeltas.PTotal    = Factura.PTotal
                                    TmpDeltas.Comision  = Factura.Comis[1] + Factura.Comis[2] + Factura.Comis[3].
                        else ASSIGN BufTmpDeltas.PNeta     = Factura.PNeta[1]
                                    BufTmpDeltas.Imp-Reduc = Factura.Impte-Reduc
                                    BufTmpDeltas.Recargo   = Factura.Recargo
                                    BufTmpDeltas.Derechos  = Factura.Derecho
                                    BufTmpDeltas.Impuesto  = Factura.Impuesto
                                    BufTmpDeltas.PTotal    = Factura.PTotal
                                    BufTmpDeltas.Comision  = Factura.Comis[1] + Factura.Comis[2] + Factura.Comis[3].          
    END.   
       
END PROCEDURE.
    
    
PROCEDURE GeneraDetalle:
    FIND FIRST claves WHERE claves.ramo     =      "10" 
                        AND claves.cvecampo = "EstFolioSat"  
                        AND claves.statcve  = BufFoliosSat.EstFolioSat NO-LOCK NO-ERROR.
    vchrStCFD1 = string(BufFoliosSat.EstFolioSat,">9 ") +
                 IF AVAILABLE claves THEN ENTRY(2,Claves.Descrip,"|") ELSE "".
    ASSIGN BufTmpDeltas.TraSerie  = BufFoliosSat.SerieSat
           BufTmpDeltas.TraFolio  = BufFoliosSat.FolioSAT 
           BufTmpDeltas.TraStaFol = vchrStCFD1
           BufTmpDeltas.TraFecCre = BufFoliosSat.Fec_Creacion
           BufTmpDeltas.TraFecEnv = BufFoliosSat.Fec_Envio
           BufTmpDeltas.TraStaPag = BufFoliosSat.EstFolioPagado
           BufTmpDeltas.TraPoliza = BufFoliosSAT.Poliza   
           BufTmpDeltas.TraEndoso = BufFoliosSAT.Endoso
           BufTmpDeltas.TraIntern = BufFoliosSAT.FolioInt
           BufTmpDeltas.TraNumFac = BufFoliosSAT.numfact.
    Run Facturas (2).                                        
    EXPORT DELIMITER "|"  BufTmpDeltas.   
END PROCEDURE.

    
