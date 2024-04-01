def var wfact as integer.
update wfact.
For each factura WHERE factura.numfact = wfact no-lock:
    disp factura.poliza 
        factura.ref 
        factura.numfact 
        factura.endoso 
        factura.folio 
        factura.folio_imp 
        factura.folrem 
        factura.consec-endoso 
        factura.st-factura 
        factura.t-mov factura.tipfact.
     
    
end. 
