package com.goldengate.delivery.handler.kafka.operations;
                                            
//TODO: Remove
import com.goldengate.atg.datasource.DsOperation.OpType;                                              

/**
 * Factory class to get the operation handler based on the specific operation type.                   
 * @author Tom Campbell                                                                               
 */
public class OperationFactory {
    private InsertOperationHandler insertOperation;                                                        
    private DeleteOperationHandler deleteOperation;                                                        
    private UpdateOperationHandler updateOperation;
    private PkUpdateOperationHandler pkupdateOperation;                                                    
    
    /**
     * Initialization method.  Used to initialize all of the supported handlers.                      
     * @param od The operation data object.                                                           
     */
    public void init(){
        /*insertOperation = new InsertOperationHandler();                                                                                                             
        
        deleteOperation = new DeleteOperationHandler();                                                                                                           
        
        updateOperation = new UpdateDBOperation();                                                                                                            
        
        pkupdateOperation = new PKUpdateDBOperation(); */                                                                                                      
    }                                                                                                 
    
    /**
     * Method to get the operation handler based on the input operation type.                         
     * @param ot The operation type.
     * @return The operation handler for the input type.  Null if the type is                         
     * not supported.                                                                                 
     */
    
    public OperationFactory getDBOperation(OpType ot){                                             
        /*if (ot == OpType.DO_INSERT){                                                                  
            return insertOperation;                                                                   
        }
        if (ot == OpType.DO_DELETE){                                                                  
            return deleteOperation;                                                                   
        }
        if ((ot == OpType.DO_UPDATE) || (ot == OpType.DO_UPDATE_FIELDCOMP) ||                         
                (ot == OpType.DO_UPDATE_AC)){                                                         
            return updateOperation;                                                                   
        }
        if (ot == OpType.DO_UPDATE_FIELDCOMP_PK){                                                     
            return pkupdateOperation;                                                                 
        }                                                                                             
       */ 
        return null;                                                                                  
    }                                                                                                 
}