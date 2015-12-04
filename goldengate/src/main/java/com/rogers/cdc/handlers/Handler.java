package com.rogers.cdc.handlers;


import com.rogers.cdc.api.mutations.MutationMapper;
/**
 * An abstract handler for CDC operations. 
 * A CDC framework (such as GoldenGate) can use it to submit change events/mutation 
 * A target system such as Kafka (or HDFS, FLume, etc) should extend this class to provide specific implementation
 * 
 */
abstract public class Handler<Op, Table, OpMapper extends MutationMapper<Op, Table>> {
	 /**  
	  * A MutationMapper implementation that maps source CDC system events to Mutations 
	  */
	protected OpMapper opMapper; 
	protected Handler(OpMapper _opMapper){
		opMapper = _opMapper;
	}
	public abstract void processOp(Op op);
	
	
	/**
	 *  CDC framework (such as GoldenGate) implement their own check-pointing mechanism to persist the last processed event.
	 * They will call flash to make sure that all events have actually been processed.
	 * Implementing classes should either (a)flush all the event to the persistent storage (for Example, Kafka should make sure that all messages have been commited)
     * or, (b) implement it's own local check-pointing mechanism.   
    */  
	public  abstract void flush();
	void handleAlter(Op op){
		assert false : "Producer::handleAlter() not implimented";
	}

}
