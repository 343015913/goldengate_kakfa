package com.rogers.cdc.handlers;

//import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.api.mutations.MutationMapper;

//TODO this should be an interface
abstract public class Handler<Op, OpMapper extends MutationMapper<Op>> {
	protected OpMapper opMapper;
	protected Handler(OpMapper _opMapper){
		opMapper = _opMapper;
	}
	public abstract void processOp(Op op);
	public  abstract void flush();
	void handleAlter(Op op){
		assert false : "Producer::handleAlter() not implimented";
	}

}
