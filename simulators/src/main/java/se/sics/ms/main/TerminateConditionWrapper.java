package se.sics.ms.main;

import se.sics.ms.helper.FinalStateInfo;
import se.sics.ms.helper.FinalStateProcessor;

/**
 * Wrapper for the termination condition in the system.
 *
 * Created by babbar on 2015-09-25.
 */
public class TerminateConditionWrapper implements FinalStateInfo {

    public final FinalStateInfo finalState;
    public final int numNodes;
    public final FinalStateProcessor stateProcessor;

    public TerminateConditionWrapper(FinalStateInfo finalState, int numNodes, FinalStateProcessor stateProcessor){
        this.finalState = finalState;
        this.numNodes = numNodes;
        this.stateProcessor = stateProcessor;
    }

}
