
package se.sics.ms.main;

import se.sics.kompics.Init;

/**
 * Init for the main aggregator host component.
 * Created by babbar on 2015-09-18.
 */
public class AggregatorHostCompInit extends Init<AggregatorHostComp>{

    public long timeout;
    public final String fileLocation;
    public final TerminateConditionWrapper conditionWrapper;

    public AggregatorHostCompInit(long timeout, String fileLocation, TerminateConditionWrapper conditionWrapper){
        this.timeout = timeout;
        this.fileLocation = fileLocation;
        this.conditionWrapper = conditionWrapper;
    }
}
