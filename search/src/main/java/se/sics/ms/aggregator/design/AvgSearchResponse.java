package se.sics.ms.aggregator.design;

import se.sics.ktoolbox.aggregator.global.api.system.DesignInfo;

/**
 * Average Search Response for the cluster for a particular window.
 *
 * Created by babbar on 2015-09-06.
 */
public class AvgSearchResponse implements DesignInfo{


    private float srTotal;
    private int countNodes;

    public AvgSearchResponse(float srTotal, int countNodes){
        this.srTotal = srTotal;
        this.countNodes = countNodes;
    }

    @Override
    public String toString() {
        return "AvgSearchResponse{" +
                "srTotal=" + srTotal +
                ", countNodes=" + countNodes +
                '}';
    }


    public float getAverage(){
        return countNodes > 0 ? (srTotal / countNodes) : 0;
    }

    public float getSrTotal() {
        return srTotal;
    }

    public int getCountNodes() {
        return countNodes;
    }
}
