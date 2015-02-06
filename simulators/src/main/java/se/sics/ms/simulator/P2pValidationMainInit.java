package se.sics.ms.simulator;

import se.sics.gvod.net.VodAddress;
import se.sics.kompics.Init;

/**
 * Created by babbarshaer on 2015-02-04.
 * 
 * Initialization class for the P2pValidatorMain.
 */
public class P2pValidationMainInit extends Init<P2pValidatorMain>{

    private final int rto;
    private final int rtoTries;
    private final double rtoScale;
    private final VodAddress self;
    
    public P2pValidationMainInit(int rto, int rtoTries, double rtoScale, VodAddress self){
        this.rto = rto;
        this.rtoTries = rtoTries;
        this.rtoScale = rtoScale;
        this.self = self;
    }


    public double getRtoScale() {
        return rtoScale;
    }

    public int getRto() {
        return this.rto;
    }

    public int getRtoTries() {
        return rtoTries;
    }

    public VodAddress getSelf(){
        return this.self;
    }
}
