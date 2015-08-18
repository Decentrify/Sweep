

package se.sics.ms.main;

import se.sics.kompics.Init;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 * Main initialization package for the
 * simulator.
 *
 * Created by babbar on 2015-08-18.
 */
public class SimulatorHostCompIInit extends Init<SimulatorHostComp>{

    public DecoratedAddress ccAddress;

    public SimulatorHostCompIInit(DecoratedAddress ccAddress){
        this.ccAddress = ccAddress;
    }

}
