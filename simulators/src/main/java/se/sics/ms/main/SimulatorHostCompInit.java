

package se.sics.ms.main;

import com.typesafe.config.Config;
import se.sics.kompics.Init;
import se.sics.p2ptoolbox.util.config.SystemConfig;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 * Main initialization package for the
 * simulator.
 *
 * Created by babbar on 2015-08-18.
 */
public class SimulatorHostCompInit extends Init<SimulatorHostComp>{

    public DecoratedAddress ccAddress;
    public Config config;
    public SystemConfig systemConfig;

    public SimulatorHostCompInit ( DecoratedAddress ccAddress, SystemConfig systemConfig, Config config){

        this.systemConfig = systemConfig;
        this.ccAddress = ccAddress;
        this.config = config;
    }

}
