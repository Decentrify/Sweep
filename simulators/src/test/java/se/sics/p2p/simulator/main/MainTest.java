package se.sics.p2p.simulator.main;

import org.junit.Test;

/**
 * Main Class for test execution.
 * Created by babbarshaer on 2015-02-04.
 */

import se.sics.ms.scenarios.general.LossyLinkScenario;
import se.sics.ms.scenarios.general.PartitionAwareBootupScenario;
import se.sics.ms.scenarios.general.SimpleBootupScenario;
import se.sics.p2ptoolbox.simulator.run.LauncherComp;
import java.net.UnknownHostException;

/**
 * @author Abhimanyu Babbar <babbar@kth.se>
 */

public class MainTest {
    
    public static long seed = 123;

    @Test
    public void myTest() throws UnknownHostException {

        LossyLinkScenario.boot(seed).simulate(LauncherComp.class);
    }

}