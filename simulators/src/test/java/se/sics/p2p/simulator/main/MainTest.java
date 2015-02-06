package se.sics.p2p.simulator.main;

import org.junit.Test;
import se.sics.kompics.Kompics;
import se.sics.kompics.simulation.SimulatorScheduler;
import se.sics.p2ptoolbox.simulator.LauncherComp;

/**
 * Created by babbarshaer on 2015-02-04.
 */

import se.sics.ms.scenarios.SimpleBootupScenario;

/**
 * @author Abhimanyu Babbar <babbar@kth.se>
 */

public class MainTest {
    
    public static long seed = 123;

    @Test
    public void myTest() {
        LauncherComp.scheduler = new SimulatorScheduler();
        LauncherComp.scenario = SimpleBootupScenario.boot(seed);

        Kompics.setScheduler(LauncherComp.scheduler);
        Kompics.createAndStart(LauncherComp.class, 1);
        try {
            Kompics.waitForTermination();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex.getMessage());
        }

//        Assert.assertEquals(null, MyExperimentResult.failureCause);
    }

}