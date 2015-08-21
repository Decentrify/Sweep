package se.sics.ms.main;

import org.junit.Test;
import se.sics.ms.scenarios.general.SimpleBootupScenario;
import se.sics.ms.scenarios.general.SimplePartitioningScenario;

/**
 * Main Class for test execution.
 * Created by babbarshaer on 2015-02-04.
 */

import se.sics.ms.scenarios.general.SimulationHostBootupScenario;
import se.sics.p2ptoolbox.simulator.run.LauncherComp;

import java.net.UnknownHostException;

/**
 * @author Abhimanyu Babbar <babbar@kth.se>
 */

public class MainTest {

    public static long seed = 123;

    @Test
    public void myTest() throws UnknownHostException {

//        int startId = 128;
//        LauncherComp.scheduler = new SimulatorScheduler();
//        LauncherComp.scenario = SimpleBootupScenario.boot(seed);
//        LauncherComp.simulatorClientAddress = new DecoratedAddress(new BasicAddress(InetAddress.getByName("127.0.0.1"), 30000, -1));
//        
//        registerSerializers(startId);
//        
//        Kompics.setScheduler(LauncherComp.scheduler);
//        Kompics.createAndStart(LauncherComp.class, 1);
//        try {
//            Kompics.waitForTermination();
//        } catch (InterruptedException ex) {
//            throw new RuntimeException(ex.getMessage());
//        }

        SimulationHostBootupScenario.boot(seed).simulate(LauncherComp.class);
//        Assert.assertEquals(null, MyExperimentResult.failureCause);
    }

}