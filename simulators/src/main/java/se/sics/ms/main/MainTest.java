package se.sics.ms.main;

import org.junit.Test;
import se.sics.ms.scenarios.general.SimpleBootupScenario;
import se.sics.ms.scenarios.general.SimplePartitioningScenario;

/**
 * Main Class for test execution.
 * Created by babbarshaer on 2015-02-04.
 */

import se.sics.ms.scenarios.general.SimulationHostBootupScenario;
import se.sics.ms.scenarios.general.ThreadedTimeInterceptor;
import se.sics.p2ptoolbox.simulator.run.LauncherComp;

import java.net.UnknownHostException;

/**
 * @author Abhimanyu Babbar <babbar@kth.se>
 */

public class MainTest {

    public static long seed = 123;


    public static void main(String[] args) throws UnknownHostException {
        myTest();
    }

    public static void myTest() throws UnknownHostException {
        SimulationHostBootupScenario.boot(seed).simulate(LauncherComp.class, new ThreadedTimeInterceptor(null));
    }

}