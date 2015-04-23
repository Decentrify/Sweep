package se.sics.p2p.simulator.main;

import org.junit.Test;
import se.sics.kompics.Kompics;
import se.sics.kompics.simulation.SimulatorScheduler;
import se.sics.ms.net.SerializerSetup;
import se.sics.p2ptoolbox.aggregator.network.AggregatorSerializerSetup;
import se.sics.p2ptoolbox.chunkmanager.ChunkManagerSerializerSetup;
import se.sics.p2ptoolbox.croupier.CroupierSerializerSetup;
import se.sics.p2ptoolbox.election.network.ElectionSerializerSetup;
import se.sics.p2ptoolbox.gradient.GradientSerializerSetup;

/**
 * Main Class for test execution.
 * Created by babbarshaer on 2015-02-04.
 */

import se.sics.ms.scenarios.SimpleBootupScenario;
import se.sics.p2ptoolbox.simulator.run.LauncherComp;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;
import se.sics.p2ptoolbox.util.serializer.BasicSerializerSetup;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Abhimanyu Babbar <babbar@kth.se>
 */

public class MainTest {
    
    public static long seed = 123;

    @Test
    public void myTest() throws UnknownHostException {
        
        int startId = 128;
        LauncherComp.scheduler = new SimulatorScheduler();
        LauncherComp.scenario = SimpleBootupScenario.boot(seed);
        LauncherComp.simulatorClientAddress = new BasicAddress(InetAddress.getByName("127.0.0.1"), 30000, -1);
        
        registerSerializers(startId);
        
        Kompics.setScheduler(LauncherComp.scheduler);
        Kompics.createAndStart(LauncherComp.class, 1);
        try {
            Kompics.waitForTermination();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex.getMessage());
        }

//        Assert.assertEquals(null, MyExperimentResult.failureCause);
    }

    /**
     * Start registering the serializers based on the start id.
     * @param startId starting id.
     */
    private static void registerSerializers(int startId){

        int currentId = startId;
        BasicSerializerSetup.registerBasicSerializers(currentId);
        currentId += BasicSerializerSetup.serializerIds;
        currentId = CroupierSerializerSetup.registerSerializers(currentId);
        currentId = GradientSerializerSetup.registerSerializers(currentId);
        currentId = ElectionSerializerSetup.registerSerializers(currentId);
        currentId = AggregatorSerializerSetup.registerSerializers(currentId);
        currentId = ChunkManagerSerializerSetup.registerSerializers(currentId);
        SerializerSetup.registerSerializers(currentId);

    }

}