package se.sics.ms.launch.global.aggregator.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ms.aggregator.data.SweepAggregatedPacket;
import se.sics.ms.launch.global.aggregator.helper.DataAnalyzer;
import se.sics.ms.launch.global.aggregator.model.SimpleDataModel;
import se.sics.p2ptoolbox.aggregator.api.model.AggregatedStatePacket;
import se.sics.p2ptoolbox.aggregator.api.msg.GlobalState;
import se.sics.p2ptoolbox.aggregator.api.msg.Ready;
import se.sics.p2ptoolbox.aggregator.api.port.GlobalAggregatorPort;
import se.sics.p2ptoolbox.aggregator.core.GlobalAggregatorComponent;
import se.sics.p2ptoolbox.aggregator.core.GlobalAggregatorComponentInit;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Receives and interprets the updates from the GlobalAggregator Application.
 * Created by babbarshaer on 2015-03-18.
 */
public class SystemAggregatorApplication extends ComponentDefinition{
    
    private ConcurrentMap<BasicAddress, SweepAggregatedPacket> systemGlobalState;
    private Logger logger = LoggerFactory.getLogger(SystemAggregatorApplication.class);
    private Component globalAggregator;
    private long timeout = 1000;
    private long windowTimeout = 10000;
    
    private Positive<Network> networkPort = requires(Network.class);
    private Positive<Timer> timerPositive = requires(Timer.class);
    private SystemAggregatorApplication myComp;
    private FileWriter writer;
    private static String DEFAULT_FILE = "shardLogs.csv";
    
    public SystemAggregatorApplication(SystemAggregatorApplicationInit init) throws IOException {
        
        doInit(init);
        subscribe(startHandler, control);
        
        globalAggregator = create(GlobalAggregatorComponent.class, new GlobalAggregatorComponentInit(timeout, windowTimeout));
        connect(globalAggregator.getNegative(Timer.class), timerPositive);
        connect(globalAggregator.getNegative(Network.class), networkPort);

        subscribe(globalStateHandler, globalAggregator.getPositive(GlobalAggregatorPort.class));
        subscribe(readyHandler, globalAggregator.getPositive(GlobalAggregatorPort.class));
    }
    
    
    
    public void doInit(SystemAggregatorApplicationInit init) throws IOException {
        
        logger.info("init");
        systemGlobalState = new ConcurrentHashMap<BasicAddress, SweepAggregatedPacket>();
        myComp = this;
        String fileLoc = init.getCsvLoc() != null ? init.getCsvLoc() : DEFAULT_FILE;
        createWriter(fileLoc);
    }

    private void createWriter(String fileLoc) throws IOException {
        
        File file = new File(fileLoc);
        if(!file.exists()){
            file.createNewFile();
        }

        System.out.println(" File Path : " + file.getAbsolutePath());
        writer = new FileWriter(file.getAbsolutePath());
    }

    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            logger.debug("System Application for capturing aggregated data booted up.");
        }
    };
    
    
    Handler<GlobalState> globalStateHandler = new Handler<GlobalState>() {
        @Override
        public void handle(GlobalState event) {

            try{
                Map<DecoratedAddress, AggregatedStatePacket> map = event.getStatePacketMap();
                logger.debug("Received Aggregated State Packet Map with size: {}", map.size());
                systemGlobalState.clear();

                for(Map.Entry<DecoratedAddress, AggregatedStatePacket> entry : map.entrySet()){

                    BasicAddress address = entry.getKey().getBase();
                    AggregatedStatePacket statePacket = entry.getValue();

                    if(statePacket instanceof SweepAggregatedPacket){
                        SweepAggregatedPacket sap = (SweepAggregatedPacket)statePacket;
                        systemGlobalState.put(address, sap);
                    }
                }
                
                writer.write(DataAnalyzer.constructShardInfoBuckets(SystemAggregatorDataModelBuilder.getSimpleDataModel(systemGlobalState)));
                writer.flush();
            } 
            catch (IOException e) {
                
                try {
                    if(writer != null)
                        writer.close();
                    
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                e.printStackTrace();
            }
        }
    };

    Handler<Ready> readyHandler = new Handler<Ready>() {
        @Override
        public void handle(Ready event) {
            logger.debug("Global Aggregator Component Ready");
        }
    };

    public Collection<SimpleDataModel> getStateInSimpleDataModel(){
        return SystemAggregatorDataModelBuilder.getSimpleDataModel(systemGlobalState);
    }
    
    
    

}
