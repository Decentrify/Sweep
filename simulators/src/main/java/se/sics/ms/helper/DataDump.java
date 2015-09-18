
package se.sics.ms.helper;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.*;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.aggregator.server.GlobalAggregatorPort;
import se.sics.ktoolbox.aggregator.server.event.AggregatedInfo;

import java.io.*;

/**
 * Main class for the data dumping in the system.
 * Created by babbar on 2015-09-18.
 */
public class DataDump {

    private static Logger logger = LoggerFactory.getLogger(DataDump.class);


//  DATA DUMP WRITE COMPONENT.

    public static class Write extends ComponentDefinition{

        Positive<GlobalAggregatorPort> aggregatorPort = requires(GlobalAggregatorPort.class);
        private String name = "WRITE";
        private ByteBuf byteBuf;
        private FileOutputStream outputStream;

        public Write(DataDumpInit.Write init) {

            doInit(init);
            subscribe(startHandler, control);
            subscribe(stopHandler, control);
            subscribe(aggregatedInfoHandler, aggregatorPort);
        }

        /**
         * Run initialization tasks for the internal variables
         * for the system before the main component boots up.
         * @param init init.
         */
        public void doInit(DataDumpInit.Write init){

            logger.debug("{}: Initialization method invoked.", name);

            try {

                File file = new File(init.location);
                if(!file.exists() || file.isDirectory()) {

                    logger.debug("Invalid file location.");
                    throw new RuntimeException("Unable to create file for the dumping data.");
                }

                outputStream = new FileOutputStream(file);
            }
            catch (FileNotFoundException e) {

                e.printStackTrace();
                throw new RuntimeException("Unable to create file output stream for the dumping data.");
            }


            logger.debug("{}: Creating buffer instance.");
            this.byteBuf = Unpooled.buffer();
        }


        /**
         * Main handler for the start event.
         * Now the system can create new components and also trigger new events.
         */
        Handler<Start> startHandler = new Handler<Start>() {
            @Override
            public void handle(Start start){
                logger.debug("{}: Start handler invoked", name);
            }
        };


        /**
         * Handler for the aggregated information from the
         * global aggregator in the system.
         */
        Handler<AggregatedInfo> aggregatedInfoHandler = new Handler<AggregatedInfo>() {
            @Override
            public void handle(AggregatedInfo aggregatedInfo) {

                logger.debug("Handler for the aggregated information from the global aggregator.");

                Serializers.lookupSerializer(AggregatedInfo.class).toBinary(aggregatedInfo, byteBuf);
//              Dump the buffer in the file and clear the buffer to get the value again.

                byte[] backingArray = byteBuf.array();
                try {
                    IOUtils.write(backingArray, outputStream);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };


        /**
         * Handler indicating that the component will be stopping,
         * releasing memory resources, if any.
         */
        Handler<Stop> stopHandler = new Handler<Stop>() {
            @Override
            public void handle(Stop stop) {

                logger.debug("No more data needs to be dumped in the file, stopping.");
                IOUtils.closeQuietly(outputStream);
            }
        };


    }



//  DATA DUMP READ COMPONENT.


    public static class Read extends ComponentDefinition{

        private String name  = "READ";
        Negative<GlobalAggregatorPort> aggregatorPort = provides(GlobalAggregatorPort.class);

        private FileInputStream inputStream;

        public Read(DataDumpInit.Read init){
            doInit(init);
        }



        /**
         * Run the initialization tasks for the internal variables
         * for the system before the component boots up.
         * @param init init
         */
        public void doInit(DataDumpInit.Read init){

            logger.debug("{}: Initializing the component", name);

            try {

                File file = new File(init.location);
                inputStream = new FileInputStream(file);

            } catch (FileNotFoundException e) {

                e.printStackTrace();
                throw new RuntimeException("Unable to locate files for creating an input stream.");
            }


            subscribe(startHandler, control);
        }


        /**
         * Main handler for the start event to the component.
         * At this stage the component is booted up and ready to trigger events
         *
         */
        Handler<Start> startHandler = new Handler<Start>() {
            @Override
            public void handle(Start start) {

                logger.debug("{}: Start Handler invoked ", name);
                initiateInformationRead();
            }
        };


        /**
         * Start reading the information dumped in the file and then send it to the
         * application above that will be connected with it.
         */
        private void initiateInformationRead(){

            logger.debug("{}: Initiating the reading of the ");
            try {

                byte[] bytes = IOUtils.toByteArray(inputStream);
                ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);

//              Read till we exhaust the bytes in the buffer.
                while(byteBuf.isReadable()){

                    AggregatedInfo aggregatedInfo = (AggregatedInfo) Serializers.lookupSerializer(AggregatedInfo.class)
                            .fromBinary(byteBuf, Optional.absent());

//                  Inform the visualizer component on top of it about the same.
                    trigger(aggregatedInfo, aggregatorPort);
                }
            }
            catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Unable to open the file for reading.");
            }
        }


    }



}
