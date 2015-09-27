
package se.sics.ms.helper;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.*;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ktoolbox.aggregator.server.GlobalAggregatorPort;
import se.sics.ktoolbox.aggregator.server.event.AggregatedInfo;
import se.sics.ms.main.AggregatorCompHelper;
import se.sics.ms.main.SimulationSerializer;
import se.sics.ms.main.SimulationSerializers;
import se.sics.p2ptoolbox.simulator.ExperimentPort;
import se.sics.p2ptoolbox.simulator.dsl.events.TerminateExperiment;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Main class for the data dumping in the system.
 * Created by babbar on 2015-09-18.
 */
public class DataDump {

    private static Logger logger = LoggerFactory.getLogger(DataDump.class);
    private static Path path;
    private static String fileName;

    public static void register(String directory, String fileName){

        Path path = Paths.get(directory);
        if(!Files.exists(path) || !Files.isWritable(path)){
            throw new RuntimeException("Invalid location or missing write permission");
        }

        logger.debug("Path constructed is valid.");
        DataDump.path = path;
        DataDump.fileName = fileName;
    }



//  ===================================
//  DATA DUMP WRITE COMPONENT.
//  ===================================

    public static class Write extends ComponentDefinition {

        Positive<GlobalAggregatorPort> aggregatorPort = requires(GlobalAggregatorPort.class);
        Positive<ExperimentPort> experimentPort = requires(ExperimentPort.class);

        private String name = "WRITE";
        private OutputStream outputStream;
        private AggregatorCompHelper helper;
        private Serializer aggregatedInfoSerializer;
        private ByteBuf byteBuf;
        private int maxWindowsPerFile;
        private int fileNameCounter;
        private int currentWindowCounter;

        public Write(DataDumpInit.Write init) {

            doInit(init);
            subscribe(startHandler, control);
            subscribe(stopHandlerUpdated, experimentPort);
            subscribe(aggregatedInfoHandler, aggregatorPort);
        }

        /**
         * Run initialization tasks for the internal variables
         * for the system before the main component boots up.
         * @param init init.
         */
        public void doInit(DataDumpInit.Write init){

            logger.debug("{}: Initialization method invoked.", name);
            helper = init.helper;
            aggregatedInfoSerializer = Serializers.lookupSerializer(AggregatedInfo.class);
            byteBuf = Unpooled.buffer();

            fileNameCounter = 0;
            currentWindowCounter = 0;
            maxWindowsPerFile = init.maxWindowsPerFile;

            try {
                outputStream = getNextOutputStream(null);
            }
            catch (IOException e) {
                e.printStackTrace();
            }

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

                logger.trace("Handler for the aggregated information from the global aggregator.");
                AggregatedInfo filteredInfo = helper.filter(aggregatedInfo);

                if(filteredInfo.getNodePacketMap().isEmpty())
                    return;

                try {

//                  Switch to the next outputstream in case we already have reached the quota.
                    if(currentWindowCounter > maxWindowsPerFile){

                        logger.debug("Current file dump completed, going to dump in a new file.");
                        outputStream = getNextOutputStream(outputStream);
                        currentWindowCounter = 0;
                    }

                    aggregatedInfoSerializer.toBinary(filteredInfo, byteBuf);
                    logger.debug("Going to write :{}, bytes", byteBuf.readableBytes());

                    int readableBytes  = byteBuf.readableBytes();
                    byteBuf.readBytes(outputStream, readableBytes);
                    outputStream.flush();

                    byteBuf.clear();
                    currentWindowCounter++;
                }
                catch (IOException e) {

                    e.printStackTrace();
                    throw new RuntimeException("Unable to write the serialized data to the stream.");
                }
            }
        };

        /**
         * Handler indicating that the component will be stopping,
         * releasing memory resources, if any.
         */
        Handler<TerminateExperiment> stopHandlerUpdated = new Handler<TerminateExperiment>() {
            @Override
            public void handle(TerminateExperiment stop) {

                logger.debug("Start writing the collection in the file.");

                IOUtils.closeQuietly(outputStream);
                System.out.println("Finished with dumping the data to file.");
            }
        };

        /**
         * Get the output stream for the
         * next file in the series.
         *
         * @return OutputStream.
         */
        private OutputStream getNextOutputStream(OutputStream currentStream) throws IOException {

            if(currentStream != null)
                IOUtils.closeQuietly(currentStream);

            StringBuffer buffer = new StringBuffer().append(DataDump.fileName).append(fileNameCounter);
            Path filePath = Paths.get(DataDump.path.toAbsolutePath().toString(), buffer.toString());

            logger.debug(filePath.toAbsolutePath().toString());
            OutputStream outputStream = Files.newOutputStream(filePath);
            fileNameCounter++;
            return outputStream;
        }


    }


//  ===================================
//  DATA DUMP READ COMPONENT.
//  ===================================

    public static class Read extends ComponentDefinition{

        private String name  = "READ";
        Negative<GlobalAggregatorPort> aggregatorPort = provides(GlobalAggregatorPort.class);

        private FileInputStream inputStream;
        private Serializer aggregatedInfoSerializer;

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
            aggregatedInfoSerializer = Serializers.lookupSerializer(AggregatedInfo.class);

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
                initiateInformationReadUpdated();
            }
        };


        /**
         * Start reading the information dumped in the file and then send it to the
         * application above that will be connected with it.
         */
        private void initiateInformationRead(){

            logger.debug("{}: Initiating the reading of the aggregated data.", name);
            try {

                byte[] bytes = IOUtils.toByteArray(inputStream);
                ByteBuffer buffer = ByteBuffer.wrap(bytes);

                int size = buffer.getInt();
                SimulationSerializer serializer = SimulationSerializers.lookupSerializer(AggregatedInfo.class);
                logger.debug("{}: ", serializer);

                while(size > 0){

                    AggregatedInfo aggregatedInfo = (AggregatedInfo) serializer.fromBinary(buffer);
                    trigger(aggregatedInfo, aggregatorPort);

                    size --;
                }

            }
            catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Unable to open the file for reading.");
            }
        }


        /**
         * Start reading the information dumped in the file and then send it to the
         * application above that will be connected with it.
         */
        private void initiateInformationReadUpdated(){

            logger.debug("{}: Initiating the reading of the aggregated data.", name);
            try {

                byte[] bytes = IOUtils.toByteArray(inputStream);
                logger.debug("Bytes Read :{}", bytes.length);
                ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);

                while(byteBuf.isReadable()){

                    AggregatedInfo aggregatedInfo= (AggregatedInfo)aggregatedInfoSerializer.fromBinary(byteBuf, Optional.absent());
                    trigger(aggregatedInfo, aggregatorPort);
                }

            }
            catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Unable to open the file for reading.");
            }
            finally {
                IOUtils.closeQuietly(inputStream);
            }
        }


    }



}
