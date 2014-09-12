package se.sics.ms.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.address.Address;
import se.sics.gvod.net.Transport;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.ms.configuration.MsConfig;

/**
 * Created by babbarshaer on 2014-09-12.
 */
public class TransportHelper {

    private static final Logger logger = LoggerFactory.getLogger(TransportHelper.class);
    private static boolean isSimulation = true;
    /**
     * Update the message before sending it on the network.
      * @param msg
     */
    public static void checkTransportAndUpdateBeforeSending(RewriteableMsg msg){

        // Code running in simulation so don't change the address.
        if(isSimulation)
            return;

        // In case a UDT message, update the port to UDT port before sending as UDT server running on a different port.
        if(msg.getProtocol() == Transport.UDT){
            logger.warn("_Abhi: {checkTransportAndUpdateBeforeSending}: Going to rewrite the ports as UDT Message.");
            msg.rewriteDestination(new Address(msg.getDestination().getIp(), MsConfig.getUdtPort(), msg.getDestination().getId()));
            msg.rewritePublicSource(new Address(msg.getSource().getIp(), MsConfig.getUdtPort(), msg.getSource().getId()));
        }
    }

    /**
     * Update the message before further processing.
     * @param msg
     */
    public static void checkTransportAndUpdateBeforeReceiving(RewriteableMsg msg){

        // Code running in simulation, so dont change address.
        if(isSimulation)
            return;

        // In case msg received is UDT, rewrite the port to original UDP port on which other protocols are running, to avoid creating confusion in the components about the ports.
        if(msg.getProtocol() == Transport.UDT){
            logger.warn("_Abhi: {checkTransportAndUpdateBeforeReceiving}: Going to rewrite the ports as UDT Message.");
            msg.rewriteDestination(new Address(msg.getDestination().getIp(), MsConfig.getPort(), msg.getDestination().getId()));
            msg.rewritePublicSource(new Address(msg.getSource().getIp(), MsConfig.getPort(), msg.getSource().getId()));
        }

    }
}
