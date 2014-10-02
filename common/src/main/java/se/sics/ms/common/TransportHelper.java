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

    // ===============
    // UDT = UDP+1
    // ===============

    private static final Logger logger = LoggerFactory.getLogger(TransportHelper.class);
    private static boolean isSimulation = false;
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

            logger.debug("Going to rewrite the ports as UDT Ports before sending in the network.");

            msg.rewriteDestination(new Address(msg.getDestination().getIp(), msg.getDestination().getPort()+1, msg.getDestination().getId()));
            msg.rewritePublicSource(new Address(msg.getSource().getIp(), msg.getSource().getPort()+1, msg.getSource().getId()));
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

            // Should be mirror image of the changes made before sending.
            // =========== NOTE: Testing for the nodes behind NAT's needs to be done using this approach.

            logger.debug("Going to update the ports to normal UDP Ports before processing of message starts.");

            msg.rewriteDestination(new Address(msg.getDestination().getIp(), msg.getDestination().getPort()-1, msg.getDestination().getId()));
            msg.rewritePublicSource(new Address(msg.getSource().getIp(), msg.getSource().getPort()-1, msg.getSource().getId()));
        }

    }
}
