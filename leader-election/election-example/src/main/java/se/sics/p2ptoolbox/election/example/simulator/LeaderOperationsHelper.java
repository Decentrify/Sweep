package se.sics.p2ptoolbox.election.example.simulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.address.Address;
import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.election.example.main.LCPComparator;
import se.sics.p2ptoolbox.election.example.main.HostManagerComp;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Helper Class for the Leader Election Protocol Operations Simulation.
 *
 * Created by babbar on 2015-04-01.
 */
public class LeaderOperationsHelper {

    private static Logger logger = LoggerFactory.getLogger(LeaderOperationsHelper.class);
    private static ConsistentHashtable<Long> ringNodes = new ConsistentHashtable<Long>();
    private static Long identifierSpaceSize = (long) 3000;

    private static Collection<VodAddress> addressCollection = new ArrayList<VodAddress>();
    private static List<VodAddress> copy = new ArrayList<VodAddress>();

    public static HostManagerComp.HostManagerCompInit generateComponentInit (long id){

        logger.info(" Generating address for peer with id: {} ", id);
        InetAddress ip = null;
        try {
            ip = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        Address address = new Address(ip, 9999, (int) id);
        VodAddress selfAddress = new VodAddress(address, 0);

        addressCollection.add(selfAddress);
        copy.add(selfAddress);

        HostManagerComp.HostManagerCompInit init = new HostManagerComp.HostManagerCompInit(selfAddress, 30000,  new LCPComparator(), 2);
        return init;
    }

    public static long getNodeId(long id){

        Long successor = ringNodes.getNode(id);

        while (successor != null && successor.equals(id)) {
            id = (id + 1) % identifierSpaceSize;
            successor = ringNodes.getNode(id);
        }

        return id;
    }


    public static Collection<VodAddress> getPeersAddressCollection(){
        return addressCollection;
    }


    public static VodAddress getUniqueAddress(){

        if(copy == null || copy.size() == 0){
            throw new IllegalStateException("No entries in the list to return to");
        }

        return copy.remove(0);
    }

}
