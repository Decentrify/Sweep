package se.sics.util;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.types.PeerDescriptor;
import se.sics.ms.util.ComparatorCollection;
import se.sics.p2ptoolbox.election.api.LEContainer;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Unit test for the application rule set.
 *
 * Created by babbar on 2015-08-19.
 */
public class ApplicationRuleSetTest {

    private static Logger logger = LoggerFactory.getLogger(ApplicationRuleSetTest.class);
    private static InetAddress ipAddress;
    private static int port;

    private static ApplicationRuleSet.SweepLCRuleSet lcRuleSet;
    private static ApplicationRuleSet.SweepCohortsRuleSet cohortsRuleSet;


    @BeforeClass
    public static void beforeClass() {

        logger.debug("One time initiation");

        LEContainerComparator comparator = new LEContainerComparator(new SimpleLCPViewComparator(),
                new ComparatorCollection.AddressComparator());

        try {

            ipAddress = InetAddress.getLocalHost();
            port = 33333;

            lcRuleSet = new ApplicationRuleSet.SweepLCRuleSet(comparator);
            cohortsRuleSet = new ApplicationRuleSet.SweepCohortsRuleSet(comparator);

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setUp(){
        logger.debug("Setup invoked before the test.");
    }


    @After
    public void tearDown(){
        logger.debug("Tear down invoked after the test.");
    }


    @Test
    public void testInitiateLeadership(){

        logger.debug("Basic leadership initiation test");

        int viewsize = 10;
        int cohortSize = 6;

        Collection<LEContainer> containerCollection = createContainers(viewsize);
        LEContainer selfContainer = createContainer(ipAddress, port, Integer.MIN_VALUE);

        Collection<DecoratedAddress> result = lcRuleSet.initiateLeadership(selfContainer, containerCollection, cohortSize);
        Assert.assertEquals("Cohort Size Comparison", cohortSize, result.size());

    }


    @Test
    public void falseLeadershipTest(){

    }


    /**
     * Create containers for the application.
     * @param size
     * @return
     */
    private Collection<LEContainer> createContainers(int size){

        logger.debug("Initiating the process of creating the containers.");

        Collection<LEContainer> containerCollection = new ArrayList<LEContainer>();
        while(size > 0){

            LEContainer container = createContainer(ipAddress, port, size);
            containerCollection.add(container);
            size --;
        }

        return containerCollection;
    }


    private LEContainer createContainer(InetAddress ipAddress, int port , int id){

        BasicAddress address = new BasicAddress(ipAddress, port, id);
        DecoratedAddress decoratedAddress = new DecoratedAddress(address);

        PeerDescriptor descriptor = new PeerDescriptor(decoratedAddress);
        LEContainer container = new LEContainer(decoratedAddress, descriptor);

        return container;

    }

}
