package se.sics.ms.util;

import se.sics.ms.util.SimpleLCPViewComparator;
import se.sics.ms.util.ApplicationRuleSet;
import se.sics.ms.util.LEContainerComparator;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.types.PeerDescriptor;
import se.sics.ms.util.ComparatorCollection;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import se.sics.ktoolbox.election.util.LEContainer;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicAddress;

/**
 * Unit test for the application rule set.
 *
 * Created by babbar on 2015-08-19.
 */
public class ApplicationRuleSetTest {

    private static Logger logger = LoggerFactory.getLogger(ApplicationRuleSetTest.class);
    private static InetAddress ipAddress;
    private static int port;
    private static Comparator<LEContainer> containerComparator;

    private static ApplicationRuleSet.SweepLCRuleSet lcRuleSet;
    private static ApplicationRuleSet.SweepCohortsRuleSet cohortsRuleSet;


    @BeforeClass
    public static void beforeClass() {

        logger.debug("One time initiation");
        systemSetup();

        containerComparator = new LEContainerComparator(new SimpleLCPViewComparator(),
                new ComparatorCollection.AddressComparator());


        try {

            ipAddress = InetAddress.getLocalHost();
            port = 33333;

            lcRuleSet = new ApplicationRuleSet.SweepLCRuleSet(containerComparator);
            cohortsRuleSet = new ApplicationRuleSet.SweepCohortsRuleSet(containerComparator);

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
    
    private static void systemSetup() {
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
        LEContainer selfContainer = createContainer(ipAddress, port, new IntIdentifier(Integer.MIN_VALUE));

        Collection<KAddress> result = lcRuleSet.initiateLeadership(selfContainer, containerCollection, cohortSize);
        Assert.assertEquals("Cohort Size Comparison", cohortSize, result.size());

    }


    @Test
    public void falseLeadershipTest(){

        int viewsize = 10;
        int cohortSize = 6;

        Collection<LEContainer> containerCollection = createContainers(viewsize);
        LEContainer selfContainer = createContainer(ipAddress, port, new IntIdentifier(Integer.MAX_VALUE));

        Collection<KAddress> result = lcRuleSet.initiateLeadership(selfContainer, containerCollection, cohortSize);
        Assert.assertEquals("Empty Cohort Size Comparison", 0, result.size());

    }


    @Test
    public void correctCohortTest(){

        int viewSize = 10;
        int cohortSize = 6;

        Collection<LEContainer> containerCollection = createContainers(viewSize);
        LEContainer selfContainer = createContainer(ipAddress, port, new IntIdentifier(Integer.MIN_VALUE));

        List<LEContainer> reverseSortedCollection = reverseSortCollection(containerComparator, containerCollection);
        KAddress expectedBestCohortAddress = reverseSortedCollection.isEmpty() ? null : reverseSortedCollection.iterator().next().getSource();

        List<KAddress> cohorts = lcRuleSet.initiateLeadership(selfContainer, containerCollection, cohortSize);
        KAddress actualBestCohortAddress = cohorts.isEmpty() ? null : cohorts.iterator().next();

        logger.debug("Expected : {}, Actual :{}", expectedBestCohortAddress, actualBestCohortAddress);
        Assert.assertEquals("Best Cohort Test", expectedBestCohortAddress, actualBestCohortAddress);
    }


    @Test
    public void followerAcceptanceTest(){

        logger.debug("Performing follower acceptance test.");
        int viewSize = 10;

        Collection<LEContainer> containerCollection = createContainers(viewSize);

        LEContainer selfContainer = createContainer(ipAddress, port, new IntIdentifier(Integer.MIN_VALUE + 1));
        LEContainer leaderContainer = createContainer(ipAddress, port, new IntIdentifier(Integer.MIN_VALUE));

        boolean actualResult = cohortsRuleSet.validate(leaderContainer, selfContainer, containerCollection);
        Assert.assertEquals("follower acceptance test", true, actualResult);

    }


    @Test
    public void followerRejectionTest(){

        logger.debug("Performing follower acceptance test.");
        int viewSize = 10;

        Collection<LEContainer> containerCollection = createContainers(viewSize);
        LEContainer selfContainer = createContainer(ipAddress, port, new IntIdentifier(Integer.MAX_VALUE));

        LEContainer leaderContainer = createContainer(ipAddress, port, new IntIdentifier(8));
        boolean actualResult = cohortsRuleSet.validate(leaderContainer, selfContainer, containerCollection);

        Assert.assertEquals("follower acceptance test", false, actualResult);

    }





    /**
     * Return a reverse sorted collection.
     *
     * @param comparator comparator
     * @param containers container
     * @return reverse sorted collection.
     */
    public List<LEContainer> reverseSortCollection(Comparator<LEContainer> comparator, Collection<LEContainer> containers){

        List<LEContainer> containerList = new ArrayList<LEContainer>(containers);
        Collections.sort(containerList, comparator);

        Collections.reverse(containerList);

        return containerList;
    }



    /**
     * Create containers for the application.
     * @param size
     * @return
     */
    private List<LEContainer> createContainers(int size){

        logger.debug("Initiating the process of creating the containers.");

        List<LEContainer> containerCollection = new ArrayList<LEContainer>();
        while(size > 0){

            LEContainer container = createContainer(ipAddress, port, new IntIdentifier(size));
            containerCollection.add(container);
            size --;
        }

        return containerCollection;
    }


    private LEContainer createContainer(InetAddress ipAddress, int port, Identifier id){

        KAddress decoratedAddress = new BasicAddress(ipAddress, port, id);

        PeerDescriptor descriptor = new PeerDescriptor(decoratedAddress);
        LEContainer container = new LEContainer(decoratedAddress, descriptor);
        return container;

    }

}
