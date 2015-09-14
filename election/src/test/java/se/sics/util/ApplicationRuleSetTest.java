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
import java.util.*;

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

        int viewsize = 10;
        int cohortSize = 6;

        Collection<LEContainer> containerCollection = createContainers(viewsize);
        LEContainer selfContainer = createContainer(ipAddress, port, Integer.MAX_VALUE);

        Collection<DecoratedAddress> result = lcRuleSet.initiateLeadership(selfContainer, containerCollection, cohortSize);
        Assert.assertEquals("Empty Cohort Size Comparison", 0, result.size());

    }


    @Test
    public void correctCohortTest(){

        int viewSize = 10;
        int cohortSize = 6;

        Collection<LEContainer> containerCollection = createContainers(viewSize);
        LEContainer selfContainer = createContainer(ipAddress, port, Integer.MIN_VALUE);

        Collection<LEContainer> reverseSortedCollection = reverseSortCollection(containerComparator, containerCollection);
        DecoratedAddress expectedBestCohortAddress = reverseSortedCollection.isEmpty() ? null : reverseSortedCollection.iterator().next().getSource();

        Collection<DecoratedAddress> cohorts = lcRuleSet.initiateLeadership(selfContainer, containerCollection, cohortSize);
        DecoratedAddress actualBestCohortAddress = cohorts.isEmpty() ? null : cohorts.iterator().next();

        logger.debug("Expected : {}, Actual :{}", expectedBestCohortAddress, actualBestCohortAddress);
        Assert.assertEquals("Best Cohort Test", expectedBestCohortAddress, actualBestCohortAddress);
    }


    @Test
    public void followerAcceptanceTest(){

        logger.debug("Performing follower acceptance test.");
        int viewSize = 10;

        Collection<LEContainer> containerCollection = createContainers(viewSize);

        LEContainer selfContainer = createContainer(ipAddress, port, Integer.MIN_VALUE + 1);
        LEContainer leaderContainer = createContainer(ipAddress, port, Integer.MIN_VALUE);

        boolean actualResult = cohortsRuleSet.validate(leaderContainer, selfContainer, containerCollection);
        Assert.assertEquals("follower acceptance test", true, actualResult);

    }


    @Test
    public void followerRejectionTest(){

        logger.debug("Performing follower acceptance test.");
        int viewSize = 10;

        Collection<LEContainer> containerCollection = createContainers(viewSize);
        LEContainer selfContainer = createContainer(ipAddress, port, Integer.MAX_VALUE);

        LEContainer leaderContainer = createContainer(ipAddress, port, 8);
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
    public Collection<LEContainer> reverseSortCollection(Comparator<LEContainer> comparator, Collection<LEContainer> containers){

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

        DecoratedAddress decoratedAddress = DecoratedAddress.open(ipAddress, port, id);

        PeerDescriptor descriptor = new PeerDescriptor(decoratedAddress);
        LEContainer container = new LEContainer(decoratedAddress, descriptor);
        return container;

    }

}
