/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.ms.util;

import se.sics.ms.util.SimpleLCPViewComparator;
import se.sics.ms.util.ApplicationRuleSet;
import se.sics.ms.util.LEContainerComparator;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ktoolbox.election.util.LEContainer;
import se.sics.ktoolbox.util.InvertedComparator;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicAddress;
import se.sics.ms.types.PeerDescriptor;
import se.sics.ms.util.ComparatorCollection;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SweepRuleSetTest {

    private static final Logger LOG = LoggerFactory.getLogger(SweepRuleSetTest.class);

    private final static Comparator<LEContainer> containerComparator;
    private final static ApplicationRuleSet.SweepLCRuleSet lcRuleSet;
    private final static ApplicationRuleSet.SweepCohortsRuleSet cohortsRuleSet;

    private final static InetAddress ip;
    private final static int port = 30000;
    private Random rand;

    static {
        try {
            ip = InetAddress.getByName("193.84.0.1");
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }

        containerComparator = new LEContainerComparator(new SimpleLCPViewComparator(),
                new ComparatorCollection.AddressComparator());
        lcRuleSet = new ApplicationRuleSet.SweepLCRuleSet(containerComparator);
        cohortsRuleSet = new ApplicationRuleSet.SweepCohortsRuleSet(containerComparator);
    }

    private void randomSetup() {
        SecureRandom srand = new SecureRandom();
        long seed = srand.nextLong();
        LOG.info("test seed:{}", seed);
        rand = new Random(seed);
    }

    private void fixedSetup() {
        long seed = 0;
        LOG.info("test seed:{}", seed);
        rand = new Random(seed);
    }

    @Test
    public void notLeaderTest() {
        fixedSetup();

        int viewsize = 10;
        int cohortSize = 6;
        LEContainer self = createSimpleContainer(0);

        LOG.debug("I am not leader");

        Map<Identifier, LEContainer> gradientView = createSimpleGradientView(viewsize);
        printGradientView(gradientView);

        List<KAddress> cohort = lcRuleSet.initiateLeadership(self, gradientView.values(), cohortSize);
        printCohort(cohort);

        Assert.assertTrue(gradientView.size() > cohortSize);
        Assert.assertTrue("not leader", cohort.size() != cohortSize);
    }

    @Test
    public void simpleFixTest() {
        LOG.debug("I should be leader - simple");
        fixedSetup();
        Pair<LEContainer, List<KAddress>> result = simpleTest();

        Assert.assertEquals(new BasicAddress(ip, port, new IntIdentifier(-1930858313)), result.getValue1().get(0));
        Assert.assertEquals(new BasicAddress(ip, port, new IntIdentifier(-1728529858)), result.getValue1().get(1));
        Assert.assertEquals(new BasicAddress(ip, port, new IntIdentifier(-1690734402)), result.getValue1().get(2));
        Assert.assertEquals(new BasicAddress(ip, port, new IntIdentifier(-1557280266)), result.getValue1().get(3));
        Assert.assertEquals(new BasicAddress(ip, port, new IntIdentifier(-1155484576)), result.getValue1().get(4));
        Assert.assertEquals(new BasicAddress(ip, port, new IntIdentifier(-938301587)), result.getValue1().get(5));
    }

    @Test
    public void simpleRandomTest() {
        LOG.debug("I should be leader - simple");
        randomSetup();
        simpleTest();
    }

    private Pair<LEContainer, List<KAddress>> simpleTest() {
        int viewsize = 10;
        int cohortSize = 6;
        LEContainer self = createSimpleContainer(Integer.MIN_VALUE);

        Map<Identifier, LEContainer> gradientView = createSimpleGradientView(viewsize);
        printGradientView(gradientView);

        List<KAddress> cohort = lcRuleSet.initiateLeadership(self, gradientView.values(), cohortSize);
        printCohort(cohort);

        Assert.assertEquals("cohort size", cohortSize, cohort.size());
        LEContainer firstCohort = gradientView.get(cohort.get(0).getId());
        Assert.assertTrue("leader", containerComparator.compare(self, firstCohort) > 0);

        return Pair.with(self, cohort);
    }

    @Test
    public void lgFlagFixTest() {
        LOG.debug("I should be leader - lgFlag");
        fixedSetup();
        Pair<LEContainer, List<KAddress>> result = lgFlagTest();

        Assert.assertEquals(new BasicAddress(ip, port, new IntIdentifier(-1930858313)), result.getValue1().get(0));
        Assert.assertEquals(new BasicAddress(ip, port, new IntIdentifier(-1728529858)), result.getValue1().get(1));
        Assert.assertEquals(new BasicAddress(ip, port, new IntIdentifier(-1557280266)), result.getValue1().get(2));
        Assert.assertEquals(new BasicAddress(ip, port, new IntIdentifier(-1155484576)), result.getValue1().get(3));
        Assert.assertEquals(new BasicAddress(ip, port, new IntIdentifier(-518907128)), result.getValue1().get(4));
        Assert.assertEquals(new BasicAddress(ip, port, new IntIdentifier(-252332814)), result.getValue1().get(5));
    }

    @Test
    public void lgFlagRandomTest() {
        LOG.debug("I should be leader - lgFlag");
        randomSetup();
        lgFlagTest();
    }
    
    /**
     * randomly switch on the leaderGroup flag
     */
    private Pair<LEContainer, List<KAddress>> lgFlagTest() {
        int viewsize = 10;
        int cohortSize = 6;
        LEContainer self = createSimpleContainer(Integer.MIN_VALUE);

        Map<Identifier, LEContainer> gradientView = createLGGradientView(viewsize);
        printGradientView(gradientView);

        List<KAddress> cohort = lcRuleSet.initiateLeadership(self, gradientView.values(), cohortSize);
        printCohort(cohort);

        Assert.assertEquals("cohort size", cohortSize, cohort.size());
        LEContainer firstCohort = gradientView.get(cohort.get(0).getId());
        Assert.assertTrue("leader", containerComparator.compare(self, firstCohort) > 0);
        
        return Pair.with(self, cohort);
    }

    private void printGradientView(Map<Identifier, LEContainer> gradientView) {
        List<LEContainer> sortedView = new ArrayList<>(gradientView.values());
        Collections.sort(sortedView, new InvertedComparator<>(containerComparator));
        List<Identifier> sortedViewIds = new ArrayList<>();
        for (LEContainer node : sortedView) {
            sortedViewIds.add(node.getSource().getId());
        }
        LOG.debug("gradient sorted view:{}", sortedViewIds);
    }

    private void printCohort(List<KAddress> cohort) {
        List<Identifier> cohortIds = new ArrayList<>();
        for (KAddress node : cohort) {
            cohortIds.add(node.getId());
        }
        LOG.debug("sorted cohort:{}", cohortIds);
    }

    private Map<Identifier, LEContainer> createSimpleGradientView(int size) {
        Map<Identifier, LEContainer> view = new HashMap<>();
        for (int i = 0; i < size; i++) {
            LEContainer newContainer = createSimpleContainer(rand.nextInt());
            view.put(newContainer.getSource().getId(), newContainer);
        }
        return view;
    }

    private Map<Identifier, LEContainer> createLGGradientView(int size) {
        Map<Identifier, LEContainer> view = new HashMap<>();
        for (int i = 0; i < size; i++) {
            LEContainer newContainer = createLGContainer(rand.nextInt(), rand.nextBoolean());
            view.put(newContainer.getSource().getId(), newContainer);
        }
        return view;
    }

    private LEContainer createSimpleContainer(int id) {
        return createContainer(id, false);
    }

    private LEContainer createLGContainer(int id, boolean lgFlag) {
        return createContainer(id, lgFlag);
    }

    private LEContainer createContainer(int id, boolean lgFlag) {
        KAddress address = new BasicAddress(ip, port, new IntIdentifier(id));
        PeerDescriptor descriptor = new PeerDescriptor(address);
        LEContainer container = new LEContainer(address, descriptor);
        return container;
    }
}
