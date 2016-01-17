

package se.sics.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.types.PeerDescriptor;

import java.util.*;
import se.sics.ktoolbox.election.rules.CohortsRuleSet;
import se.sics.ktoolbox.election.rules.LCRuleSet;
import se.sics.ktoolbox.election.util.LCPeerView;
import se.sics.ktoolbox.election.util.LEContainer;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.nat.NatType;

/**
 * Leader election rule set for the application.
 * <p/>
 * Created by babbar on 2015-08-12.
 */
public class ApplicationRuleSet {

    private static Logger logger = LoggerFactory.getLogger(ApplicationRuleSet.class);

    /**
     * Implementation of the election rule set
     * for the leader component.
     * <p/>
     * Created by babbar on 2015-08-11.
     */
    public static class SweepLCRuleSet extends LCRuleSet {


        public SweepLCRuleSet(Comparator<LEContainer> comparator) {
            super(comparator);
        }

        @Override
        public List<KAddress> initiateLeadership(LEContainer selfContainer, Collection<LEContainer> view, int cohortsSize) {

            List<LEContainer> intermediate = new ArrayList<LEContainer>();
            KAddress selfAddress = selfContainer.getSource();

//      Stage 1: Check if node self is Nated.
            if (NatType.isNated(selfAddress)) {
                return new ArrayList<KAddress>();
            }

//      Stage 2 : Check if I am the Best node based on utility comparison
            List<LEContainer> list = new ArrayList<LEContainer>(view);

            // Collection need to be reversed, as top N are fetched.
            list = reverseSortList(leComparator, list);

            if (list.size() > 0 && leComparator.compare(selfContainer, list.get(0)) < 0)
                return new ArrayList<KAddress>();


//          Stage 3: Create list of nodes for the leader group which are not behind nat.
            for (LEContainer container : list) {

                KAddress address = container.getSource();
                if (NatType.isNated(address))
                    continue;

                intermediate.add(container);
            }

//          Stage 4: Check for the length of the list.
            if (intermediate.size() <= cohortsSize)
                return getResult(intermediate);

            return getTopNResults(intermediate, cohortsSize);
        }


        @Override
        public List<KAddress> continueLeadership(LEContainer selfContainer, Collection<LEContainer> collection, int cohortsSize) {

//          IMPORTANT: Create shallow copies of the object to prevent messing with the original objects.
            selfContainer = disableContainer(selfContainer);
            Collection<LEContainer> disabledContainers = disableLGMembership(collection);

//          Delegate the task to the initiate leadership as same process.
            return initiateLeadership(selfContainer, disabledContainers, cohortsSize);
        }


        @Override
        public boolean terminateLeadership(LCPeerView old, LCPeerView updated) {

            PeerDescriptor previous;
            PeerDescriptor current;

            if (!(old instanceof PeerDescriptor) || !(updated instanceof PeerDescriptor)) {
                throw new IllegalArgumentException("Unknown types of arguments.");
            }

            previous = (PeerDescriptor) old;
            current = (PeerDescriptor) updated;

            // Return true in event of increase in partitioning depth.
            return (current.getPartitioningDepth() > previous.getPartitioningDepth());
        }


//      --------------------------------------------------------------------------------------------------------
        /**
         * Sort the cohorts and extract the best to be a part of leader group.
         *
         * @param cohorts
         * @param size
         * @return
         */
        private List<KAddress> getTopNResults(List<LEContainer> cohorts, int size) {

            if (cohorts.size() < size)
                return getResult(cohorts);

            return getResult(cohorts.subList(0, size));
        }


        /**
         * Convenience method to extract the address information
         * from the collection provided
         *
         * @param cohorts
         * @return
         */
        private List<KAddress> getResult(Collection<LEContainer> cohorts) {

            List<KAddress> result = new ArrayList<KAddress>();
            for (LEContainer container : cohorts) {
                result.add(container.getSource());
            }

            return result;
        }


        /**
         * Construct an a sorted set with nodes which has the leader group membership
         * checked off.
         *
         * @return Sorted Set.
         */
        private Collection<LEContainer> disableLGMembership(Collection<LEContainer> collection) {

            Collection<LEContainer> result = new ArrayList<LEContainer>();

            LEContainer update;
            for (LEContainer container : collection) {

                update = disableContainer(container);
                result.add(update);
            }

            return result;
        }

        /**
         * Create a copy of the container with the disabled membership.
         *
         * @param container container.
         * @return membership check disabled.
         */
        private LEContainer disableContainer(LEContainer container) {

            LEContainer result;
            result = new LEContainer(container.getSource(), container.getLCPeerView().disableLGMembership());

            return result;
        }

    }




    /**
     * Application specific rule set for the cohorts.
     *
     * Created by babbar on 2015-08-11.
     */
    public static class SweepCohortsRuleSet extends CohortsRuleSet {


        public SweepCohortsRuleSet(Comparator<LEContainer> comparator) {
            super(comparator);
        }

        @Override
        public boolean validate(LEContainer leaderContainer, LEContainer selfContainer, Collection<LEContainer> view) {

            boolean promise;
            LEContainer bestContainer = selfContainer;

            if(view != null && !view.isEmpty()){

//              Create a local collection to be sorted.
                List<LEContainer> intermediate = new ArrayList<LEContainer>(view);
                intermediate = reverseSortList(leComparator, intermediate);

//              Anybody better in the collection.
                bestContainer = leComparator.compare(bestContainer, intermediate.get(0)) > 0 ?
                        bestContainer : intermediate.get(0);
            }

            promise = ( leComparator.compare(leaderContainer, bestContainer) >= 0 );
            return promise;
        }
    }

    /**
     * Reverse sort the container list.
     *
     * Reverse sorting needs to be performed
     * because the way comparator is written. The comparator produces
     * +ve value in case element is a better node in terms of utility.
     * Therefore, the best nodes are at the end of the list which needs to reversed.
     *
     * @param comparator comparator.
     * @param list list.
     *
     * @return reverse sorted list.
     */
    private static List<LEContainer> reverseSortList(Comparator<LEContainer> comparator, List<LEContainer> list){

        Collections.sort(list, comparator);
        Collections.reverse(list);

        return list;
    }
}
