

package se.sics.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.types.PeerDescriptor;
import se.sics.p2ptoolbox.election.api.LCPeerView;
import se.sics.p2ptoolbox.election.api.LEContainer;
import se.sics.p2ptoolbox.election.api.rules.CohortsRuleSet;
import se.sics.p2ptoolbox.election.api.rules.LCRuleSet;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
import se.sics.p2ptoolbox.util.traits.Nated;

import java.util.*;

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
        public Collection<DecoratedAddress> initiateLeadership(LEContainer selfContainer, Collection<LEContainer> view, int cohortsSize) {

            List<LEContainer> intermediate = new ArrayList<LEContainer>();
            DecoratedAddress selfAddress = selfContainer.getSource();

//      Stage 1: Check if node self is Nated.
            if (isNated(selfAddress)) {
                return new ArrayList<DecoratedAddress>();
            }

//      Stage 2 : Check if I am the Best node based on utility comparison
            List<LEContainer> list = new ArrayList<LEContainer>(view);
            Collections.sort(list, leComparator);

            if (list.size() > 0 && leComparator.compare(selfContainer, list.get(list.size() -1)) < 0)
                return new ArrayList<DecoratedAddress>();


//      Stage 3: Create list of nodes for the leader group which are not behind nat.
            for (LEContainer container : view) {

                DecoratedAddress address = container.getSource();
                if (isNated(address))
                    continue;

                intermediate.add(container);
            }

//      Stage 4: Check for the length of the list.
            if (intermediate.size() <= cohortsSize)
                return getResult(intermediate);


            return getTopNResults(intermediate, cohortsSize);
        }


        @Override
        public Collection<DecoratedAddress> continueLeadership(LEContainer selfContainer, Collection<LEContainer> collection, int cohortsSize) {

//      IMPORTANT: Create shallow copies of the object to prevent messing with the original objects.
            selfContainer = disableContainer(selfContainer);
            Collection<LEContainer> disabledContainers = disableLGMembership(collection);

//      Delegate the task to the initiate leadership as same process.
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
         * Convenience method to check if the address is
         * behind the nat.
         *
         * @param address
         * @return true if nated.
         */
        private boolean isNated(DecoratedAddress address) {
            return (address.hasTrait(Nated.class));
        }


        /**
         * Sort the cohorts and extract the best to be a part of leader group.
         *
         * @param cohorts
         * @param size
         * @return
         */
        private Collection<DecoratedAddress> getTopNResults(List<LEContainer> cohorts, int size) {

            if (cohorts.size() < size)
                return getResult(cohorts);

            Collections.sort(cohorts, leComparator);
            return getResult(cohorts.subList(0, size));
        }


        /**
         * Convenience method to extract the address information
         * from the collection provided
         *
         * @param cohorts
         * @return
         */
        private Collection<DecoratedAddress> getResult(Collection<LEContainer> cohorts) {

            Collection<DecoratedAddress> result = new ArrayList<DecoratedAddress>();
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

//          Create a local collection to be sorted.
                List<LEContainer> intermediate = new ArrayList<LEContainer>(view);
                Collections.sort(intermediate, leComparator);

//          Anybody better in the collection.
                bestContainer = leComparator.compare(bestContainer, intermediate.get(0)) > 0 ?
                        bestContainer : intermediate.get(0);
            }

            promise = ( leComparator.compare(leaderContainer, bestContainer) >= 0 );
            return promise;
        }
    }




}
