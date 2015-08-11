package se.sics.util;

import se.sics.p2ptoolbox.election.api.LEContainer;
import se.sics.p2ptoolbox.election.api.rules.CohortsRuleSet;

import java.util.*;

/**
 * Application specific rule set for the cohorts.
 *
 * Created by babbar on 2015-08-11.
 */
public class SweepCohortsRuleSet extends CohortsRuleSet{


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
