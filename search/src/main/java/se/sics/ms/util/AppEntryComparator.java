package se.sics.ms.util;

import se.sics.ms.types.ApplicationEntry;

import java.util.Comparator;

/**
 * Comparator for the application entries,
 * falling on the implicit comparison of the Application EntryId.
 *
 * Created by babbar on 2015-06-05.
 */
public class AppEntryComparator implements Comparator<ApplicationEntry> {

    @Override
    public int compare(ApplicationEntry o1, ApplicationEntry o2) {

        if(o1 == o2)
            return 0;

        return o1.getApplicationEntryId().compareTo(o2.getApplicationEntryId());
    }
}
