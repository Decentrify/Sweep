package se.sics.ms.util;

import java.util.Comparator;

/**
 * Created by babbar on 2015-05-27.
 */
public class GenComparator implements Comparator<Super>{

    @Override
    public int compare(Super o1, Super o2) {
        return Integer.compare(o1.val, o2.val);
    }
}
