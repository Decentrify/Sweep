package se.sics.ms.util;

import java.util.*;

/**
 * Common Helper Common Helper for the
 * Created by babbar on 2015-04-18.
 */
public class CommonHelper {

    /**
     * Generic method used to return a sorted list.
     * @param collection Any Collection of samples.
     * @param comparator Comparator for sorting.
     * @param <E> Collection Type
     *
     * @return Sorted Collection
     */
    public static  <E> List<E> sortCollection(Collection<E> collection, Comparator<E> comparator){

        List<E> list = new ArrayList<E>();
        list.addAll(collection);
        Collections.sort(list, comparator);

        return list;
    }



}
