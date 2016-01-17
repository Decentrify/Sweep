package se.sics.ms.util;

import se.sics.kompics.network.Transport;

import java.util.*;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ktoolbox.util.network.basic.DecoratedHeader;

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


    /**
     * Convenience Constructor method for the basic content message.
     * @param header header
     * @param content content
     * @param <E> type
     * @return content message
     */
    public static <E> KContentMsg<KAddress, KHeader<KAddress>, E> getDecoratedContentMsg(KHeader<KAddress> header, E content){
        return new BasicContentMsg(header, content);
    }
    
    
    public static <E> BasicContentMsg<KAddress, KHeader<KAddress>, E> getDecoratedContentMessage(KAddress sourceAddress, KAddress destination, Transport transport, E content){
        
        KHeader<KAddress> decoratedHeader = new BasicHeader(sourceAddress, destination,  transport);
        return new BasicContentMsg(decoratedHeader, content);
        
    }

    /**
     * Convenience Constructor for the basic content message
     * having additional overlay id information.
     *  
     * @param sourceAddress addr
     * @param dest destination
     * @param transport UDP/ TCP/ UDT
     * @param overlayId overlay
     * @param content container
     * @param <E> type
     * @return Basic Network Message
     */
    public static <E> BasicContentMsg<KAddress, DecoratedHeader<KAddress>, E> getDecoratedMsgWithOverlay (KAddress sourceAddress, KAddress dest, Transport transport, Identifier overlayId, E content){

        BasicHeader<KAddress> basicHeader = new BasicHeader(sourceAddress, dest,  transport);
        DecoratedHeader<KAddress> decoratedHeader = new DecoratedHeader(basicHeader, overlayId);
        return new BasicContentMsg(decoratedHeader, content);
    }
}
