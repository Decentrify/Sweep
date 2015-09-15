package se.sics.ms.common;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.types.PartitionId;
import se.sics.ms.types.PeerDescriptor;
import se.sics.ms.util.CommonHelper;
import se.sics.ms.util.ComparatorCollection;
import se.sics.ms.util.PartitionHelper;
import se.sics.p2ptoolbox.croupier.util.CroupierContainer;
import se.sics.p2ptoolbox.gradient.util.GradientLocalView;
import se.sics.p2ptoolbox.util.Container;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;

import java.util.*;

/**
 * Container the routing table information
 * and all its associated operations.
 * <p/>
 * Created by babbar on 2015-04-17.
 */
public class RoutingTableHandler {

    private Map<MsConfig.Categories, Map<Integer, Pair<Integer, HashMap<Integer, RoutingTableContainer>>>> routingTable;
    private Logger logger = LoggerFactory.getLogger(RoutingTableHandler.class);
    private int maxRoutingEntries;
    private Comparator<RoutingTableContainer> ageComparator;

    public RoutingTableHandler(int maxRoutingEntries) {

        routingTable = new HashMap<MsConfig.Categories, Map<Integer, Pair<Integer, HashMap<Integer, RoutingTableContainer>>>>();
        this.maxRoutingEntries = maxRoutingEntries;
        this.ageComparator = new ComparatorCollection.AgeComparator();
    }


    /**
     * Based on the provided collection of the nodes, update the routing table information.
     *
     * @param croupierSample Collection of nodes.
     */
    public void addEntriesToRoutingTable(Collection<Container> croupierSample) {

        for (Container container : croupierSample) {

            if (!(container instanceof CroupierContainer)) {
                continue;
            }

            CroupierContainer croupierContainer = (CroupierContainer) container;

            //TODO: Temp fix Alex Fix Container Data.
            PeerDescriptor descriptor = (PeerDescriptor)((GradientLocalView)croupierContainer.getContent()).appView;
            MsConfig.Categories category = categoryFromCategoryId(descriptor.getOverlayId().getCategoryId());

            PartitionId partitionInfo = new PartitionId(descriptor.getOverlayAddress().getPartitioningType(),
                    descriptor.getOverlayAddress().getPartitionIdDepth(), descriptor.getOverlayAddress().getPartitionId());

            Map<Integer, Pair<Integer, HashMap<Integer, RoutingTableContainer>>> categoryRoutingMap = routingTable.get(category);

            if (categoryRoutingMap == null) {
                categoryRoutingMap = new HashMap<Integer, Pair<Integer, HashMap<Integer, RoutingTableContainer>>>();
                routingTable.put(category, categoryRoutingMap);
            }

            Pair<Integer, HashMap<Integer, RoutingTableContainer>> partitionBucket = categoryRoutingMap.get(partitionInfo.getPartitionId());

            if (partitionBucket == null) {
                partitionBucket = checkAndAddNewBucket(partitionInfo, categoryRoutingMap);
            } else {
                int comparisonResult = new Integer(partitionInfo.getPartitionIdDepth()).compareTo(partitionBucket.getValue0());

                if (comparisonResult > 0) {
                    logger.debug("Need to remove the old bucket and create own");
                    partitionBucket = checkAndAddNewBucket(partitionInfo, categoryRoutingMap);
                } else if (comparisonResult < 0) {
                    logger.debug("Received a node with lower partition depth, not incorporating it.");
                    continue;
                }
            }

            addToPartitionBucket(croupierContainer, partitionBucket);
        }
    }


    /**
     * Based on the sample supplied, check if the same entry is located in the bucket.
     * Replace based on newer age.<br\>
     * <p/>
     * <b>CAUTION:</b> It might be possible that you may replace a newer sample by an old one because of this
     * but eventually the old sample should be removed as the node will be constantly pushing the sample in network with zero age.
     *
     * @param croupierContainer SampleContainer
     * @param partitionBucket   PartitionBucket for a particular partition id and partitioning depth.
     */
    private void addToPartitionBucket(CroupierContainer croupierContainer, Pair<Integer, HashMap<Integer, RoutingTableContainer>> partitionBucket) {

        BasicAddress receivedBaseAddress = croupierContainer.getSource().getBase();
        RoutingTableContainer selfContainer = partitionBucket.getValue1().get(receivedBaseAddress.getId());

        if (selfContainer != null) {

            if (selfContainer.getAge() >= croupierContainer.getAge()) {
                partitionBucket.getValue1().remove(receivedBaseAddress.getId());
            } else
                return;
        }

        selfContainer = new RoutingTableContainer(croupierContainer.getAge(), croupierContainer.getSource(), (PeerDescriptor)((GradientLocalView)croupierContainer.getContent()).appView);
        partitionBucket.getValue1().put(receivedBaseAddress.getId(), selfContainer);

        if (partitionBucket.getValue1().size() > maxRoutingEntries) {

            List<RoutingTableContainer> sortedList = CommonHelper.sortCollection(partitionBucket.getValue1().values(), ageComparator);
            if (sortedList != null && sortedList.size() > 0) {

                // Remove the first element in case we exceed the size.
                partitionBucket.getValue1().remove(sortedList.get(0).getSource().getBase().getId());
            }
        }
    }


    /**
     * Based on the partition and routing map information, check for old  buckets, clean them up and add new buckets with updated
     * partition information.
     *
     * @param partitionInfo      Partition Id of the new sample.
     * @param categoryRoutingMap Current Routing Map.
     */
    private Pair<Integer, HashMap<Integer, RoutingTableContainer>> checkAndAddNewBucket(PartitionId partitionInfo, Map<Integer, Pair<Integer, HashMap<Integer, RoutingTableContainer>>> categoryRoutingMap) {

        Pair<Integer, HashMap<Integer, RoutingTableContainer>> newPartitionBucket = Pair.with(partitionInfo.getPartitionIdDepth(), new HashMap<Integer, RoutingTableContainer>());
        removeOldBuckets(partitionInfo, categoryRoutingMap);
        categoryRoutingMap.put(partitionInfo.getPartitionId(), newPartitionBucket);
        logger.debug("Creating new bucket for the partition id: {}, partitiondepth: {}", partitionInfo.getPartitionId(), partitionInfo.getPartitionIdDepth());


        // If not the first bucket, then push in the bucket for other partition also.

        if (!isFirstBucket(partitionInfo)) {

            int otherPartitionId = PartitionHelper.getPartitionIdOtherHalf(partitionInfo);
            Pair<Integer, HashMap<Integer, RoutingTableContainer>> otherPartitionBucket = Pair.with(partitionInfo.getPartitionIdDepth(), new HashMap<Integer, RoutingTableContainer>());
            categoryRoutingMap.put(otherPartitionId, otherPartitionBucket);
            logger.debug("Creating new bucket for the partition id: {}, partitiondepth: {}", otherPartitionId, partitionInfo.getPartitionIdDepth());
        }

        return newPartitionBucket;
    }


    /**
     * Helper method to spill the contents in new bucket as a new partition is detected.
     * Based on the new partition id passed, look at the previous buckets and then remove the contents.
     * Create new buckets for left spill and right spill. <br\><br\>
     * <p/>
     * <b>CAUTION:</b> For now given that partition is an event which would happen after a long time, we will be only looking at the bucket before us.
     * If the bucket is present then move the contents to new buckets respectively. For the case in which the difference between the buckets
     * is greater than 1, needs to be handled separately.
     *
     * @param partition          Updated Partition Id.
     * @param categoryRoutingMap Current Routing Map.
     */
    public void removeOldBuckets(PartitionId partition, Map<Integer, Pair<Integer, HashMap<Integer, RoutingTableContainer>>> categoryRoutingMap) {


        if (partition == null) {
            throw new IllegalArgumentException("Partition Id is null");
        }

        if (partition.getPartitionIdDepth() == 0)
            return;

        int oldPartitionId = PartitionHelper.getPreviousPartitionId(partition);

        if (categoryRoutingMap.get(oldPartitionId) == null) {
            logger.warn("Unable to find partition bucket for previous partition: {}", oldPartitionId);
            return;
        }

        // Simply remove the old map. We can remove here the old map because the method is called when we couldn't find any map with the id passed.
        categoryRoutingMap.remove(oldPartitionId);
    }



    /**
     * Iterate over the routing table and increment the ages of the descriptor.
     * Help to remove the old nodes in the system.
     */
    public void incrementRoutingTableDescriptorAges() {

        for (Map<Integer, Pair<Integer, HashMap<Integer, RoutingTableContainer>>> categoryRoutingMap : routingTable.values()) {

            for (Pair<Integer, HashMap<Integer, RoutingTableContainer>> bucket : categoryRoutingMap.values()) {

                for (RoutingTableContainer routingTableContainer : bucket.getValue1().values()) {
                    routingTableContainer.incrementAge();
                }
            }
        }
    }



    /**
     * Based on the partitioning information, check the depth to determine if it is first bucket.
     *
     * @param partitionInfo partition information.
     * @return true is first bucket.
     */
    private boolean isFirstBucket(PartitionId partitionInfo) {
        return partitionInfo.getPartitionIdDepth() == 0;
    }


    /**
     * Get the category information based on the id of the enum value.
     *
     * @param categoryId category id.
     * @return Category.
     */
    private MsConfig.Categories categoryFromCategoryId(int categoryId) {
        return MsConfig.Categories.values()[categoryId];
    }


    /**
     * Based on the provided category return the corresponding
     * category routing map.
     *
     * @param category category map
     * @return Map
     */
    public Map<Integer, Pair<Integer, HashMap<Integer, RoutingTableContainer>>> getCategoryRoutingMap(MsConfig.Categories category){
        return this.routingTable.get(category);
    }



    public Collection<Map<Integer, Pair<Integer, HashMap<Integer, RoutingTableContainer>>>> values(){
        return this.routingTable.values();
    }

}
