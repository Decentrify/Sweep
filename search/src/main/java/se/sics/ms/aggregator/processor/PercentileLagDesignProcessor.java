//
//package se.sics.ms.aggregator.processor;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import se.sics.ktoolbox.aggregator.server.event.AggregatedInfo;
//import se.sics.ktoolbox.aggregator.server.util.DesignInfoContainer;
//import se.sics.ktoolbox.aggregator.server.util.DesignProcessor;
//import se.sics.ktoolbox.aggregator.util.PacketInfo;
//import se.sics.ms.aggregator.design.PercentileLagDesignInfo;
//import se.sics.ms.aggregator.design.PercentileLagDesignInfoContainer;
//import se.sics.ms.data.InternalStatePacket;
//
//import java.util.*;
//
///**
// * Processor for constructing the percentile lag in the system.
// *
// * Created by babbar on 2015-09-22.
// */
//public class PercentileLagDesignProcessor implements DesignProcessor<PacketInfo, PercentileLagDesignInfo> {
//
//    Logger logger = LoggerFactory.getLogger(PercentileLagDesignProcessor.class);
//
//    @Override
//    public DesignInfoContainer<PercentileLagDesignInfo> process(List<AggregatedInfo> list) {
//        logger.debug("Call to process the windows in the system.");
//
//        Iterator<AggregatedInfo> iterator = list.iterator();
//        Collection<PercentileLagDesignInfo> designInfos= new ArrayList<PercentileLagDesignInfo>();
//
//        while(iterator.hasNext()){
//
//            AggregatedInfo info = iterator.next();
//            Map<Integer, List<PacketInfo>> packetMap = info.getNodePacketMap();
//
//            List<Long> entryList = new ArrayList<Long>();
//            for(Map.Entry<Integer, List<PacketInfo>> entry : packetMap.entrySet()) {
//
//                for(PacketInfo packet : entry.getValue()){
//                    if(packet instanceof InternalStatePacket){
//                        entryList.add(((InternalStatePacket) packet).getNumEntries());
//                    }
//                }
//            }
//
//            PercentileLagDesignInfo lagInfo = getPercentileLagDesignInfo(info.getTime(), entryList);
//            if(lagInfo == null)
//                continue;
//
//            designInfos.add(lagInfo);
//        }
//
//        return new PercentileLagDesignInfoContainer(designInfos);
//    }
//
//    @Override
//    public void cleanState() {
//        logger.debug("Call to clean the internal state of the processor.");
//    }
//
//
//
//    public PercentileLagDesignInfo getPercentileLagDesignInfo( long time, List<Long> entryList){
//
//        Collections.sort(entryList);
//
//        if(entryList.isEmpty()){
//            return null;
//        }
//
//        long maxVal = entryList.get(entryList.size()-1);
//        List<Long> lagList = new ArrayList<Long>();
//
//        for(Long entry : entryList){
//            lagList.add(maxVal - entry);
//        }
//
//        Collections.sort(lagList);
//
//        long fifty = lagList.get((lagList.size() * 50) / 100);
//        long seventyFive = lagList.get((lagList.size() * 75) / 100);
//        long ninety = lagList.get((lagList.size() * 90) / 100);
//
//        return new PercentileLagDesignInfo(time, fifty, seventyFive, ninety);
//    }
//
//
//
//}
