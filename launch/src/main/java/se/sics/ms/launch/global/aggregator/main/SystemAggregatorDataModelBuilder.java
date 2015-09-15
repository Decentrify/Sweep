//package se.sics.ms.launch.global.aggregator.main;
//
//import se.sics.ms.aggregator.data.ComponentUpdate;
//import se.sics.ms.aggregator.data.SweepAggregatedPacket;
//import se.sics.ms.data.aggregator.ElectionLeaderComponentUpdate;
//import se.sics.ms.data.aggregator.SearchComponentUpdate;
//import se.sics.ms.launch.global.aggregator.model.SimpleDataModel;
//import se.sics.p2ptoolbox.util.network.impl.BasicAddress;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
///**
// * Clean Up the data and provide us the view on the data.
// *
// * Created by babbarshaer on 2015-03-20.
// */
//public class SystemAggregatorDataModelBuilder {
//
//
//    public static List<SimpleDataModel> getSimpleDataModel(Map<BasicAddress, SweepAggregatedPacket> globalStateMap){
//
//        List<SimpleDataModel> sdmList = new ArrayList<SimpleDataModel>();
//
//        for(BasicAddress src: globalStateMap.keySet()){
//
//            SimpleDataModel sdm = new SimpleDataModel(src.getId());
//            Map<Class, Map<Integer,ComponentUpdate>> componentDataMap = globalStateMap.get(src).getComponentDataMap();
//
//
//            for(Class updateClassType : componentDataMap.keySet()){
//
//                for(ComponentUpdate update : componentDataMap.get(updateClassType).values()){
//
//                    if(update instanceof SearchComponentUpdate){
//                        SearchComponentUpdate scp = (SearchComponentUpdate) update;
//                        sdm.setPartitionId(scp.getPartitionId());
//                        sdm.setPartitionDepth(scp.getPartitionDepth());
//                        sdm.setNumberOfEntries(scp.getNumberOfEntries());
//
//                        break;
//                    }
//
//                    else if(update instanceof ElectionLeaderComponentUpdate){
//                        ElectionLeaderComponentUpdate elcu = (ElectionLeaderComponentUpdate) update;
//                        sdm.setLeader(elcu.isLeader());
//
//                        break;
//                    }
//                }
//            }
//
//            sdmList.add(sdm);
//        }
//
//        return sdmList;
//    }
//
//}
