package se.sics.ms.net;

import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.data.*;
import se.sics.ms.serializer.*;
import se.sics.ms.types.Id;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchDescriptor;
import se.sics.ms.types.SearchPattern;
import se.sics.ms.util.PartitionHelper;
import se.sics.p2ptoolbox.util.serializer.BasicSerializerSetup;

/**
 * Main class for the setting up of the serializers in the system as well as the basic serializers used.
 * Created by babbar on 2015-04-21.
 */
public class SerializerSetup {


    public static enum SweepSerializers {

        searchDescriptor(SearchDescriptor.class, "applicationDescriptor"),
        searchPattern(SearchPattern.class, "searchPattern"),
        indexEntry(IndexEntry.class, "indexEntry"),
        partitionInfo(PartitionHelper.PartitionInfo.class, "partitionInfo"),
        idInfo(Id.class, "idInfo"),

        searchInfoRequest(SearchInfo.Request.class, "searchInfoRequest"),
        searchInfoResponse(SearchInfo.Response.class, "searchInfoResponse"),

        addEntryRequest(AddIndexEntry.Request.class, "addIndexEntryRequest"),
        addEntryResponse(AddIndexEntry.Response.class, "addIndexEntryResponse"),
        replicationPrepareRequest(ReplicationPrepareCommit.Request.class, "replicationPrepareCommitRequest"),
        replicationPrepareResponse(ReplicationPrepareCommit.Response.class, "replicationPrepareCommitResponse"),
        replicationCommitRequest(ReplicationCommit.Request.class, "replicationCommitRequest"),

        controlInfoRequest(ControlInformation.Request.class, "controlInformationRequest"),
        controlInfoResponse(ControlInformation.Response.class, "controlInformationResponse"),
        partitioningPrepareRequest(PartitionPrepare.Request.class, "partitioningPrepareRequest"),
        partitioningPrepareResponse(PartitionPrepare.Response.class, "partitioningPrepareResponse"),
        partitioningCommitRequest(PartitionCommit.Request.class, "partitionCommitRequest"),
        partitioningCommitResponse(PartitionCommit.Response.class, "partitionCommitResponse"),
        delayedPartitioningRequest(DelayedPartitioning.Request.class, "delayedPartitioningRequest"),
        delayedPartitioningResponse(DelayedPartitioning.Response.class, "delayedPartitioningResponse"),

        indexExchangeRequest(IndexExchange.Request.class, "indexExchangeRequest"),
        indexExchangeResponse(IndexExchange.Response.class, "indexExchangeResponse"),
        repairRequest(Repair.Request.class, "repairRequest"),
        repairResponse(Repair.Response.class, "repairResponse"),

        leaderLookupRequest(LeaderLookup.Request.class, "leaderLookupRequest"),
        leaderLookupResponse(LeaderLookup.Response.class, "leaderLookupResponse");


        private final Class serializedClass;
        private final String serializerName;

        private SweepSerializers(Class serializedClass, String serializerName){
            this.serializedClass =serializedClass;
            this.serializerName = serializerName;
        }
    }

    public static void checkSetup(){

        for(SweepSerializers ss : SweepSerializers.values()){

            if (Serializers.lookupSerializer(ss.serializedClass) == null) {
                throw new RuntimeException("No serializer for " + ss.serializedClass);
            }

        }

        BasicSerializerSetup.checkSetup();
    }


    public static int registerSerializers (int startId){

        int currentId = startId;

        // Main Object Serializers.
        SearchDescriptorSerializer sdSerializer = new SearchDescriptorSerializer(currentId++);
        Serializers.register(sdSerializer, SweepSerializers.searchDescriptor.serializerName);
        Serializers.register(SweepSerializers.searchDescriptor.serializedClass, SweepSerializers.searchDescriptor.serializerName);

        SearchPatternSerializer spSerializer = new SearchPatternSerializer(currentId++);
        Serializers.register(spSerializer, SweepSerializers.searchPattern.serializerName);
        Serializers.register(SweepSerializers.searchPattern.serializedClass, SweepSerializers.searchPattern.serializerName);

        IndexEntrySerializer indexEntrySerializer = new IndexEntrySerializer(currentId++);
        Serializers.register(indexEntrySerializer, SweepSerializers.indexEntry.serializerName);
        Serializers.register(SweepSerializers.indexEntry.serializedClass, SweepSerializers.indexEntry.serializerName);

        PartitionInfoSerializer piSerializer = new PartitionInfoSerializer(currentId++);
        Serializers.register(piSerializer, SweepSerializers.partitionInfo.serializerName);
        Serializers.register(SweepSerializers.partitionInfo.serializedClass, SweepSerializers.partitionInfo.serializerName);

        IdSerializer idSerializer = new IdSerializer(currentId++);
        Serializers.register(idSerializer, SweepSerializers.idInfo.serializerName);
        Serializers.register(SweepSerializers.idInfo.serializedClass, SweepSerializers.idInfo.serializerName);


        // Search Request / Response.
        SearchInfoSerializer.Request siRequest = new SearchInfoSerializer.Request(currentId++);
        Serializers.register(siRequest, SweepSerializers.searchInfoRequest.serializerName);
        Serializers.register(SweepSerializers.searchInfoRequest.serializedClass, SweepSerializers.searchInfoRequest.serializerName);

        SearchInfoSerializer.Response siResponse = new SearchInfoSerializer.Response(currentId++);
        Serializers.register(siResponse, SweepSerializers.searchInfoResponse.serializerName);
        Serializers.register(SweepSerializers.searchInfoResponse.serializedClass, SweepSerializers.searchInfoResponse.serializerName);


        // Add Index Entry Protocol.
        AddIndexEntrySerializer.Request aieRequest = new AddIndexEntrySerializer.Request(currentId++);
        Serializers.register(aieRequest, SweepSerializers.addEntryRequest.serializerName);
        Serializers.register(SweepSerializers.addEntryRequest.serializedClass, SweepSerializers.addEntryRequest.serializerName);

        AddIndexEntrySerializer.Response aieResponse = new AddIndexEntrySerializer.Response(currentId++);
        Serializers.register(aieResponse, SweepSerializers.addEntryResponse.serializerName);
        Serializers.register(SweepSerializers.addEntryResponse.serializedClass, SweepSerializers.addEntryResponse.serializerName);

        ReplicationPrepareCommitSerializer.Request rpcRequest = new ReplicationPrepareCommitSerializer.Request(currentId++);
        Serializers.register(rpcRequest, SweepSerializers.replicationPrepareRequest.serializerName);
        Serializers.register(SweepSerializers.replicationPrepareRequest.serializedClass, SweepSerializers.replicationPrepareRequest.serializerName);

        ReplicationPrepareCommitSerializer.Response rpcResponse = new ReplicationPrepareCommitSerializer.Response(currentId++);
        Serializers.register(rpcResponse, SweepSerializers.replicationPrepareResponse.serializerName);
        Serializers.register(SweepSerializers.replicationPrepareResponse.serializedClass, SweepSerializers.replicationPrepareResponse.serializerName);

        ReplicationCommitSerializer.Request rcRequest = new ReplicationCommitSerializer.Request(currentId++);
        Serializers.register(rcRequest, SweepSerializers.replicationCommitRequest.serializerName);
        Serializers.register(SweepSerializers.replicationCommitRequest.serializedClass, SweepSerializers.replicationCommitRequest.serializerName);


        // Two Phase Commit Protocol.
        ControlInformationSerializer.Request ciRequest = new ControlInformationSerializer.Request(currentId++);
        Serializers.register(ciRequest, SweepSerializers.controlInfoRequest.serializerName);
        Serializers.register(SweepSerializers.controlInfoRequest.serializedClass, SweepSerializers.controlInfoRequest.serializerName);

        ControlInformationSerializer.Response ciResponse = new ControlInformationSerializer.Response(currentId++);
        Serializers.register(ciResponse, SweepSerializers.controlInfoResponse.serializerName);
        Serializers.register(SweepSerializers.controlInfoResponse.serializedClass, SweepSerializers.controlInfoResponse.serializerName);

        PartitionPrepareSerializer.Request ppRequest = new PartitionPrepareSerializer.Request(currentId++);
        Serializers.register(ppRequest, SweepSerializers.partitioningPrepareRequest.serializerName);
        Serializers.register(SweepSerializers.partitioningPrepareRequest.serializedClass, SweepSerializers.partitioningPrepareRequest.serializerName);

        PartitionPrepareSerializer.Response ppResponse = new PartitionPrepareSerializer.Response(currentId++);
        Serializers.register(ppResponse, SweepSerializers.partitioningPrepareResponse.serializerName);
        Serializers.register(SweepSerializers.partitioningPrepareResponse.serializedClass, SweepSerializers.partitioningPrepareResponse.serializerName);

        PartitionCommitSerializer.Request pcRequest = new PartitionCommitSerializer.Request(currentId++);
        Serializers.register(pcRequest, SweepSerializers.partitioningCommitRequest.serializerName);
        Serializers.register(SweepSerializers.partitioningCommitRequest.serializedClass, SweepSerializers.partitioningCommitRequest.serializerName);

        PartitionCommitSerializer.Response pcResponse = new PartitionCommitSerializer.Response(currentId++);
        Serializers.register(pcResponse, SweepSerializers.partitioningCommitResponse.serializerName);
        Serializers.register(SweepSerializers.partitioningCommitResponse.serializedClass, SweepSerializers.partitioningCommitResponse.serializerName);

        DelayedPartitioningSerializer.Request dpRequest = new DelayedPartitioningSerializer.Request(currentId++);
        Serializers.register(dpRequest, SweepSerializers.delayedPartitioningRequest.serializerName);
        Serializers.register(SweepSerializers.delayedPartitioningRequest.serializedClass, SweepSerializers.delayedPartitioningRequest.serializerName);

        DelayedPartitioningSerializer.Response dpResponse = new DelayedPartitioningSerializer.Response(currentId++);
        Serializers.register(dpResponse, SweepSerializers.delayedPartitioningResponse.serializerName);
        Serializers.register(SweepSerializers.delayedPartitioningResponse.serializedClass, SweepSerializers.delayedPartitioningResponse.serializerName);


        //Index Exchange Protocol.
        IndexExchangeSerializer.Request ieRequest = new IndexExchangeSerializer.Request(currentId++);
        Serializers.register(ieRequest, SweepSerializers.indexExchangeRequest.serializerName);
        Serializers.register(SweepSerializers.indexExchangeRequest.serializedClass, SweepSerializers.indexExchangeRequest.serializerName);

        IndexExchangeSerializer.Response ieResponse = new IndexExchangeSerializer.Response(currentId++);
        Serializers.register(ieResponse, SweepSerializers.indexExchangeResponse.serializerName);
        Serializers.register(SweepSerializers.indexExchangeResponse.serializedClass, SweepSerializers.indexExchangeResponse.serializerName);

        RepairSerializer.Request rRequest = new RepairSerializer.Request(currentId++);
        Serializers.register(rRequest, SweepSerializers.repairRequest.serializerName);
        Serializers.register(SweepSerializers.repairRequest.serializedClass, SweepSerializers.repairRequest.serializerName);

        RepairSerializer.Response rResponse = new RepairSerializer.Response(currentId++);
        Serializers.register(rResponse, SweepSerializers.repairResponse.serializerName);
        Serializers.register(SweepSerializers.repairResponse.serializedClass, SweepSerializers.repairResponse.serializerName);

        // Leader Look up
        LeaderLookUpSerializer.Request llRequest = new LeaderLookUpSerializer.Request(currentId++);
        Serializers.register(llRequest, SweepSerializers.leaderLookupRequest.serializerName);
        Serializers.register(SweepSerializers.leaderLookupRequest.serializedClass, SweepSerializers.leaderLookupRequest.serializerName);

        LeaderLookUpSerializer.Response llResponse = new LeaderLookUpSerializer.Response(currentId++);
        Serializers.register(llResponse, SweepSerializers.leaderLookupResponse.serializerName);
        Serializers.register(SweepSerializers.leaderLookupResponse.serializedClass, SweepSerializers.leaderLookupResponse.serializerName);

        return currentId;
    }

}
