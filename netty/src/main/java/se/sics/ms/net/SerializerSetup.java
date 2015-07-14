package se.sics.ms.net;

import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.aggregator.data.SweepAggregatedPacket;
import se.sics.ms.aggregator.serializer.SweepPacketSerializer;
import se.sics.ms.data.*;
import se.sics.ms.data.aggregator.ElectionLeaderComponentUpdate;
import se.sics.ms.data.aggregator.ElectionLeaderUpdateSerializer;
import se.sics.ms.data.aggregator.SearchComponentUpdate;
import se.sics.ms.data.aggregator.SearchComponentUpdateSerializer;
import se.sics.ms.serializer.*;
import se.sics.ms.types.*;
import se.sics.ms.util.PartitionHelper;
import se.sics.p2ptoolbox.util.serializer.BasicSerializerSetup;

/**
 * Main class for the setting up of the serializers in the system as well as the basic serializers used.
 * Created by babbar on 2015-04-21.
 */
public class SerializerSetup {


    public static enum SweepSerializers {

        searchDescriptor(PeerDescriptor.class, "applicationDescriptor"),
        searchPattern(SearchPattern.class, "searchPattern"),
        indexEntry(IndexEntry.class, "indexEntry"),
        partitionInfo(PartitionHelper.PartitionInfo.class, "partitionInfo"),
        idInfo(Id.class, "idInfo"),
        indexHash(IndexHash.class, "indexHash"),
        partitionInfoHash(PartitionHelper.PartitionInfoHash.class, "partitionInfoHash"),
        entryHash(EntryHash.class, "entryHash"),
        overlayId(OverlayId.class, "overlayId"),
        baseLeaderUnit(BaseLeaderUnit.class, "baseLeaderUnit"),
        leaderUnitUpdate(LeaderUnitUpdate.class, "leaderUnitUpdate"),
        
        // aggregators
        searchComponentUpdate(SearchComponentUpdate.class, "searchComponentUpdate"),
        leaderComponentUpdate(ElectionLeaderComponentUpdate.class, "leaderComponentUpdate"),
        sweepAggregatedPacket(SweepAggregatedPacket.class, "sweepAggregatedPacket"),

        searchInfoRequest(SearchInfo.Request.class, "searchInfoRequest"),
        searchInfoResponse(SearchInfo.Response.class, "searchInfoResponse"),
        searchInfoResponseUpdated(SearchInfo.ResponseUpdated.class, "searchInfoResponseUpdated"),

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

        indexHashExchangeRequest(IndexHashExchange.Request.class, "indexHashExchangeRequest"),
        indexHashExchangeResponse(IndexHashExchange.Response.class, "indexHashExchangeResponse"),
        indexExchangeRequest(IndexExchange.Request.class, "indexExchangeRequest"),
        indexExchangeResponse(IndexExchange.Response.class, "indexExchangeResponse"),
        repairRequest(Repair.Request.class, "repairRequest"),
        repairResponse(Repair.Response.class, "repairResponse"),

        leaderLookupRequest(LeaderLookup.Request.class, "leaderLookupRequest"),
        leaderLookupResponse(LeaderLookup.Response.class, "leaderLookupResponse"),

        applicationEntryId(ApplicationEntry.ApplicationEntryId.class, "applicationEntryId"),
        applicationEntry(ApplicationEntry.class, "applicationEntry"),
        leaderPullEntryRequest(LeaderPullEntry.Request.class, "leaderPullEntryRequest"),
        leaderPullEntryResponse(LeaderPullEntry.Response.class, "leaderPullEntryResponse"),
        entryHashExchangeRequest(EntryHashExchange.Request.class, "entryHashExchangeRequest"),
        entryHashExchangeResponse(EntryHashExchange.Response.class, "entryHashExchangeResponse"),
        entryExchangeRequest(EntryExchange.Request.class, "entryExchangeRequest"),
        entryExchangeResponse(EntryExchange.Response.class, "entryExchangeResponse"),

        controlPullRequest(ControlPull.Request.class, "controlPullRequest"),
        controlPullResponse(ControlPull.Response.class, "controlPullResponse"),
        shardingPrepareRequest(ShardingPrepare.Request.class, "shardingPrepareRequest"),
        shardingPrepareResponse(ShardingPrepare.Response.class, "shardingPrepareResponse"),
        shardingCommitRequest(ShardingCommit.Request.class, "shardingCommitRequest"),
        shardingCommitResponse(ShardingCommit.Response.class, "shardingCommitResponse"),

        landingEntryAddPrepareRequest(LandingEntryAddPrepare.Request.class, "landingEntryAddPrepareRequest"),
        landingEntryAddPrepareResponse(LandingEntryAddPrepare.Response.class, "landingEntryAddPrepareResponse"),
        applicationEntryAddPrepareRequest(ApplicationEntryAddPrepare.Request.class, "applicationEntryAddPrepareRequest"),
        applicationEntryAddPrepareResponse(ApplicationEntryAddPrepare.Response.class, "applicationEntryAddPrepareResponse"),

        entryAddCommitRequest(EntryAddCommit.Request.class, "entryAddCommitRequest");

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

        IndexHashSerializer indexHashSerializer = new IndexHashSerializer(currentId++);
        Serializers.register(indexHashSerializer, SweepSerializers.indexHash.serializerName);
        Serializers.register(SweepSerializers.indexHash.serializedClass, SweepSerializers.indexHash.serializerName);

        PartitionInfoHashSerializer partitionHashSerializer = new PartitionInfoHashSerializer(currentId++);
        Serializers.register(partitionHashSerializer, SweepSerializers.partitionInfoHash.serializerName);
        Serializers.register(SweepSerializers.partitionInfoHash.serializedClass, SweepSerializers.partitionInfoHash.serializerName);

        // Aggregators.
        SearchComponentUpdateSerializer scuSerializer = new SearchComponentUpdateSerializer(currentId++);
        Serializers.register(scuSerializer, SweepSerializers.searchComponentUpdate.serializerName);
        Serializers.register(SweepSerializers.searchComponentUpdate.serializedClass, SweepSerializers.searchComponentUpdate.serializerName);

        ElectionLeaderUpdateSerializer lcuSerializer = new ElectionLeaderUpdateSerializer(currentId++);
        Serializers.register(lcuSerializer, SweepSerializers.leaderComponentUpdate.serializerName);
        Serializers.register(SweepSerializers.leaderComponentUpdate.serializedClass, SweepSerializers.leaderComponentUpdate.serializerName);

        SweepPacketSerializer sapSerializer = new SweepPacketSerializer(currentId++);
        Serializers.register(sapSerializer, SweepSerializers.sweepAggregatedPacket.serializerName);
        Serializers.register(SweepSerializers.sweepAggregatedPacket.serializedClass, SweepSerializers.sweepAggregatedPacket.serializerName);

        // Search Request / Response.
        SearchInfoSerializer.Request siRequest = new SearchInfoSerializer.Request(currentId++);
        Serializers.register(siRequest, SweepSerializers.searchInfoRequest.serializerName);
        Serializers.register(SweepSerializers.searchInfoRequest.serializedClass, SweepSerializers.searchInfoRequest.serializerName);

        SearchInfoSerializer.Response siResponse = new SearchInfoSerializer.Response(currentId++);
        Serializers.register(siResponse, SweepSerializers.searchInfoResponse.serializerName);
        Serializers.register(SweepSerializers.searchInfoResponse.serializedClass, SweepSerializers.searchInfoResponse.serializerName);

        SearchInfoSerializer.ResponseUpdated siResponseUpdated = new SearchInfoSerializer.ResponseUpdated(currentId++);
        Serializers.register(siResponseUpdated, SweepSerializers.searchInfoResponseUpdated.serializerName);
        Serializers.register(SweepSerializers.searchInfoResponseUpdated.serializedClass, SweepSerializers.searchInfoResponseUpdated.serializerName);

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

        IndexHashExchangeSerializer.Request hashExchangeRequest = new IndexHashExchangeSerializer.Request(currentId++);
        Serializers.register(hashExchangeRequest, SweepSerializers.indexHashExchangeRequest.serializerName);
        Serializers.register(SweepSerializers.indexHashExchangeRequest.serializedClass, SweepSerializers.indexHashExchangeRequest.serializerName);

        IndexHashExchangeSerializer.Response hashExchangeResponse = new IndexHashExchangeSerializer.Response(currentId++);
        Serializers.register(hashExchangeResponse, SweepSerializers.indexHashExchangeResponse.serializerName);
        Serializers.register(SweepSerializers.indexHashExchangeResponse.serializedClass, SweepSerializers.indexHashExchangeResponse.serializerName);


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


        // Epoch Update Serializers.
        ApplicationEntryIdSerializer entryIdSerializer = new ApplicationEntryIdSerializer(currentId++);
        Serializers.register(entryIdSerializer, SweepSerializers.applicationEntryId.serializerName);
        Serializers.register(SweepSerializers.applicationEntryId.serializedClass, SweepSerializers.applicationEntryId.serializerName);


        // Epoch Update Serializers.
        ApplicationEntrySerializer applicationEntrySerializer = new ApplicationEntrySerializer(currentId++);
        Serializers.register(applicationEntrySerializer, SweepSerializers.applicationEntry.serializerName);
        Serializers.register(SweepSerializers.applicationEntry.serializedClass, SweepSerializers.applicationEntry.serializerName);


        LeaderPullEntrySerializer.Request leaderPullEntryRequestSerializer = new LeaderPullEntrySerializer.Request(currentId++);
        Serializers.register(leaderPullEntryRequestSerializer, SweepSerializers.leaderPullEntryRequest.serializerName);
        Serializers.register(SweepSerializers.leaderPullEntryRequest.serializedClass, SweepSerializers.leaderPullEntryRequest.serializerName);


        LeaderPullEntrySerializer.Response leaderPullEntryResponseSerializer = new LeaderPullEntrySerializer.Response(currentId++);
        Serializers.register(leaderPullEntryResponseSerializer, SweepSerializers.leaderPullEntryResponse.serializerName);
        Serializers.register(SweepSerializers.leaderPullEntryResponse.serializedClass, SweepSerializers.leaderPullEntryResponse.serializerName);

        EntryHashExchangeSerializer.Request hashExchangeRequestSerializer = new EntryHashExchangeSerializer.Request(currentId++);
        Serializers.register(hashExchangeRequestSerializer, SweepSerializers.entryHashExchangeRequest.serializerName);
        Serializers.register(SweepSerializers.entryHashExchangeRequest.serializedClass, SweepSerializers.entryHashExchangeRequest.serializerName);

        EntryHashExchangeSerializer.Response hashExchangeResponseSerializer = new EntryHashExchangeSerializer.Response(currentId++);
        Serializers.register(hashExchangeResponseSerializer, SweepSerializers.entryHashExchangeResponse.serializerName);
        Serializers.register(SweepSerializers.entryHashExchangeResponse.serializedClass, SweepSerializers.entryHashExchangeResponse.serializerName);

        EntryHashSerializer hashSerializer = new EntryHashSerializer(currentId++);
        Serializers.register(hashSerializer, SweepSerializers.entryHash.serializerName);
        Serializers.register(SweepSerializers.entryHash.serializedClass, SweepSerializers.entryHash.serializerName);


        EntryExchangeSerializer.Request entryExchangeRequestSerializer = new EntryExchangeSerializer.Request(currentId++);
        Serializers.register(entryExchangeRequestSerializer, SweepSerializers.entryExchangeRequest.serializerName);
        Serializers.register(SweepSerializers.entryExchangeRequest.serializedClass, SweepSerializers.entryExchangeRequest.serializerName);


        EntryExchangeSerializer.Response entryExchangeResponseSerializer = new EntryExchangeSerializer.Response(currentId++);
        Serializers.register(entryExchangeResponseSerializer, SweepSerializers.entryExchangeResponse.serializerName);
        Serializers.register(SweepSerializers.entryExchangeResponse.serializedClass, SweepSerializers.entryExchangeResponse.serializerName);


        OverlayIdSerializer overlayIdSerializer = new OverlayIdSerializer(currentId++);
        Serializers.register(overlayIdSerializer, SweepSerializers.overlayId.serializerName);
        Serializers.register(SweepSerializers.overlayId.serializedClass, SweepSerializers.overlayId.serializerName);

        ControlPullSerializer.Request controlPullRequestSerializer = new ControlPullSerializer.Request(currentId++);
        Serializers.register(controlPullRequestSerializer, SweepSerializers.controlPullRequest.serializerName);
        Serializers.register(SweepSerializers.controlPullRequest.serializedClass, SweepSerializers.controlPullRequest.serializerName);

        ControlPullSerializer.Response controlPullResponseSerializer = new ControlPullSerializer.Response(currentId++);
        Serializers.register(controlPullResponseSerializer, SweepSerializers.controlPullResponse.serializerName);
        Serializers.register(SweepSerializers.controlPullResponse.serializedClass, SweepSerializers.controlPullResponse.serializerName);


        BaseLeaderUnitSerializer baseLeaderUnitSerializer = new BaseLeaderUnitSerializer(currentId++);
        Serializers.register(baseLeaderUnitSerializer, SweepSerializers.baseLeaderUnit.serializerName);
        Serializers.register(SweepSerializers.baseLeaderUnit.serializedClass, SweepSerializers.baseLeaderUnit.serializerName);

        LeaderUnitUpdateSerializer luUpdateSerializer = new LeaderUnitUpdateSerializer(currentId++);
        Serializers.register(luUpdateSerializer, SweepSerializers.leaderUnitUpdate.serializerName);
        Serializers.register(SweepSerializers.leaderUnitUpdate.serializedClass, SweepSerializers.leaderUnitUpdate.serializerName);
        
        ShardPrepareSerializer.Request shardPrepareRequestSerializer = new ShardPrepareSerializer.Request(currentId++);
        Serializers.register(shardPrepareRequestSerializer, SweepSerializers.shardingPrepareRequest.serializerName);
        Serializers.register(SweepSerializers.shardingPrepareRequest.serializedClass, SweepSerializers.shardingPrepareRequest.serializerName);

        ShardPrepareSerializer.Response shardPrepareResponseSerializer = new ShardPrepareSerializer.Response(currentId++);
        Serializers.register(shardPrepareResponseSerializer, SweepSerializers.shardingPrepareResponse.serializerName);
        Serializers.register(SweepSerializers.shardingPrepareResponse.serializedClass, SweepSerializers.shardingPrepareResponse.serializerName);


        ShardCommitSerializer.Request shardCommitRequestSerializer = new ShardCommitSerializer.Request(currentId++);
        Serializers.register(shardCommitRequestSerializer, SweepSerializers.shardingCommitRequest.serializerName);
        Serializers.register(SweepSerializers.shardingCommitRequest.serializedClass, SweepSerializers.shardingCommitRequest.serializerName);
        
        ShardCommitSerializer.Response shardCommitResponseSerializer = new ShardCommitSerializer.Response(currentId++);
        Serializers.register(shardCommitResponseSerializer, SweepSerializers.shardingCommitResponse.serializerName);
        Serializers.register(SweepSerializers.shardingCommitResponse.serializedClass, SweepSerializers.shardingCommitResponse.serializerName);
        
        
        LandingEntryAddSerializer.Request landingEntryAddRequestSerializer = new LandingEntryAddSerializer.Request(currentId++);
        Serializers.register(landingEntryAddRequestSerializer, SweepSerializers.landingEntryAddPrepareRequest.serializerName);
        Serializers.register(SweepSerializers.landingEntryAddPrepareRequest.serializedClass, SweepSerializers.landingEntryAddPrepareRequest.serializerName);

        LandingEntryAddSerializer.Response landingEntryAddResponseSerializer = new LandingEntryAddSerializer.Response(currentId++);
        Serializers.register(landingEntryAddResponseSerializer, SweepSerializers.landingEntryAddPrepareResponse.serializerName);
        Serializers.register(SweepSerializers.landingEntryAddPrepareResponse.serializedClass, SweepSerializers.landingEntryAddPrepareResponse.serializerName);

        ApplicationEntryAddSerializer.Request applicationEntryAddPrepareRequestSerializer = new ApplicationEntryAddSerializer.Request(currentId++);
        Serializers.register(applicationEntryAddPrepareRequestSerializer, SweepSerializers.applicationEntryAddPrepareRequest.serializerName);
        Serializers.register(SweepSerializers.applicationEntryAddPrepareRequest.serializedClass, SweepSerializers.applicationEntryAddPrepareRequest.serializerName);

        ApplicationEntryAddSerializer.Response applicationEntryAddPrepareResponseSerializer = new ApplicationEntryAddSerializer.Response(currentId++);
        Serializers.register(applicationEntryAddPrepareResponseSerializer, SweepSerializers.applicationEntryAddPrepareResponse.serializerName);
        Serializers.register(SweepSerializers.applicationEntryAddPrepareResponse.serializedClass, SweepSerializers.applicationEntryAddPrepareResponse.serializerName);
        
        
        EntryAddCommitSerializer entryAddCommitSerializer = new EntryAddCommitSerializer(currentId++);
        Serializers.register(entryAddCommitSerializer, SweepSerializers.entryAddCommitRequest.serializerName);
        Serializers.register(SweepSerializers.entryAddCommitRequest.serializedClass, SweepSerializers.entryAddCommitRequest.serializerName);
        
        return currentId;
    }

}
