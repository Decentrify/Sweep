package se.sics.ms.net;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.data.*;
import se.sics.ms.types.*;
import se.sics.ms.util.EntryScorePair;
import se.sics.ms.util.IdScorePair;
import se.sics.ms.util.PartitionHelper;
import se.sics.ms.util.PartitioningType;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.*;
import java.util.*;
import se.sics.ktoolbox.election.util.PublicKeySerializer;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicAddress;
import se.sics.ktoolbox.util.network.nat.NatAwareAddressImpl;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;

/**
 * Main Test Class for the Serializers used in the application.
 *
 * Created by babbar on 2015-04-22.
 */
public class SerializerTest {

    private static Logger logger = LoggerFactory.getLogger(SerializerTest.class);
    private static InetAddress localHost;
    private static Random random;
    static {
        try {
            localHost = InetAddress.getByName("localhost");
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static ByteBuf originalBuf, copiedBuf;
    private static KAddress selfAddress, destinationAddress;
    private static PeerDescriptor selfDescriptor;
    private static PeerDescriptor otherDescriptor;
    private static PublicKey publicKey;
    private static PrivateKey privateKey;
    private static IndexEntry randomIndexEntry;
    private static IndexEntry specialIndexEntry;

    @BeforeClass
    public static void oneTimeSetup() throws NoSuchAlgorithmException {

        logger.info("Executing the one time setup.");
        int seed = 100;
        
        systemSetup();

        selfAddress = NatAwareAddressImpl.open(new BasicAddress(localHost, 54321, new IntIdentifier(1)));
        destinationAddress = NatAwareAddressImpl.open(new BasicAddress(localHost, 54322, new IntIdentifier(2)));
        selfDescriptor = new PeerDescriptor(selfAddress, 1);
        otherDescriptor = new PeerDescriptor(destinationAddress, 0);

        originalBuf = Unpooled.buffer();
        copiedBuf = Unpooled.buffer();
        randomIndexEntry = new IndexEntry("globalId", 0, "url", "Avengers: Age of Ultron", 10189, new Date(),   "English", MsConfig.Categories.Video, "Description", "Hash", publicKey);
        specialIndexEntry = new IndexEntry(" ", 0, "url", "Avengers: Age of Ultron", 10189, new Date(),   "English", MsConfig.Categories.Video, "Description", "Hash", publicKey);

        random = new Random(seed);
        generateKeys();
    }
    
    private static void systemSetup() {
        int currentId = 128;
        currentId = BasicSerializerSetup.registerBasicSerializers(currentId);
        currentId = SweepSerializerSetup.registerSerializers(currentId);
        
        PublicKeySerializer pkSerializer =  new PublicKeySerializer(currentId++);
        Serializers.register(pkSerializer, "publicKeySerializer");
        Serializers.register(PublicKey.class, "publicKeySerializer");

        SweepSerializerSetup.checkSetup();
    }

    @Before
    public void beforeTest() {
        logger.info(" Cleaning the byte buffers. ");

        originalBuf.clear();
        copiedBuf.clear();
    }

    @After
    public void afterTest() {
        logger.info(" Finished a Test. ");
    }


    @Test
    public void descriptorSerializationTest(){

        logger.info("Search Descriptor Serialization Test. ");

        Serializer sdSerializer = Serializers.lookupSerializer(PeerDescriptor.class);
        sdSerializer.toBinary(selfDescriptor, originalBuf);
        copiedBuf = Unpooled.wrappedBuffer(originalBuf.array());

        PeerDescriptor cpyDescriptor = (PeerDescriptor) sdSerializer.fromBinary(copiedBuf, Optional.absent());
        Assert.assertEquals(" SearchDescriptor Equality Check ", selfDescriptor, cpyDescriptor);
    }


    @Test
    public void publicKeySerializerTest(){

        logger.info("Public Key Serializer Test");

        Serializer pubKeySerializer = Serializers.lookupSerializer(PublicKey.class);
        PublicKey copiedPubKey = (PublicKey) addObjectAndCreateCopiedObject(pubKeySerializer, originalBuf, publicKey, copiedBuf);

        Assert.assertEquals("Public Key Comparison", publicKey, copiedPubKey);
    }


    @Test
    public void searchPatternSerializationTest(){
        logger.info("Search pattern serialization test");

        SearchPattern originalPattern = new SearchPattern("Avengers: Age of Ultron", 1012, 1021, new Date(), new Date(), "English", MsConfig.Categories.Books, "Pattern 1");
        Serializer spSerializer = Serializers.lookupSerializer(SearchPattern.class);

        SearchPattern copiedPattern = (SearchPattern) addObjectAndCreateCopiedObject(spSerializer, originalBuf, originalPattern, copiedBuf);
        Assert.assertEquals("Search Pattern Serialization", originalPattern, copiedPattern);
    }


    @Test
    public void indexEntrySerializationTest(){
        logger.info("Index Entry Serialization Test");

        IndexEntry originalEntry = getSpecialIndexEntry();
        Serializer serializer = Serializers.lookupSerializer(IndexEntry.class);

        IndexEntry copiedEntry = (IndexEntry) addObjectAndCreateCopiedObject(serializer, originalBuf, originalEntry, copiedBuf);
        Assert.assertEquals("Index Entry Serialization", originalEntry, copiedEntry);
    }


    @Test
    public void partitionInfoSerializationTest(){
        logger.info("Partition Info Serialization Test");

        PartitionHelper.PartitionInfo originalPartitionInfo = new PartitionHelper.PartitionInfo(10, UUID.randomUUID(), PartitioningType.NEVER_BEFORE, "Hash", publicKey);
        Serializer piSerializer = Serializers.lookupSerializer(PartitionHelper.PartitionInfo.class);

        PartitionHelper.PartitionInfo copiedPartitionInfo = (PartitionHelper.PartitionInfo) addObjectAndCreateCopiedObject(piSerializer, originalBuf, originalPartitionInfo, copiedBuf);
        Assert.assertEquals("Partition Info Serialization", originalPartitionInfo, copiedPartitionInfo);
    }


    @Test
    public void idInfoSerialization(){
        logger.info("ID Info Serialization Test");

        Id originalIdInfo = new Id(1, publicKey);
        Serializer idSerializer = Serializers.lookupSerializer(Id.class);

        Id copiedIdInfo = (Id) addObjectAndCreateCopiedObject(idSerializer, originalBuf, originalIdInfo, copiedBuf);
        Assert.assertEquals("Id Info Serialization", originalIdInfo, copiedIdInfo);
    }

    @Test
    public void searchInfoRequestSerialization(){
        logger.info("Search Info Request Serialization Test");

        SearchPattern originalSearchPattern = new SearchPattern("Avengers: Age of Ultron", 1012, 1021, new Date(), new Date(), "English", MsConfig.Categories.Books, "Pattern 1");
        SearchInfo.Request originalSIRequest = new SearchInfo.Request(UUID.randomUUID(), 1, originalSearchPattern);
        Serializer siSerializer = Serializers.lookupSerializer(SearchInfo.Request.class);

        SearchInfo.Request copiedSIRequest = (SearchInfo.Request) addObjectAndCreateCopiedObject(siSerializer, originalBuf, originalSIRequest, copiedBuf);
        Assert.assertEquals("Search Info Serialization", originalSIRequest, copiedSIRequest);
    }

    @Test
    public void searchInfoResponseSerialization(){
        logger.info("Search Info Response Serialization Test");

        Collection<IndexEntry> entryCollection = getIndexEntryCollection(3);
        SearchInfo.Response originalSIResponse = new SearchInfo.Response(UUID.randomUUID(), entryCollection, 1, 2, 0);
        Serializer sirSerializer = Serializers.lookupSerializer(SearchInfo.Response.class);

        SearchInfo.Response copiedSIResponse = (SearchInfo.Response) addObjectAndCreateCopiedObject(sirSerializer, originalBuf, originalSIResponse, copiedBuf);
        Assert.assertEquals("Search Info Response Serialization", originalSIResponse, copiedSIResponse);
    }

    @Test
    public void landingEntryTest() {
        logger.info("Landing Entry Test");

        IndexEntry entry = IndexEntry.DEFAULT_ENTRY;
        Serializer addEntryRequestSerializer = Serializers.lookupSerializer(IndexEntry.class);

        IndexEntry copiedEntry = (IndexEntry) addObjectAndCreateCopiedObject(addEntryRequestSerializer, originalBuf, entry, copiedBuf);
        Assert.assertEquals("Add Index Entry Request Serialization", entry, copiedEntry);
    }

    @Test
    public void addIndexEntryRequestTest() {
        logger.info("Add Index Entry Request Test");

        IndexEntry entry = getSpecialIndexEntry();
        AddIndexEntry.Request originalRequest = new AddIndexEntry.Request(UUID.randomUUID(), entry);
        Serializer addEntryRequestSerializer = Serializers.lookupSerializer(AddIndexEntry.Request.class);

        AddIndexEntry.Request copiedRequest = (AddIndexEntry.Request) addObjectAndCreateCopiedObject(addEntryRequestSerializer, originalBuf, originalRequest, copiedBuf);
        Assert.assertEquals("Add Index Entry Request Serialization", originalRequest, copiedRequest);
    }

    @Test
    public void addIndexEntryResponseTest(){
        logger.info("Add Index Entry Response Test");

        AddIndexEntry.Response originalResponse = new AddIndexEntry.Response(UUID.randomUUID());
        Serializer addEntryResponseSerializer = Serializers.lookupSerializer(AddIndexEntry.Response.class);

        AddIndexEntry.Response copiedResponse = (AddIndexEntry.Response) addObjectAndCreateCopiedObject(addEntryResponseSerializer, originalBuf, originalResponse, copiedBuf);
        Assert.assertEquals("Add Index Entry Response Serialization", originalResponse, copiedResponse);
    }

    @Test
    public void addIndexPromiseRequestTest(){
        logger.info("Add Index Promise Request Serialization Test");

        ReplicationPrepareCommit.Request originalRequest = new ReplicationPrepareCommit.Request(getRandomIndexEntry(), UUID.randomUUID());
        Serializer promiseSerializer = Serializers.lookupSerializer(ReplicationPrepareCommit.Request.class);

        ReplicationPrepareCommit.Request copiedRequest = (ReplicationPrepareCommit.Request) addObjectAndCreateCopiedObject(promiseSerializer, originalBuf, originalRequest, copiedBuf);
        Assert.assertEquals("Add Entry Promise Serialization", originalRequest, copiedRequest);
    }


    @Test
    public void addIndexPromiseResponseTest(){
        logger.info("Add Index Promise Response Serialization Test");

        ReplicationPrepareCommit.Response originalResponse = new ReplicationPrepareCommit.Response(UUID.randomUUID(), 10);
        Serializer promiseSerializer = Serializers.lookupSerializer(ReplicationPrepareCommit.Response.class);

        ReplicationPrepareCommit.Response copiedResponse = (ReplicationPrepareCommit.Response) addObjectAndCreateCopiedObject(promiseSerializer, originalBuf, originalResponse, copiedBuf);
        Assert.assertEquals("Add Entry Promise Serialization", originalResponse, copiedResponse);
    }



    @Test
    public void addIndexCommitRequestTest(){
        logger.info("Add Index Commit Request Serialization Test");

        ReplicationCommit.Request originalRequest = new ReplicationCommit.Request(UUID.randomUUID(), 12, "signature");
        Serializer commitSerializer = Serializers.lookupSerializer(ReplicationCommit.Request.class);

        ReplicationCommit.Request copiedRequest = (ReplicationCommit.Request) addObjectAndCreateCopiedObject(commitSerializer, originalBuf, originalRequest, copiedBuf);
        Assert.assertEquals("Add Entry Commit Request Serialization", originalRequest, copiedRequest);
    }

    @Test
    public void controlInfoRequestTest(){
        logger.info("Control Information Request Serialization Test");

        ControlInformation.Request originalRequest = new ControlInformation.Request(UUID.randomUUID(), new OverlayId(0));
        Serializer requestSerializer = Serializers.lookupSerializer(ControlInformation.Request.class);

        ControlInformation.Request copiedRequest = (ControlInformation.Request) addObjectAndCreateCopiedObject(requestSerializer, originalBuf, originalRequest, copiedBuf);
        Assert.assertEquals("Control Information Request Serialization", originalRequest, copiedRequest);
    }


    @Test
    public void controlInfoResponseTest(){
        logger.info("Control Information Request Serialization Test");

        byte[] testByteArray = new byte[1];
        testByteArray[0] = new Integer(1).byteValue();

        ControlInformation.Response originalResponse = new ControlInformation.Response(UUID.randomUUID(), testByteArray);
        Serializer responseSerializer = Serializers.lookupSerializer(ControlInformation.Response.class);

        ControlInformation.Response copiedResponse = (ControlInformation.Response) addObjectAndCreateCopiedObject(responseSerializer, originalBuf, originalResponse, copiedBuf);
        Assert.assertEquals("Control Information Request Serialization", originalResponse, copiedResponse);
    }


    @Test
    public void partitionPrepareRequestTest(){
        logger.info("Partition Prepare Request Serialization Test");

        PartitionPrepare.Request originalRequest = new PartitionPrepare.Request(UUID.randomUUID(), getPartitionInfo(), new OverlayId(0));
        Serializer partitionPrepareSerializer = Serializers.lookupSerializer(PartitionPrepare.Request.class);

        PartitionPrepare.Request copiedRequest = (PartitionPrepare.Request) addObjectAndCreateCopiedObject(partitionPrepareSerializer, originalBuf, originalRequest, copiedBuf);
        Assert.assertEquals("Partition Prepare Request Serialization", originalRequest, copiedRequest);

    }


    @Test
    public void partitionPrepareResponseTest(){
        logger.info("Partition Prepare Request Serialization Test");

        PartitionPrepare.Response originalResponse = new PartitionPrepare.Response(UUID.randomUUID(), UUID.randomUUID());
        Serializer partitionPrepareSerializer = Serializers.lookupSerializer(PartitionPrepare.Response.class);

        PartitionPrepare.Response copiedResponse = (PartitionPrepare.Response) addObjectAndCreateCopiedObject(partitionPrepareSerializer, originalBuf, originalResponse, copiedBuf);
        Assert.assertEquals("Partition Prepare Request Serialization", originalResponse, copiedResponse);

    }


    @Test
    public void partitionCommitRequestTest(){
        logger.info("Partition Commit Request Test ");

        PartitionCommit.Request originalRequest = new PartitionCommit.Request(UUID.randomUUID(), UUID.randomUUID());
        Serializer partitionCommitSerializer = Serializers.lookupSerializer(PartitionCommit.Request.class);

        PartitionCommit.Request copiedRequest = (PartitionCommit.Request)addObjectAndCreateCopiedObject(partitionCommitSerializer, originalBuf, originalRequest, copiedBuf);
        Assert.assertEquals("Partition Commit Request Serializer", originalRequest, copiedRequest);
    }


    @Test
    public void partitionCommitResponseTest(){
        logger.info("Partition Commit Response Test.");

        PartitionCommit.Response originalResponse = new PartitionCommit.Response(UUID.randomUUID(), UUID.randomUUID());
        Serializer partitionCommitSerializer = Serializers.lookupSerializer(PartitionCommit.Response.class);

        PartitionCommit.Response copiedResponse = (PartitionCommit.Response)addObjectAndCreateCopiedObject(partitionCommitSerializer, originalBuf, originalResponse, copiedBuf);
        Assert.assertEquals("Partition Commit Request Serializer", originalResponse, copiedResponse);

    }

    @Test
    public void delayedPartitioningRequestTest(){
        logger.info("Delayed Partitioning Request Test");

        List<UUID> uuidList = new ArrayList<UUID>();
        uuidList.add(UUID.randomUUID());

        DelayedPartitioning.Request originalRequest = new DelayedPartitioning.Request(UUID.randomUUID(), uuidList);
        Serializer delayedPartitioningSerializer = Serializers.lookupSerializer(DelayedPartitioning.Request.class);

        DelayedPartitioning.Request copiedRequest = (DelayedPartitioning.Request)addObjectAndCreateCopiedObject(delayedPartitioningSerializer, originalBuf, originalRequest, copiedBuf);
        Assert.assertEquals("Delayed Partitioning Request Serialization", originalRequest, copiedRequest);

    }


    @Test
    public void delayedPartitioningResponseTest(){
        logger.info("Delayed Partitioning Response Test");

        LinkedList<PartitionHelper.PartitionInfo> partitionInfos = new LinkedList<PartitionHelper.PartitionInfo>();
        partitionInfos.add(getPartitionInfo());
        partitionInfos.add(getPartitionInfo());

        DelayedPartitioning.Response originalResponse = new DelayedPartitioning.Response(UUID.randomUUID(), partitionInfos);
        Serializer delayedPartitioningSerializer = Serializers.lookupSerializer(DelayedPartitioning.Response.class);

        DelayedPartitioning.Response copiedResponse = (DelayedPartitioning.Response)addObjectAndCreateCopiedObject(delayedPartitioningSerializer, originalBuf, originalResponse, copiedBuf);
        Assert.assertEquals("Delayed Partitioning Request Serialization", originalResponse, copiedResponse);

    }



    @Test
    public void addIndexExchangeRequestTest(){
        logger.info("Add Index Exchange Request Serialization Test.");

        Collection<Id> ids = new ArrayList<Id>();

        ids.add(new Id(10, publicKey));
        ids.add(new Id(11, publicKey));


        IndexExchange.Request originalExchangeRequest  = new IndexExchange.Request(UUID.randomUUID(), ids, 0);
        Serializer exchangeSerializer = Serializers.lookupSerializer(IndexExchange.Request.class);

        IndexExchange.Request copiedExchangeRequest = (IndexExchange.Request)addObjectAndCreateCopiedObject(exchangeSerializer, originalBuf, originalExchangeRequest, copiedBuf);
        Assert.assertEquals("Add Index Exchange Request Serialization", originalExchangeRequest, copiedExchangeRequest);
    }


    @Test
    public void addIndexExchangeResponseTest(){
        logger.info("Add Index Exchange Response Serialization Test.");

        IndexExchange.Response originalExchangeResponse  = new IndexExchange.Response(UUID.randomUUID(), getIndexEntryCollection(10), 10, 0, 0);
        Serializer exchangeSerializer = Serializers.lookupSerializer(IndexExchange.Response.class);

        IndexExchange.Response copiedExchangeResponse = (IndexExchange.Response)addObjectAndCreateCopiedObject(exchangeSerializer, originalBuf, originalExchangeResponse, copiedBuf);
        Assert.assertEquals("Add Index Exchange Response Serialization", originalExchangeResponse, copiedExchangeResponse);
    }


    @Test
    public void repairRequestTest(){
        logger.info("Repair Request Serialization Test");

        Long[] missingIds = new Long[2];
        missingIds[0] = (long)10;
        missingIds[1] = (long)11;

        Repair.Request originalRequest = new Repair.Request(UUID.randomUUID(), missingIds);
        Serializer repairSerializer = Serializers.lookupSerializer(Repair.Request.class);

        Repair.Request copiedRequest = (Repair.Request)addObjectAndCreateCopiedObject(repairSerializer, originalBuf, originalRequest, copiedBuf);
        Assert.assertEquals("Repair Request Serialization", originalRequest, copiedRequest);
    }


    @Test
    public void repairResponseTest(){
        logger.info("Repair Request Serialization Test");

        Repair.Response originalResponse = new Repair.Response(UUID.randomUUID(), getIndexEntryCollection(10));
        Serializer repairSerializer = Serializers.lookupSerializer(Repair.Response.class);

        Repair.Response copiedResponse = (Repair.Response)addObjectAndCreateCopiedObject(repairSerializer, originalBuf, originalResponse, copiedBuf);
        Assert.assertEquals("Repair Request Serialization", originalResponse, copiedResponse);
    }


    @Test
    public void leaderLookUpRequestTest(){
        logger.info("Leader Lookup Request Serialization Test");

        LeaderLookup.Request originalRequest = new LeaderLookup.Request(UUID.randomUUID());
        Serializer lookupSerializer = Serializers.lookupSerializer(LeaderLookup.Request.class);

        LeaderLookup.Request copiedRequest = (LeaderLookup.Request)addObjectAndCreateCopiedObject(lookupSerializer, originalBuf, originalRequest, copiedBuf);
        Assert.assertEquals("Leader lookup request serialization", originalRequest, copiedRequest);

    }

    @Test
    public void leaderLookUpResponseTest(){
        logger.info("Leader Lookup Response Serialization Test");

        List<PeerDescriptor> descList = new ArrayList<PeerDescriptor>();
        descList.add(selfDescriptor);
        descList.add(otherDescriptor);

        LeaderLookup.Response originalResponse = new LeaderLookup.Response(UUID.randomUUID(), true, descList);
        Serializer lookupSerializer = Serializers.lookupSerializer(LeaderLookup.Response.class);

        LeaderLookup.Response copiedRequest = (LeaderLookup.Response)addObjectAndCreateCopiedObject(lookupSerializer, originalBuf, originalResponse, copiedBuf);
        Assert.assertEquals("Leader lookup response serialization", originalResponse, copiedRequest);

    }

    
    @Test
    public void indexEntryTest(){
        logger.info("Index Entry Test");
        
        Serializer serializer = Serializers.lookupSerializer(IndexEntry.class);
        IndexEntry entry = (IndexEntry)addObjectAndCreateCopiedObject(serializer, originalBuf, specialIndexEntry, copiedBuf);

        org.junit.Assert.assertEquals("Index Entry Test", specialIndexEntry, entry);
    }
    
    
    @Test
    public void indexHashRequestTest(){
        logger.info("Index Hash Exchange Request Test");

        IndexHashExchange.Request request = new IndexHashExchange.Request(UUID.randomUUID(), 10, new Long[]{(long)10, (long)11, (long)12},0);
        Serializer serializer = Serializers.lookupSerializer(IndexHashExchange.Request.class);

        IndexHashExchange.Request copiedRequest = (IndexHashExchange.Request) addObjectAndCreateCopiedObject(serializer, originalBuf, request, copiedBuf);
        Assert.assertEquals("Index Hash Exchange Request", request, copiedRequest);
    }



    @Test
    public void indexHashExchangeResponseTest(){
        logger.info("Index Hash Exchange Response Test");

        IndexHashExchange.Response originalResponse = new IndexHashExchange.Response(UUID.randomUUID(), getIndexHashCollection(5), 0);
        Serializer serializer = Serializers.lookupSerializer(IndexHashExchange.Response.class);

        IndexHashExchange.Response copiedResponse = (IndexHashExchange.Response) addObjectAndCreateCopiedObject(serializer, originalBuf, originalResponse, copiedBuf);
        Assert.assertEquals("Index Hash Exchange Request", originalResponse.getIndexHashes().size(), copiedResponse.getIndexHashes().size());
    }


    @Test
    public void partitionHashInfoTest(){
        logger.info("Partition Info Hash Test.");

        PartitionHelper.PartitionInfoHash infoHash = new PartitionHelper.PartitionInfoHash(UUID.randomUUID(), "Abhi");
        Serializer serializer = Serializers.lookupSerializer(PartitionHelper.PartitionInfoHash.class);

        PartitionHelper.PartitionInfoHash copiedInfoHash = (PartitionHelper.PartitionInfoHash)addObjectAndCreateCopiedObject(serializer, originalBuf, infoHash, copiedBuf);
        org.junit.Assert.assertEquals("Partition Hash Info Test", infoHash, copiedInfoHash);
    }
    



    @Test
    public void testEntryId(){
        logger.info("Application Entry Id Test.");

        ApplicationEntry.ApplicationEntryId entryId = testApplicationEntryId();
        Serializer serializer = Serializers.lookupSerializer(ApplicationEntry.ApplicationEntryId.class);

        ApplicationEntry.ApplicationEntryId copiedEntryId = (ApplicationEntry.ApplicationEntryId)addObjectAndCreateCopiedObject(serializer, originalBuf, entryId, copiedBuf);
        org.junit.Assert.assertEquals("Application Entry Id Test", entryId, copiedEntryId);
    }


    @Test
    public void testApplicationEntry() {
        logger.info("Application Entry Test");

        ApplicationEntry entry  = new ApplicationEntry(getApplicationEntryId(0, 100, 1));
        Serializer serializer = Serializers.lookupSerializer(ApplicationEntry.class);
        ApplicationEntry copiedEntry = (ApplicationEntry)addObjectAndCreateCopiedObject(serializer, originalBuf, entry, copiedBuf);

        org.junit.Assert.assertEquals("Application Entry Test", entry, copiedEntry);
    }



    @Test
    public void leaderPullEntryRequestTest(){

        logger.info("Leader Pull Entry Test");

        UUID pullRoundId = UUID.randomUUID();
        LeaderPullEntry.Request request = new LeaderPullEntry.Request(pullRoundId, getApplicationEntryId(1,100,0));
        Serializer serializer = Serializers.lookupSerializer(LeaderPullEntry.Request.class);
        LeaderPullEntry.Request copiedRequest = (LeaderPullEntry.Request)addObjectAndCreateCopiedObject(serializer, originalBuf, request, copiedBuf);

        org.junit.Assert.assertEquals("Leader Pull Entry Request Test", request, copiedRequest);
    }


    @Test
    public void leaderPullEntryResponseTest(){

        logger.info("Leader Pull Entry Response Test");
        Collection<ApplicationEntry> entries = getApplicationEntryCollection(2);
        UUID pullRoundId = UUID.randomUUID();
        int overlayId = 0;

        LeaderPullEntry.Response response = new LeaderPullEntry.Response(pullRoundId, entries, overlayId);
        Serializer serializer = Serializers.lookupSerializer(LeaderPullEntry.Response.class);
        LeaderPullEntry.Response copiedResponse = (LeaderPullEntry.Response)addObjectAndCreateCopiedObject(serializer, originalBuf, response, copiedBuf);

        org.junit.Assert.assertEquals("Leader Pull Entry Response Test", response, copiedResponse);
    }


    @Test
    public void entryHashExchangeRequestTest(){

        logger.info("Entry Hash Exchange Request Test");

        UUID exchangeRound = UUID.randomUUID();
        ApplicationEntry.ApplicationEntryId entryId = getApplicationEntryId(0, Integer.MIN_VALUE, 0);
        EntryHashExchange.Request request = new EntryHashExchange.Request(exchangeRound, entryId);

        Serializer serializer = Serializers.lookupSerializer(EntryHashExchange.Request.class);
        EntryHashExchange.Request copiedRequest = (EntryHashExchange.Request)addObjectAndCreateCopiedObject(serializer, originalBuf, request, copiedBuf);

        org.junit.Assert.assertEquals("Entry Hash Exchange Request Test", request, copiedRequest);
    }


    @Test
    public void entryHashExchangeResponseTest(){

        logger.info("Entry Hash Exchange Response Test");

        List<EntryHash> entryHashCollection = getEntryHashCollection(3);
        UUID exchangeRound = UUID.randomUUID();

        EntryHashExchange.Response response = new EntryHashExchange.Response(exchangeRound, entryHashCollection);
        Serializer serializer = Serializers.lookupSerializer(EntryHashExchange.Response.class);
        EntryHashExchange.Response copiedResponse = (EntryHashExchange.Response)addObjectAndCreateCopiedObject(serializer, originalBuf, response, copiedBuf);
        org.junit.Assert.assertEquals("Entry Hash Exchange Response Test", response, copiedResponse);
    }

    @Test
    public void entryExchangeRequestTest(){

        logger.info("Entry Exchange Response Test");
        Collection<ApplicationEntry.ApplicationEntryId> entryIds = entryIdCollection(3);
        UUID exchangeId = UUID.randomUUID();

        EntryExchange.Request request = new EntryExchange.Request(exchangeId, entryIds);
        Serializer serializer = Serializers.lookupSerializer(EntryExchange.Request.class);

        EntryExchange.Request copiedRequest = (EntryExchange.Request)addObjectAndCreateCopiedObject(serializer, originalBuf, request, copiedBuf);
        org.junit.Assert.assertEquals("Entry Exchange Response Test", request, copiedRequest);
    }


    @Test
    public void entryExchangeResponseTest(){

        logger.info("Entry Exchange Request Test");
        Collection<ApplicationEntry> entries= getApplicationEntryCollection(3);
        UUID exchangeId = UUID.randomUUID();

        EntryExchange.Response response = new EntryExchange.Response(exchangeId, entries, 0);
        Serializer serializer = Serializers.lookupSerializer(EntryExchange.Response.class);

        EntryExchange.Response copiedResponse = (EntryExchange.Response)addObjectAndCreateCopiedObject(serializer, originalBuf, response, copiedBuf);
        org.junit.Assert.assertEquals("Entry Exchange Request Test", response, copiedResponse);
    }





    @Test
    public void controlPullNullRequestTest() {

        logger.info("Control Pull Null Unit Request Test.");

        UUID pullRound = UUID.randomUUID();
        ControlPull.Request request = new ControlPull.Request (pullRound, new OverlayId(0), null);
        Serializer serializer = Serializers.lookupSerializer(ControlPull.Request.class);
        ControlPull.Request copiedRequest = (ControlPull.Request)addObjectAndCreateCopiedObject(serializer, originalBuf, request, copiedBuf);
        org.junit.Assert.assertEquals("Control Pull Null Unit Request Test", request, copiedRequest);

    }

    @Test
    public void controlPullRequestTest(){

        logger.info("Control Pull Request Test");
        UUID pullRound = UUID.randomUUID();
        OverlayId overlayId = new OverlayId(0);
        BaseLeaderUnit unit = new BaseLeaderUnit(0l, 100, 10l, LeaderUnit.LUStatus.ONGOING);
        Serializer serializer = Serializers.lookupSerializer(ControlPull.Request.class);
        ControlPull.Request request = new ControlPull.Request(pullRound, overlayId, unit);
        ControlPull.Request copiedRequest = (ControlPull.Request)addObjectAndCreateCopiedObject(serializer, originalBuf, request, copiedBuf);
        org.junit.Assert.assertEquals("Control Pull Request Test", request, copiedRequest);
    }



    @Test
    public void controlPullNullResponseTest() {

        logger.info("Control Pull Null Response Test");

        UUID pullRound = UUID.randomUUID();
        List<LeaderUnit> leaderUnits = getLeaderUnitCollection(3);
        int overlayId = 0;

        Serializer serializer= Serializers.lookupSerializer(ControlPull.Response.class);
        ControlPull.Response response = new ControlPull.Response(pullRound, null, null, leaderUnits, overlayId);
        ControlPull.Response copiedResponse = (ControlPull.Response)addObjectAndCreateCopiedObject(serializer, originalBuf, response, copiedBuf);

        org.junit.Assert.assertEquals("Control Pull Response Test", response, copiedResponse);
    }


    @Test
    public void controlPullResponseTest() {

        logger.info("Control Pull Response Test");

        UUID pullRound = UUID.randomUUID();
        List<LeaderUnit> leaderUnits = getLeaderUnitCollection(3);
        int overlayId = 0;

        Serializer serializer= Serializers.lookupSerializer(ControlPull.Response.class);
        ControlPull.Response response = new ControlPull.Response(pullRound, selfAddress, publicKey, leaderUnits, overlayId);
        ControlPull.Response copiedResponse = (ControlPull.Response)addObjectAndCreateCopiedObject(serializer, originalBuf, response, copiedBuf);

        org.junit.Assert.assertEquals("Control Pull Response Test", response, copiedResponse);
    }



    @Test
    public void shardPrepareRequestTest(){
        
        logger.info("Sharding Prepare Request Test");
        
        UUID shardRoundId = UUID.randomUUID();
        OverlayId id = new OverlayId(0);
        LeaderUnitUpdate unitUpdate = new LeaderUnitUpdate(null, new BaseLeaderUnit(1, 100));
        ShardingPrepare.Request request = new ShardingPrepare.Request(shardRoundId, unitUpdate, id);
        
        Serializer serializer = Serializers.lookupSerializer(ShardingPrepare.Request.class);
        ShardingPrepare.Request copiedRequest = (ShardingPrepare.Request)addObjectAndCreateCopiedObject(serializer, originalBuf, request, copiedBuf);
        
        org.junit.Assert.assertEquals("Sharding Prepare Request Test", request, copiedRequest);
    }



    @Test
    public void shardingPrepareResponseTest(){

        logger.info("Sharding Prepare Response Test");

        UUID shardRoundId = UUID.randomUUID();
        ShardingPrepare.Response response = new ShardingPrepare.Response(shardRoundId);

        Serializer serializer = Serializers.lookupSerializer(ShardingPrepare.Response.class);
        ShardingPrepare.Response copiedResponse = (ShardingPrepare.Response)addObjectAndCreateCopiedObject(serializer, originalBuf, response, copiedBuf);

        org.junit.Assert.assertEquals("Sharding Prepare Response Test", response, copiedResponse);
    }

    
    
    @Test
    public void shardCommitRequestTest(){
        
        logger.info("Shard Commit Request Test");
        
        UUID shardCommitRoundId = UUID.randomUUID();
        ShardingCommit.Request request = new ShardingCommit.Request(shardCommitRoundId);
        
        Serializer serializer = Serializers.lookupSerializer(ShardingCommit.Request.class);
        ShardingCommit.Request copiedRequest = (ShardingCommit.Request)addObjectAndCreateCopiedObject(serializer, originalBuf, request, copiedBuf);

        org.junit.Assert.assertEquals("Sharding Commit Request Test", request, copiedRequest);
    }
    
    
    @Test
    public void shardCommitResponseTest(){
        
        logger.info("Shard Commit Response Test");
        
        UUID shardCommitRoundId = UUID.randomUUID();
        ShardingCommit.Response response = new ShardingCommit.Response(shardCommitRoundId);
        
        
        Serializer serializer = Serializers.lookupSerializer(ShardingCommit.Response.class);
        ShardingCommit.Response copiedResponse = (ShardingCommit.Response)addObjectAndCreateCopiedObject(serializer, originalBuf, response, copiedBuf);

        org.junit.Assert.assertEquals("Sharding Commit Response Test", response, copiedResponse);
    }

    
    
    
    @Test
    public void applicationEntryAddPrepareTest(){
        
        logger.info("Application Entry Add Prepare Request Test");
        
        ApplicationEntry applicationEntry = getTestApplicationEntry();
        UUID entryAddRoundId = UUID.randomUUID();
        
        ApplicationEntryAddPrepare.Request request = new ApplicationEntryAddPrepare.Request(entryAddRoundId, applicationEntry);
        Serializer serializer = Serializers.lookupSerializer(ApplicationEntryAddPrepare.Request.class);
        
        ApplicationEntryAddPrepare.Request copiedRequest = (ApplicationEntryAddPrepare.Request)addObjectAndCreateCopiedObject(serializer, originalBuf, request, copiedBuf);
        org.junit.Assert.assertEquals("Application Entry Add Prepare Request Test", request, copiedRequest);
    }
    
    
    
    @Test
    public void landingEntryAddPrepareTest(){
        
        logger.info("Landing Entry Add Prepare Request Test");

        ApplicationEntry.ApplicationEntryId entryId =  getApplicationEntryId(0, 100, 0);
        ApplicationEntry applicationEntry = new ApplicationEntry(entryId);
        UUID entryAdditionRoundId = UUID.randomUUID();
        
        LandingEntryAddPrepare.Request request = new LandingEntryAddPrepare.Request(entryAdditionRoundId, applicationEntry, null);
        Serializer serializer = Serializers.lookupSerializer(LandingEntryAddPrepare.Request.class);
        
        LandingEntryAddPrepare.Request copiedRequest = (LandingEntryAddPrepare.Request)addObjectAndCreateCopiedObject(serializer, originalBuf, request, copiedBuf);
        org.junit.Assert.assertEquals("Landing Entry Add Prepare Request Test", request, copiedRequest);
        
    }
    
    
    @Test
    public void applicationEntryAddPrepareResponseTest(){
        
        logger.info("Application Entry Add Prepare Response Test");

        ApplicationEntry.ApplicationEntryId entryId = new ApplicationEntry.ApplicationEntryId(0, 100, 0);
        UUID entryAdditionRoundId = UUID.randomUUID();
        
        ApplicationEntryAddPrepare.Response response = new ApplicationEntryAddPrepare.Response(entryAdditionRoundId, entryId);
        Serializer serializer = Serializers.lookupSerializer(ApplicationEntryAddPrepare.Response.class);
        
        ApplicationEntryAddPrepare.Response copiedResponse = (ApplicationEntryAddPrepare.Response)addObjectAndCreateCopiedObject(serializer, originalBuf, response, copiedBuf);
        org.junit.Assert.assertEquals("Application Entry Add Prepare Response Test", response, copiedResponse);
    }
    
    
    @Test
    public void landingEntryAddPrepareResponseTest(){
        
        logger.info("Landing Entry Add Prepare Response Test");

        ApplicationEntry.ApplicationEntryId entryId = new ApplicationEntry.ApplicationEntryId(0, 100, 0);
        UUID entryAdditionRoundId = UUID.randomUUID();

        LandingEntryAddPrepare.Response response = new LandingEntryAddPrepare.Response(entryAdditionRoundId, entryId);
        Serializer serializer = Serializers.lookupSerializer(LandingEntryAddPrepare.Response.class);

        LandingEntryAddPrepare.Response copiedResponse = (LandingEntryAddPrepare.Response)addObjectAndCreateCopiedObject(serializer, originalBuf, response, copiedBuf);
        org.junit.Assert.assertEquals("Landing Entry Add Prepare Response Test", response, copiedResponse);
    }
    
    
    @Test
    public void entryAddCommitTest(){
        
        logger.info("Entry Add Commit Test");

        ApplicationEntry.ApplicationEntryId entryId= getApplicationEntryId(0, 100, 0);
        UUID commitRoundId = UUID.randomUUID();
        String signature = "signature";
        
        EntryAddCommit.Request request = new EntryAddCommit.Request(commitRoundId, entryId, signature);
        Serializer serializer = Serializers.lookupSerializer(EntryAddCommit.Request.class);
        
        EntryAddCommit.Request copiedRequest = (EntryAddCommit.Request)addObjectAndCreateCopiedObject(serializer,originalBuf, request, copiedBuf);
        org.junit.Assert.assertEquals("Entry Add Commit Test", request, copiedRequest);
    }
    


    @Test
    public void idScorePairTest(){

        logger.info("Id Score Pair Test");

        IdScorePair originalScorePair = new IdScorePair(getApplicationEntryId(0, 100, 100), new Float(1.343));
        Serializer serializer = Serializers.lookupSerializer(IdScorePair.class);

        IdScorePair copiedScorePair = (IdScorePair)addObjectAndCreateCopiedObject(serializer, originalBuf, originalScorePair, copiedBuf);
        Assert.assertEquals("Id Score Pair Test", originalScorePair, copiedScorePair);
    }

    @Test
    public void searchQueryRequestTest(){
        logger.info("Search Query Request Test");

        SearchPattern searchPattern = new SearchPattern("sweep", 1, 1, null, null, "English", MsConfig.Categories.Video, "Description");
        SearchQuery.Request request = new SearchQuery.Request(UUID.randomUUID(), 0, searchPattern);

        Serializer serializer = Serializers.lookupSerializer(SearchQuery.Request.class);
        SearchQuery.Request copiedRequest = (SearchQuery.Request)addObjectAndCreateCopiedObject(serializer, originalBuf, request, copiedBuf);

        Assert.assertEquals("Search query request test", request, copiedRequest);
    }

    @Test
    public void searchQueryResponseTest(){
        logger.info("Search Query Response Test");

        List<IdScorePair> scorePairs = new ArrayList<IdScorePair>();
        IdScorePair idsp1 = new IdScorePair(new ApplicationEntry.ApplicationEntryId(0,100,100), new Float(1.22));
        IdScorePair idsp2 = new IdScorePair(new ApplicationEntry.ApplicationEntryId(1,100,100), new Float(1.32));

        scorePairs.add(idsp1);
        scorePairs.add(idsp2);

        SearchQuery.Response response  = new SearchQuery.Response(UUID.randomUUID(), 0, scorePairs);
        Serializer serializer = Serializers.lookupSerializer(SearchQuery.Response.class);


        SearchQuery.Response copiedResponse = (SearchQuery.Response)addObjectAndCreateCopiedObject(serializer, originalBuf, response, copiedBuf);
        Assert.assertEquals("SearchQuery Response Test", response, copiedResponse);
    }

    
    @Test
    public void entryScorePairTest(){
        logger.info("Entry Score Pair Test");
        EntryScorePair esp = new EntryScorePair(getTestApplicationEntry(), new Float(1.112));
        
        Serializer serializer = Serializers.lookupSerializer(EntryScorePair.class);
        EntryScorePair copiedPair = (EntryScorePair)addObjectAndCreateCopiedObject(serializer, originalBuf, esp, copiedBuf);
        
        Assert.assertEquals("entry score pair test", esp, copiedPair);
    }
    
    
    
    @Test
    public void searchFetchRequestTest(){
        logger.info("Search Fetch Request Test");

        List<IdScorePair> entryIds = new ArrayList<IdScorePair>();

        entryIds.add(new IdScorePair(new ApplicationEntry.ApplicationEntryId(0, 100, 100), new Float(1.32)));
        entryIds.add(new IdScorePair(new ApplicationEntry.ApplicationEntryId(1, 100, 100), new Float(1.22)));

        SearchFetch.Request fetchRequest = new SearchFetch.Request(UUID.randomUUID(), entryIds);
        Serializer serializer = Serializers.lookupSerializer(SearchFetch.Request.class);

        SearchFetch.Request copiedFetchRequest = (SearchFetch.Request)addObjectAndCreateCopiedObject(serializer, originalBuf, fetchRequest, copiedBuf);
        Assert.assertEquals("search fetch request", fetchRequest, copiedFetchRequest);
    }

    @Test
    public void searchFetchResponseTest(){

        logger.info("Search Fetch Response Test");
        List<ApplicationEntry> collection = getApplicationEntryCollection(3);
        List<EntryScorePair> entryScorePairs = new ArrayList<EntryScorePair>();
        
        for(ApplicationEntry entry : collection){
            entryScorePairs.add(new EntryScorePair(entry, new Float(1.0)));
        }
        SearchFetch.Response response =  new SearchFetch.Response(UUID.randomUUID(), entryScorePairs);

        Serializer serializer = Serializers.lookupSerializer(SearchFetch.Response.class);
        SearchFetch.Response copiedResponse = (SearchFetch.Response)addObjectAndCreateCopiedObject(serializer, originalBuf, response, copiedBuf);

        Assert.assertEquals("Search fetch response", response, copiedResponse);
    }


    /**
     * Helper method to take the object and then add it to the buffer and then
     * copy the buffer to another
     * @param serializer
     * @param originalBuf
     * @param originalObject
     * @param copiedBuf
     * @return Copied Object
     */
    private Object addObjectAndCreateCopiedObject(Serializer serializer, ByteBuf originalBuf, Object originalObject, ByteBuf copiedBuf){

        serializer.toBinary(originalObject , originalBuf);
        copiedBuf = Unpooled.wrappedBuffer(originalBuf.array());

        return serializer.fromBinary(copiedBuf, Optional.absent());
    }


    /**
     * Helper method to take the object and then add it to the buffer and then
     * copy the buffer to another
     * @param originalBuf original buffer
     * @param originalObject original object
     * @param copiedBuf copied object.
     *
     * @return Copied Object
     */
    private Object addObjectAndCreateCopiedObject(ByteBuf originalBuf, Object originalObject, ByteBuf copiedBuf){

        Serializers.toBinary(originalObject , originalBuf);
        copiedBuf = Unpooled.wrappedBuffer(originalBuf.array());

        return Serializers.fromBinary(copiedBuf, Optional.absent());
    }



    public static void generateKeys() throws NoSuchAlgorithmException {

        logger.info("Generated public / private key pair.");

        KeyPairGenerator keyGen;
        keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(1024);
        final KeyPair key = keyGen.generateKeyPair();
        privateKey = key.getPrivate();
        publicKey = key.getPublic();

    }


    /**
     * Helper method to help with the assertion check.
     * @param description
     * @param expected
     * @param original
     */
    private void performEqualsCheck(String description, Object expected, Object original){
        Assert.assertEquals(description, expected, original);

    }


    private PartitionHelper.PartitionInfo getPartitionInfo(){
        PartitionHelper.PartitionInfo originalPartitionInfo = new PartitionHelper.PartitionInfo(random.nextInt(), UUID.randomUUID(), PartitioningType.NEVER_BEFORE, "Hash", publicKey);
        return originalPartitionInfo;
    }


    private List<IndexEntry> getIndexEntryCollection(int entryNumber){

        IndexEntry entry;
        List<IndexEntry> entryCollection = new ArrayList<IndexEntry>();

        while(entryNumber > 0){

            entry = new IndexEntry("globalId"+entryNumber, entryNumber, "url", "Avengers: Age of Ultron", 10189, new Date(),   "English", MsConfig.Categories.Video, "Description", "Hash", publicKey);
            entryCollection.add(entry);
            entryNumber --;
        }
        return entryCollection;
    }

    public IndexEntry getRandomIndexEntry() {
        return randomIndexEntry;
    }



    public Collection<IndexHash> getIndexHashCollection(int length){

        Collection<IndexHash> collection = new ArrayList<IndexHash>();
        for(int i = 0 ; i < length; i++){
            IndexHash hash = new IndexHash(getRandomIndexEntry());
            collection.add(hash);
        }

        return collection;
    }



    public IndexEntry getSpecialIndexEntry(){

        IndexEntry entry = new IndexEntry(null, -9223372036854775808l,  "220", "messi.mp4", 1, new Date(), "english", MsConfig.Categories.Video, "Something", null, null);
        return entry;
    }



    public ApplicationEntry getTestApplicationEntry (){

        ApplicationEntry.ApplicationEntryId entryId = testApplicationEntryId();
        IndexEntry entry = getRandomIndexEntry();

        return new ApplicationEntry(entryId, entry);
    }


    /**
     * Simply construct an application entryid.
     * @return
     */
    public ApplicationEntry.ApplicationEntryId testApplicationEntryId(){
        return getApplicationEntryId(0,100,0);
    }


    public ApplicationEntry.ApplicationEntryId getApplicationEntryId(long epochId, int leaderId, long entryId){
        return new ApplicationEntry.ApplicationEntryId(epochId, leaderId, entryId);
    }




    public List<ApplicationEntry> getApplicationEntryCollection(int size) {

        List<IndexEntry> entryCollection = getIndexEntryCollection(10);
        List<ApplicationEntry> applicationEntries = new ArrayList<ApplicationEntry>();

        for(IndexEntry entry : entryCollection){
            ApplicationEntry.ApplicationEntryId entryId = getApplicationEntryId(0, 100, entry.getId());
            applicationEntries.add(new ApplicationEntry(entryId, entry));
        }

        return applicationEntries;
    }



    public List<EntryHash> getEntryHashCollection(int size) {

        List<EntryHash> collection = new ArrayList<EntryHash>();
        List<ApplicationEntry> entryCollection = getApplicationEntryCollection(size);

        for(ApplicationEntry entry : entryCollection){
            collection.add(new EntryHash(entry));
        }

        return collection;
    }


    public Collection<ApplicationEntry.ApplicationEntryId> entryIdCollection(int size) {

        Collection<ApplicationEntry.ApplicationEntryId> entryIdCollection = new ArrayList<ApplicationEntry.ApplicationEntryId>();
        Collection<ApplicationEntry> entryCollection  = getApplicationEntryCollection(size);

        for(ApplicationEntry entry : entryCollection) {
            entryIdCollection.add(entry.getApplicationEntryId());
        }

        return entryIdCollection;
    }


    /**
     * Generate a random leader unit collection in the system.
     * All leader units returned as part of this collection has closed leader units.
     * @param size
     * @return
     */
    public List<LeaderUnit> getLeaderUnitCollection(int size) {

        List<LeaderUnit> luCollection = new ArrayList<LeaderUnit>();

        long epoch = 0;
        int leaderId = 100;

        while(size > 0 ){

            LeaderUnit lu = new BaseLeaderUnit( epoch, leaderId, size );
            luCollection.add(lu);

            size --;
        }

        return luCollection;
    }



}
