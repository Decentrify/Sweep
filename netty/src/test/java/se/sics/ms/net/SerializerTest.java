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
import se.sics.gvod.net.VodAddress;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.data.*;
import se.sics.ms.serializer.ReplicationPrepareCommitSerializer;
import se.sics.ms.types.*;
import se.sics.ms.util.PartitionHelper;
import se.sics.p2ptoolbox.election.network.util.PublicKeySerializer;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;
import se.sics.p2ptoolbox.util.serializer.BasicSerializerSetup;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.*;
import java.util.*;

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
    private static DecoratedAddress selfAddress, destinationAddress;
    private static SearchDescriptor selfDescriptor;
    private static SearchDescriptor otherDescriptor;
    private static PublicKey publicKey;
    private static PrivateKey privateKey;
    private static IndexEntry randomIndexEntry;

    @BeforeClass
    public static void oneTimeSetup() throws NoSuchAlgorithmException {

        logger.info("Executing the one time setup.");
        int currentId = 128;
        BasicSerializerSetup.registerBasicSerializers(currentId);
        currentId = currentId + BasicSerializerSetup.serializerIds;
        currentId = SerializerSetup.registerSerializers(currentId);
        
        registerPublicKeySerializer(currentId);

        SerializerSetup.checkSetup();

        selfAddress = new DecoratedAddress(localHost, 54321, 1);
        destinationAddress = new DecoratedAddress(localHost, 54322, 2);
        selfDescriptor = new SearchDescriptor(selfAddress, 1);
        otherDescriptor = new SearchDescriptor(destinationAddress, 0);

        originalBuf = Unpooled.buffer();
        copiedBuf = Unpooled.buffer();
        randomIndexEntry = new IndexEntry("globalId", 0, "url", "Avengers: Age of Ultron", 10189, new Date(),   "English", MsConfig.Categories.Video, "Description", "Hash", publicKey);
        random = new Random();
        generateKeys();
    }

    private static void registerPublicKeySerializer(int currentId) {

        PublicKeySerializer pkSerializer =  new PublicKeySerializer(currentId++);
        Serializers.register(pkSerializer, "publicKeySerializer");
        Serializers.register(PublicKey.class, "publicKeySerializer");

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

        Serializer sdSerializer = Serializers.lookupSerializer(SearchDescriptor.class);
        sdSerializer.toBinary(selfDescriptor, originalBuf);
        copiedBuf = Unpooled.wrappedBuffer(originalBuf.array());

        SearchDescriptor cpyDescriptor = (SearchDescriptor) sdSerializer.fromBinary(copiedBuf, Optional.absent());
        Assert.assertEquals(" SearchDescriptor Equality Check ", selfDescriptor, cpyDescriptor);
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

        IndexEntry originalEntry = getRandomIndexEntry();
        Serializer serializer = Serializers.lookupSerializer(IndexEntry.class);

        IndexEntry copiedEntry = (IndexEntry) addObjectAndCreateCopiedObject(serializer, originalBuf, originalEntry, copiedBuf);
        Assert.assertEquals("Index Entry Serialization", originalEntry, copiedEntry);
    }


    @Test
    public void partitionInfoSerializationTest(){
        logger.info("Partition Info Serialization Test");

        PartitionHelper.PartitionInfo originalPartitionInfo = new PartitionHelper.PartitionInfo(10, UUID.randomUUID(), VodAddress.PartitioningType.NEVER_BEFORE, "Hash", publicKey);
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
    public void addIndexEntryRequestTest() {
        logger.info("Add Index Entry Request Test");

        IndexEntry entry = getRandomIndexEntry();
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


        IndexExchange.Request originalExchangeRequest  = new IndexExchange.Request(UUID.randomUUID(), ids);
        Serializer exchangeSerializer = Serializers.lookupSerializer(IndexExchange.Request.class);

        IndexExchange.Request copiedExchangeRequest = (IndexExchange.Request)addObjectAndCreateCopiedObject(exchangeSerializer, originalBuf, originalExchangeRequest, copiedBuf);
        Assert.assertEquals("Add Index Exchange Request Serialization", originalExchangeRequest, copiedExchangeRequest);
    }


    @Test
    public void addIndexExchangeResponseTest(){
        logger.info("Add Index Exchange Response Serialization Test.");

        IndexExchange.Response originalExchangeResponse  = new IndexExchange.Response(UUID.randomUUID(), getIndexEntryCollection(10), 10, 0);
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

        List<SearchDescriptor> descList = new ArrayList<SearchDescriptor>();
        descList.add(selfDescriptor);
        descList.add(otherDescriptor);

        LeaderLookup.Response originalResponse = new LeaderLookup.Response(UUID.randomUUID(), true, descList);
        Serializer lookupSerializer = Serializers.lookupSerializer(LeaderLookup.Response.class);

        LeaderLookup.Response copiedRequest = (LeaderLookup.Response)addObjectAndCreateCopiedObject(lookupSerializer, originalBuf, originalResponse, copiedBuf);
        Assert.assertEquals("Leader lookup response serialization", originalResponse, copiedRequest);

    }

    @Test
    public void indexHashRequestTest(){
        logger.info("Index Hash Exchange Request Test");

        IndexHashExchange.Request request = new IndexHashExchange.Request(UUID.randomUUID(), 10, new Long[]{(long)10, (long)11, (long)12});
        Serializer serializer = Serializers.lookupSerializer(IndexHashExchange.Request.class);

        IndexHashExchange.Request copiedRequest = (IndexHashExchange.Request) addObjectAndCreateCopiedObject(serializer, originalBuf, request, copiedBuf);
        Assert.assertEquals("Index Hash Exchange Request", request, copiedRequest);
    }



    @Test
    public void indexHashExchangeResponseTest(){
        logger.info("Index Hash Exchange Response Test");

        IndexHashExchange.Response originalResponse = new IndexHashExchange.Response(UUID.randomUUID(), getIndexHashCollection(5));
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
        PartitionHelper.PartitionInfo originalPartitionInfo = new PartitionHelper.PartitionInfo(random.nextInt(), UUID.randomUUID(), VodAddress.PartitioningType.NEVER_BEFORE, "Hash", publicKey);
        return originalPartitionInfo;
    }


    private Collection<IndexEntry> getIndexEntryCollection(int entryNumber){

        IndexEntry entry;
        Collection<IndexEntry> entryCollection = new ArrayList<IndexEntry>();

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


}
