/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.peersearch.net;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.gvod.common.msgs.*;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.config.BaseCommandLineConfig;
import se.sics.gvod.net.Nat;
import se.sics.gvod.address.Address;
import se.sics.gvod.common.UtilityVod;
import se.sics.gvod.config.VodConfig;
import se.sics.gvod.common.hp.HPMechanism;
import se.sics.gvod.common.hp.HPRole;
import se.sics.peersearch.exceptions.IllegalSearchString;
import se.sics.peersearch.messages.*;
import se.sics.gvod.timer.TimeoutId;
import se.sics.gvod.timer.UUID;
import se.sics.peersearch.types.IndexEntry;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.peersearch.types.SearchPattern;

/**
 *
 * @author jdowling
 */
public class EncodingDecodingTest {

    private static Address src, dest;
    private static InetSocketAddress inetSrc, inetDest;
    private static VodAddress gSrc, gDest;
    private static int overlay = 120;
    private static UtilityVod utility = new UtilityVod(1, 12, 123);
    private static TimeoutId id = UUID.nextUUID();
    private static int age = 200;
    private static int freshness = 100;
    private static int remoteClientId = 12123454;
    private static HPMechanism hpMechanism = HPMechanism.PRP_PRC;
    private static HPRole hpRole = HPRole.PRC_RESPONDER;
    private static Nat nat;
    private static VodDescriptor nodeDescriptor;
    private static List<VodDescriptor> descriptors = new ArrayList<VodDescriptor>();
    private static byte[] availableChunks = new byte[2031];
    private static byte[][] availablePieces = new byte[52][19];

    public EncodingDecodingTest() {
        System.setProperty("java.net.preferIPv4Stack", "true");
    }

    private void opCodeCorrect(ChannelBuffer buffer, Encodable msg) {
        byte type = buffer.readByte();
        assert (type == msg.getOpcode());
    }
    
    
    @BeforeClass
    public static void setUpClass() throws Exception {
//        InetAddress self = InetAddress.getLocalHost();
        InetAddress self = InetAddress.getByName("127.0.0.1");
        src = new Address(self, 58027, 123);
        dest = new Address(self, 65535, 123);
        inetSrc = new InetSocketAddress(self, 58027);
        inetDest = new InetSocketAddress(self, 65535);
        gSrc = new VodAddress(src, VodConfig.SYSTEM_OVERLAY_ID);
        gDest = new VodAddress(dest, VodConfig.SYSTEM_OVERLAY_ID);
        nodeDescriptor = new VodDescriptor(gSrc, utility,
                age, BaseCommandLineConfig.DEFAULT_MTU);
        descriptors.add(nodeDescriptor);
        nat = new Nat(Nat.Type.NAT,
                Nat.MappingPolicy.HOST_DEPENDENT,
                Nat.AllocationPolicy.PORT_PRESERVATION,
                Nat.FilteringPolicy.PORT_DEPENDENT,
                1,
                100 * 1000l);

        DirectMsgNettyFactory.setMsgFrameDecoder(MessageFrameDecoder.class);

    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void searchRequest() {
        SearchPattern pattern = new SearchPattern("abc", 1, 100, new Date(100L), new Date(200L), "language", IndexEntry.Category.Books, "booo");
        UUID requestId = (UUID)UUID.nextUUID();
        SearchMessage.Request msg = new SearchMessage.Request(gSrc, gDest, UUID.nextUUID(), requestId, pattern);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            SearchMessage.Request request =
                    SearchMessageFactory.Request.fromBuffer(buffer);
            assert (request.getRequestId().equals(requestId));
            assert (request.getPattern().equals(pattern));
        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }
    
    
    @Test
    public void searchResponse() {
        try {
            UUID requestId = (UUID)UUID.nextUUID();
            TimeoutId id = UUID.nextUUID();
            int numResponses = 5, responseNum = 1;
            String url = "url";
            String fileName = "fileName";
            Long size = 123L;
            Date time = new Date();
            String language = "language";
            String description = "description";
            String hash = "hash";
            IndexEntry entry = new IndexEntry(url, fileName, size, time, language, IndexEntry.Category.Music, description, hash);
            SearchMessage.Response msg = new SearchMessage.Response(gSrc, gDest, id, requestId, numResponses, responseNum, new IndexEntry[] {entry});
            try {
                ChannelBuffer buffer = msg.toByteArray();
                opCodeCorrect(buffer, msg);
                SearchMessage.Response response =
                        SearchMessageFactory.Response.fromBuffer(buffer);
                assert (id.equals(response.getTimeoutId()));
                assert (response.getNumResponses() == numResponses);
                assert (response.getResponseNumber() == responseNum);
                assert (response.getRequestId().equals(requestId));
                assert (response.getResults()[0].equals(entry));
            } catch (MessageDecodingException ex) {
                Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
                assert (false);
            } catch (MessageEncodingException ex) {
                Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
                assert (false);
            }
        } catch (IllegalSearchString ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
                assert (false);
        }
    }

    @Test
    public void AddIndexEntryRequest() {
        TimeoutId id = UUID.nextUUID();
        int numResponses = 5, responseNum = 1;
        String url = "url";
        String fileName = "fileName";
        Long size = 123L;
        Date time = new Date();
        String language = "language";
        String description = "description";
        String hash = "hash";
        IndexEntry entry = new IndexEntry(url, fileName, size, time, language, IndexEntry.Category.Music, description, hash);
        AddIndexEntryMessage.Request msg = new AddIndexEntryMessage.Request(gSrc, gDest, UUID.nextUUID(), entry, (UUID) id, numResponses, responseNum);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            AddIndexEntryMessage.Request request =
                    AddIndexEntryMessageFactory.Request.fromBuffer(buffer);
            assert (id.equals(request.getId()));
            assert (request.getNumResponses() == numResponses);
            assert (request.getResponseNumber() == responseNum);
            assert (request.getEntry().getUrl().equals(url));
            assert (request.getEntry().getFileName().equals(fileName));
            assert (request.getEntry().getFileSize() == size);
            assert (request.getEntry().getUploaded().equals(time));
            assert (request.getEntry().getLanguage().equals(language));
            assert (request.getEntry().getDescription().equals(description));
            assert (request.getEntry().getHash().equals(hash));
            assert (request.getEntry().getCategory() == IndexEntry.Category.Music);
            assert (request.getEntry().getId().equals(Long.MIN_VALUE));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void AddIndexEntryResponse() {
        TimeoutId id = UUID.nextUUID();
        int numResponses = 5, responseNum = 1;
        String url = "url";
        String fileName = "fileName";
        Long size = 123L;
        Date time = new Date();
        String language = "language";
        String description = "description";
        String hash = "hash";
        String leaderId = "leaderId";
        Long entryId = 1L;
        IndexEntry entry = new IndexEntry(entryId, url, fileName, size, time, language, IndexEntry.Category.Music, description, hash, leaderId);
        AddIndexEntryMessage.Response msg = new AddIndexEntryMessage.Response(gSrc, gDest, UUID.nextUUID(), entry, (UUID) id, numResponses, responseNum);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            AddIndexEntryMessage.Response response =
                    AddIndexEntryMessageFactory.Response.fromBuffer(buffer);
            assert (id.equals(response.getId()));
            assert (response.getNumResponses() == numResponses);
            assert (response.getResponseNumber() == responseNum);
            assert (response.getEntry().getUrl().equals(url));
            assert (response.getEntry().getFileName().equals(fileName));
            assert (response.getEntry().getFileSize() == size);
            assert (response.getEntry().getUploaded().equals(time));
            assert (response.getEntry().getLanguage().equals(language));
            assert (response.getEntry().getDescription().equals(description));
            assert (response.getEntry().getHash().equals(hash));
            assert (response.getEntry().getLeaderId().equals(leaderId));
            assert (entryId.equals(response.getEntry().getId()));
            assert (response.getEntry().getCategory() == IndexEntry.Category.Music);

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void ReplicationRequest() {
        TimeoutId id = UUID.nextUUID();
        int numResponses = 5, responseNum = 1;
        String url = "url";
        String fileName = "fileName";
        Long size = 123L;
        Date time = new Date();
        String language = "language";
        String description = "description";
        String hash = "hash";
        IndexEntry entry = new IndexEntry(url, fileName, size, time, language, IndexEntry.Category.Music, description, hash);
        ReplicationMessage.Request msg = new ReplicationMessage.Request(gSrc, gDest, UUID.nextUUID(), (UUID) id, entry, numResponses, responseNum);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            ReplicationMessage.Request request =
                    ReplicationMessageFactory.Request.fromBuffer(buffer);
            assert (id.equals(request.getId()));
            assert (request.getNumResponses() == numResponses);
            assert (request.getResponseNumber() == responseNum);
            assert (request.getIndexEntry().getUrl().equals(url));
            assert (request.getIndexEntry().getFileName().equals(fileName));
            assert (request.getIndexEntry().getFileSize() == size);
            assert (request.getIndexEntry().getUploaded().equals(time));
            assert (request.getIndexEntry().getLanguage().equals(language));
            assert (request.getIndexEntry().getDescription().equals(description));
            assert (request.getIndexEntry().getHash().equals(hash));
            assert (request.getIndexEntry().getCategory() == IndexEntry.Category.Music);
            assert (request.getIndexEntry().getId().equals(Long.MIN_VALUE));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void ReplicationResponse() {
        TimeoutId id = UUID.nextUUID();
        ReplicationMessage.Response msg = new ReplicationMessage.Response(gSrc, gDest, UUID.nextUUID(), (UUID) id);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            ReplicationMessage.Response request =
                    ReplicationMessageFactory.Response.fromBuffer(buffer);
            assert (id.equals(request.getId()));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void IndexExchangeRequest() {
        long oldestMissingIndexValue = 1L;
        Long[] existingEntries = new Long[]{1L, 2L};
        int numResponses=1;
        int responseNumber=2;
        IndexExchangeMessage.Request msg = new IndexExchangeMessage.Request(gSrc, gDest, UUID.nextUUID(), oldestMissingIndexValue, existingEntries, numResponses, responseNumber);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            IndexExchangeMessage.Request request =
                    IndexExchangeMessageFactory.Request.fromBuffer(buffer);
            assert (request.getOldestMissingIndexValue() == oldestMissingIndexValue);
            for(int i=0; i<request.getExistingEntries().length; i++) {
                assert (request.getExistingEntries()[i] == existingEntries[i]);
            }
            assert (request.getNumResponses() == numResponses);
            assert (request.getResponseNumber() == responseNumber);

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void IndexExchangeResponse() {
        String url = "url";
        String fileName = "fileName";
        Long size = 123L;
        Date time = new Date();
        String language = "language";
        String description = "description";
        String hash = "hash";
        IndexEntry entry;
        entry = new IndexEntry(url, fileName, size, time, language, IndexEntry.Category.Music, description, hash);
        IndexEntry[] items = new IndexEntry[]{entry};
        int numResponses=1;
        int responseNumber=2;
        IndexExchangeMessage.Response msg = new IndexExchangeMessage.Response(gSrc, gDest, UUID.nextUUID(), items, numResponses, responseNumber);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            IndexExchangeMessage.Response response =
                    IndexExchangeMessageFactory.Response.fromBuffer(buffer);

            assert (response.getIndexEntries()[0].equals(entry));
            assert (response.getNumResponses() == numResponses);
            assert (response.getResponseNumber() == responseNumber);

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void GapDetectionRequest() {
        long id = 1L;
        GapDetectionMessage.Request msg = new GapDetectionMessage.Request(gSrc, gDest, UUID.nextUUID(), id);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            GapDetectionMessage.Request request =
                    GapDetectionMessageFactory.Request.fromBuffer(buffer);
            assert (request.getMissingEntryId() == id);

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void GapDetectionResponse() {
        String url = "url";
        String fileName = "fileName";
        Long size = 123L;
        Date time = new Date();
        String language = "language";
        String description = "description";
        String hash = "hash";
        IndexEntry entry = null;
        entry = new IndexEntry(url, fileName, size, time, language, IndexEntry.Category.Music, description, hash);
        int numResponses=1;
        int responseNumber=2;
        GapDetectionMessage.Response msg = new GapDetectionMessage.Response(gSrc, gDest, UUID.nextUUID(), entry, numResponses, responseNumber);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            GapDetectionMessage.Response response =
                    GapDetectionMessageFactory.Response.fromBuffer(buffer);

            assert (response.getMissingEntry().equals(entry));
            assert (response.getNumResponses() == numResponses);
            assert (response.getResponseNumber() == responseNumber);
            assert (response.getMissingEntry().getUrl().equals(url));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void ElectionRequest() {
        int counter = 1;
        ElectionMessage.Request msg = new ElectionMessage.Request(gSrc, gDest, 1, 2, UUID.nextUUID(), counter);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            ElectionMessage.Request request =
                    ElectionMessageFactory.Request.fromBuffer(buffer);

            assert (request.getClientId() == 1);
            assert (request.getRemoteId() == 2);
            assert (request.getVoteID() == counter);

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void ElectionResponse() {
        int voteId = 1;
        boolean convereged = true;
        boolean vote = true;

        InetAddress address1 = null;
        try {
            address1 = InetAddress.getByName("192.168.0.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
                VodConfig.SYSTEM_OVERLAY_ID, nat);
        ElectionMessage.Response msg = new ElectionMessage.Response(gSrc, gDest, 1, 2, gDest, UUID.nextUUID(), RelayMsgNetty.Status.OK, voteId, convereged, vote, vodAddress1);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            ElectionMessage.Response response =
                    ElectionMessageFactory.Response.fromBuffer(buffer);

            assert (response.getClientId() == 1);
            assert (response.getRemoteId() == 2);
            assert (response.getStatus() == RelayMsgNetty.Status.OK);
            assert (response.isConvereged() == convereged);
            assert (response.getVoteId() == voteId);
            assert (response.isVote() == vote);
            assert (response.getHighest().equals(vodAddress1));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void GradientShuffleRequest() {
        InetAddress address1 = null;
        try {
            address1 = InetAddress.getByName("192.168.0.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
                VodConfig.SYSTEM_OVERLAY_ID, nat);

        GradientShuffleMessage.Request msg = new GradientShuffleMessage.Request(gSrc, gDest, UUID.nextUUID(), new VodAddress[]{vodAddress1});
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            GradientShuffleMessage.Request request =
                    GradientShuffleMessageFactory.Request.fromBuffer(buffer);
            for(int i=0; i<request.getAddresses().length; i++)
                assert (request.getAddresses()[i].equals(vodAddress1));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void GradientShuffleResponse() {
        InetAddress address1 = null;
        try {
            address1 = InetAddress.getByName("192.168.0.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
                VodConfig.SYSTEM_OVERLAY_ID, nat);

        GradientShuffleMessage.Response msg = new GradientShuffleMessage.Response(gSrc, gDest, UUID.nextUUID(), new VodAddress[]{vodAddress1});
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            GradientShuffleMessage.Response response =
                    GradientShuffleMessageFactory.Response.fromBuffer(buffer);
            for(int i=0; i<response.getAddresses().length; i++)
                assert (response.getAddresses()[i].equals(vodAddress1));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void LeaderAnnouncement() {
        InetAddress address1 = null;
        try {
            address1 = InetAddress.getByName("192.168.0.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
                VodConfig.SYSTEM_OVERLAY_ID, nat);

        LeaderAnnouncementMessage msg = new LeaderAnnouncementMessage(gSrc, gDest, vodAddress1);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            LeaderAnnouncementMessage response =
                    LeaderAnnouncementMessageFactory.fromBuffer(buffer);
            assert (response.getLeader().equals(vodAddress1));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void LeaderSuspectionRequest() {
        InetAddress address1 = null;
        try {
            address1 = InetAddress.getByName("192.168.0.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
                VodConfig.SYSTEM_OVERLAY_ID, nat);

        LeaderSuspectionMessage.Request msg = new LeaderSuspectionMessage.Request(gSrc, gDest, 1, 2, UUID.nextUUID(), vodAddress1);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            LeaderSuspectionMessage.Request request =
                    LeaderSuspectionMessageFactory.Request.fromBuffer(buffer);

            assert (request.getClientId() == 1);
            assert (request.getRemoteId() == 2);
            assert (request.getLeader().equals(vodAddress1));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void LeaderSuspectionResponse() {
        boolean isSuspected = true;
        InetAddress address1 = null;
        try {
            address1 = InetAddress.getByName("192.168.0.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
                VodConfig.SYSTEM_OVERLAY_ID, nat);

        LeaderSuspectionMessage.Response msg = new LeaderSuspectionMessage.Response(gSrc, gDest, 1, 2, gDest, UUID.nextUUID(), RelayMsgNetty.Status.OK, isSuspected, vodAddress1);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            LeaderSuspectionMessage.Response response =
                    LeaderSuspectionMessageFactory.Response.fromBuffer(buffer);

            assert (response.getClientId() == 1);
            assert (response.getRemoteId() == 2);
            assert (response.getStatus() == RelayMsgNetty.Status.OK);
            assert (response.isSuspected() == isSuspected);
            assert (response.getLeader().equals(vodAddress1));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void HeartbeatRequest() {

        HeartbeatMessage.Request msg = new HeartbeatMessage.Request(gSrc, gDest, UUID.nextUUID());
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            HeartbeatMessage.Request request =
                    HeartbeatMessageFactory.Request.fromBuffer(buffer);

            assert (request.getVodDestination().equals(gDest));
            assert (request.getVodSource().equals(gSrc));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void HeartbeatResponse() {

        HeartbeatMessage.Response msg = new HeartbeatMessage.Response(gSrc, gDest, UUID.nextUUID());
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            HeartbeatMessage.Response response =
                    HeartbeatMessageFactory.Response.fromBuffer(buffer);

            assert (response.getVodDestination().equals(gDest));
            assert (response.getVodSource().equals(gSrc));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void AddIndexEntryRoutedMessage() {
        TimeoutId id = UUID.nextUUID();
        int numResponses = 5, responseNum = 1;
        String url = "url";
        String fileName = "fileName";
        Long size = 123L;
        Date time = new Date();
        String language = "language";
        String description = "description";
        String hash = "hash";
        IndexEntry entry = new IndexEntry(url, fileName, size, time, language, IndexEntry.Category.Music, description, hash);
        AddIndexEntryMessage.Request request = new AddIndexEntryMessage.Request(gSrc, gDest, UUID.nextUUID(), entry, (UUID) id, numResponses, responseNum);
        AddIndexEntryRoutedMessage msg = new AddIndexEntryRoutedMessage(gSrc, gDest, request);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            AddIndexEntryRoutedMessage response =
                    AddIndexEntryRoutedMessageFactory.fromBuffer(buffer);

            assert (response.getVodDestination().equals(gDest));
            assert (response.getVodSource().equals(gSrc));
            assert (response.getMessage().getEntry().equals(entry));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void GapDetectionRoutedMessage() {
        String url = "url";
        String fileName = "fileName";
        Long size = 123L;
        Date time = new Date();
        String language = "language";
        String description = "description";
        String hash = "hash";
        GapDetectionMessage.Request request = new GapDetectionMessage.Request(gSrc, gDest, UUID.nextUUID(), 1L);
        GapDetectionRoutedMessage msg = new GapDetectionRoutedMessage(gSrc, gDest, request);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            GapDetectionRoutedMessage response =
                    GapDetectionRoutedMessageFactory.fromBuffer(buffer);

            assert (response.getVodDestination().equals(gDest));
            assert (response.getVodSource().equals(gSrc));
            assert (response.getMessage().getMissingEntryId()==1L);

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void VotingResultMessage() {
        InetAddress address1 = null;
        try {
            address1 = InetAddress.getByName("192.168.0.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
                VodConfig.SYSTEM_OVERLAY_ID, nat);

        VotingResultMessage msg = new VotingResultMessage(gSrc, gDest, new VodAddress[]{vodAddress1});
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            VotingResultMessage response =
                    VotingResultMessageFactory.fromBuffer(buffer);

            assert (response.getVodDestination().equals(gDest));
            assert (response.getVodSource().equals(gSrc));
            assert (response.getView()[0].equals(vodAddress1));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void RejectFollowerRequest() {

        RejectFollowerMessage.Request msg = new RejectFollowerMessage.Request(gSrc, gDest, UUID.nextUUID());
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            RejectFollowerMessage.Request request =
                    RejectFollowerMessageFactory.Request.fromBuffer(buffer);

            assert (request.getVodDestination().equals(gDest));
            assert (request.getVodSource().equals(gSrc));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void RejectFollowerResponse() {
        boolean inView = true;
        RejectFollowerMessage.Response msg = new RejectFollowerMessage.Response(gSrc, gDest, UUID.nextUUID(), inView);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            RejectFollowerMessage.Response request =
                    RejectFollowerMessageFactory.Response.fromBuffer(buffer);

            assert (request.getVodDestination().equals(gDest));
            assert (request.getVodSource().equals(gSrc));
            assert (request.isInView() == inView);

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void RejectLeaderMessage() {
        InetAddress address1 = null;
        try {
            address1 = InetAddress.getByName("192.168.0.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
                VodConfig.SYSTEM_OVERLAY_ID, nat);

        RejectLeaderMessage msg = new RejectLeaderMessage(gSrc, gDest, vodAddress1);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            RejectLeaderMessage response =
                    RejectLeaderMessageFactory.fromBuffer(buffer);

            assert (response.getVodDestination().equals(gDest));
            assert (response.getVodSource().equals(gSrc));
            assert (response.getBetterLeader().equals(vodAddress1));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void StartIndexRequestMessage() {
        TimeoutId uiid = UUID.nextUUID();

        StartIndexRequestMessage msg = new StartIndexRequestMessage(gSrc, gDest, uiid);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            StartIndexRequestMessage response =
                    StartIndexRequestMessageFactory.fromBuffer(buffer);

            assert (response.getVodDestination().equals(gDest));
            assert (response.getVodSource().equals(gSrc));
            assert (response.getTimeoutId().equals(uiid));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void IndexRequestMessage() {
        TimeoutId uiid = UUID.nextUUID();
        long index = 1L;

        InetAddress address1 = null;
        try {
            address1 = InetAddress.getByName("192.168.0.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
                VodConfig.SYSTEM_OVERLAY_ID, nat);


        IndexRequestMessage msg = new IndexRequestMessage(gSrc, gDest, uiid, index, vodAddress1);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            IndexRequestMessage response =
                    IndexRequestMessageFactory.fromBuffer(buffer);

            assert (response.getVodDestination().equals(gDest));
            assert (response.getVodSource().equals(gSrc));
            assert (response.getTimeoutId().equals(uiid));
            assert (response.getIndex() == index);
            assert (response.getLeaderAddress().equals(vodAddress1));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }


    @Test
    public void IndexDisseminationMessage() {
        long index = 1L;

        IndexDisseminationMessage msg = new IndexDisseminationMessage(gSrc, gDest, index);
        try {
            ChannelBuffer buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            IndexDisseminationMessage response =
                    IndexDisseminationMessageFactory.fromBuffer(buffer);

            assert (response.getVodDestination().equals(gDest));
            assert (response.getVodSource().equals(gSrc));
            assert (response.getIndex() == index);

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }
}
