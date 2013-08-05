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
import io.netty.buffer.ByteBuf;
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

    private void opCodeCorrect(ByteBuf buffer, Encodable msg) {
        byte type = buffer.readByte();
        assert (type == msg.getOpcode());
    }
    
    
    @BeforeClass
    public static void setUpClass() throws Exception {
        InetAddress self = InetAddress.getByName("localhost");
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

        DirectMsgNettyFactory.Base.setMsgFrameDecoder(MessageFrameDecoder.class);
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
        SearchMessage.Request msg = new SearchMessage.Request(gSrc, gDest, UUID.nextUUID(), pattern);
        try {
            ByteBuf buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            SearchMessage.Request request =
                    SearchMessageFactory.Request.fromBuffer(buffer);
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
            SearchMessage.Response msg = new SearchMessage.Response(gSrc, gDest, id, numResponses, responseNum, new IndexEntry[] {entry});
            try {
                ByteBuf buffer = msg.toByteArray();
                opCodeCorrect(buffer, msg);
                SearchMessage.Response response =
                        SearchMessageFactory.Response.fromBuffer(buffer);
                assert (id.equals(response.getTimeoutId()));
                assert (response.getNumResponses() == numResponses);
                assert (response.getResponseNumber() == responseNum);
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
        String url = "url";
        String fileName = "fileName";
        Long size = 123L;
        Date time = new Date();
        String language = "language";
        String description = "description";
        String hash = "hash";
        IndexEntry entry = new IndexEntry(url, fileName, size, time, language, IndexEntry.Category.Music, description, hash);
        AddIndexEntryMessage.Request msg = new AddIndexEntryMessage.Request(gSrc, gDest, UUID.nextUUID(), entry);
        try {
            ByteBuf buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            AddIndexEntryMessage.Request request = AddIndexEntryMessageFactory.Request.fromBuffer(buffer);
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
    public void AddIndexEntryResponse() {;
        AddIndexEntryMessage.Response msg = new AddIndexEntryMessage.Response(gSrc, gDest, UUID.nextUUID());
        try {
            ByteBuf buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            AddIndexEntryMessage.Response response = AddIndexEntryMessageFactory.Response.fromBuffer(buffer);
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
        int numResponses = 5, responseNum = 1;
        String url = "url";
        String fileName = "fileName";
        Long size = 123L;
        Date time = new Date();
        String language = "language";
        String description = "description";
        String hash = "hash";
        IndexEntry entry = new IndexEntry(url, fileName, size, time, language, IndexEntry.Category.Music, description, hash);
        ReplicationMessage.Request msg = new ReplicationMessage.Request(gSrc, gDest, UUID.nextUUID(), entry, numResponses, responseNum);
        try {
            ByteBuf buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            ReplicationMessage.Request request = ReplicationMessageFactory.Request.fromBuffer(buffer);
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
        ReplicationMessage.Response msg = new ReplicationMessage.Response(gSrc, gDest, UUID.nextUUID());
        try {
            ByteBuf buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            ReplicationMessage.Response request = ReplicationMessageFactory.Response.fromBuffer(buffer);

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
            ByteBuf buffer = msg.toByteArray();
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
            ByteBuf buffer = msg.toByteArray();
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
    public void ElectionRequest() {
        int counter = 1;
        VodDescriptor self = new VodDescriptor(gSrc);
        ElectionMessage.Request msg = new ElectionMessage.Request(gSrc, gDest, UUID.nextUUID(), counter, self);
        try {
            ByteBuf buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            ElectionMessage.Request request = ElectionMessageFactory.Request.fromBuffer(buffer);

            assert (request.getVoteID() == counter);
            assert (request.getLeaderCandidateDescriptor().equals(self));

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
        boolean converged = true;
        boolean vote = true;

        InetAddress address1 = null;
        try {
            address1 = InetAddress.getByName("192.168.0.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1), VodConfig.SYSTEM_OVERLAY_ID, nat);
        VodDescriptor vodDescriptor = new VodDescriptor(vodAddress1);
        ElectionMessage.Response msg = new ElectionMessage.Response(gSrc, gDest, UUID.nextUUID(), voteId, converged, vote, vodDescriptor);
        try {
            ByteBuf buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            ElectionMessage.Response response = ElectionMessageFactory.Response.fromBuffer(buffer);

            assert (response.isConvereged() == converged);
            assert (response.getVoteId() == voteId);
            assert (response.isVote() == vote);
            assert (response.getHighestUtilityNode().equals(vodDescriptor));

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
            e.printStackTrace();
        }
        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
                VodConfig.SYSTEM_OVERLAY_ID, nat);
        VodDescriptor vodDescriptor = new VodDescriptor(vodAddress1);
        Set<VodDescriptor> set = new HashSet<VodDescriptor>();
        set.add(vodDescriptor);

        GradientShuffleMessage.Request msg = new GradientShuffleMessage.Request(gSrc, gDest, UUID.nextUUID(), set);
        try {
            ByteBuf buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            GradientShuffleMessage.Request request = GradientShuffleMessageFactory.Request.fromBuffer(buffer);
            assert (request.getVodDescriptors().iterator().next().equals(vodDescriptor));

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
            e.printStackTrace();
        }
        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
                VodConfig.SYSTEM_OVERLAY_ID, nat);
        VodDescriptor vodDescriptor = new VodDescriptor(vodAddress1);
        Set<VodDescriptor> set = new HashSet<VodDescriptor>();
        set.add(vodDescriptor);

        GradientShuffleMessage.Response msg = new GradientShuffleMessage.Response(gSrc, gDest, UUID.nextUUID(), set);
        try {
            ByteBuf buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            GradientShuffleMessage.Response response =
                    GradientShuffleMessageFactory.Response.fromBuffer(buffer);
            assert (response.getVodDescriptors().iterator().next().equals(vodDescriptor));

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
            e.printStackTrace();
        }
        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1), VodConfig.SYSTEM_OVERLAY_ID, nat);
        VodDescriptor vodDescriptor = new VodDescriptor(vodAddress1);

        LeaderDeathAnnouncementMessage msg = new LeaderDeathAnnouncementMessage(gSrc, gDest, vodDescriptor);
        try {
            ByteBuf buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            LeaderDeathAnnouncementMessage response = LeaderAnnouncementMessageFactory.fromBuffer(buffer);
            assert (response.getLeader().equals(vodDescriptor));

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
            e.printStackTrace();
        }
        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
                VodConfig.SYSTEM_OVERLAY_ID, nat);

        LeaderSuspicionMessage.Request msg = new LeaderSuspicionMessage.Request(gSrc, gDest, 1, 2, UUID.nextUUID(), vodAddress1);
        try {
            ByteBuf buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            LeaderSuspicionMessage.Request request =
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
            e.printStackTrace();
        }
        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
                VodConfig.SYSTEM_OVERLAY_ID, nat);

        LeaderSuspicionMessage.Response msg = new LeaderSuspicionMessage.Response(gSrc, gDest, 1, 2, gDest, UUID.nextUUID(), RelayMsgNetty.Status.OK, isSuspected, vodAddress1);
        try {
            ByteBuf buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            LeaderSuspicionMessage.Response response =
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
            ByteBuf buffer = msg.toByteArray();
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
            ByteBuf buffer = msg.toByteArray();
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
    public void VotingResultMessage() {
        InetAddress address1 = null;
        try {
            address1 = InetAddress.getByName("192.168.0.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        VodDescriptor leaderVodDescriptor = new VodDescriptor(gSrc);
        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1), VodConfig.SYSTEM_OVERLAY_ID, nat);
        VodDescriptor vodDescriptor = new VodDescriptor(vodAddress1);
        Set<VodDescriptor> set = new HashSet<VodDescriptor>();
        set.add(vodDescriptor);

        LeaderViewMessage msg = new LeaderViewMessage(gSrc, gDest, leaderVodDescriptor, set);
        try {
            ByteBuf buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            LeaderViewMessage response = VotingResultMessageFactory.fromBuffer(buffer);

            assert (response.getVodDestination().equals(gDest));
            assert (response.getVodSource().equals(gSrc));
            assert (response.getLeaderVodDescriptor().equals(leaderVodDescriptor));
            assert (response.getVodDescriptors().iterator().next().equals(vodDescriptor));

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
            ByteBuf buffer = msg.toByteArray();
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
            ByteBuf buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            RejectFollowerMessage.Response request =
                    RejectFollowerMessageFactory.Response.fromBuffer(buffer);

            assert (request.getVodDestination().equals(gDest));
            assert (request.getVodSource().equals(gSrc));
            assert (request.isNodeInView() == inView);

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
            e.printStackTrace();
        }
        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
                VodConfig.SYSTEM_OVERLAY_ID, nat);
        VodDescriptor vodDescriptor = new VodDescriptor(vodAddress1);

        RejectLeaderMessage msg = new RejectLeaderMessage(gSrc, gDest, vodDescriptor);
        try {
            ByteBuf buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            RejectLeaderMessage response =
                    RejectLeaderMessageFactory.fromBuffer(buffer);

            assert (response.getVodDestination().equals(gDest));
            assert (response.getVodSource().equals(gSrc));
            assert (response.getBetterLeader().equals(vodDescriptor));

        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void LeaderLookupRequest() {
        LeaderLookupMessage.Request msg = new LeaderLookupMessage.Request(gSrc, gDest, UUID.nextUUID());
        try {
            ByteBuf buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            LeaderLookupMessage.Request request = LeaderLookupMessageFactory.Request.fromBuffer(buffer);
        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void LeaderLookupResponse() {
        InetAddress address = null;
        try {
            address = InetAddress.getByName("192.168.0.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        boolean terminated = false;
        VodAddress vodAddress = new VodAddress(new Address(address, 8081, 1),
                VodConfig.SYSTEM_OVERLAY_ID, nat);
        VodDescriptor vodDescriptor = new VodDescriptor(vodAddress);

        List<VodDescriptor> items = new ArrayList<VodDescriptor>();
        items.add(vodDescriptor);
        LeaderLookupMessage.Response msg = new LeaderLookupMessage.Response(gSrc, gDest, UUID.nextUUID(), terminated, items);
        try {
            ByteBuf buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            LeaderLookupMessage.Response response = LeaderLookupMessageFactory.Response.fromBuffer(buffer);

            assert terminated == response.isLeader();
            assert (response.getVodDescriptors().get(0).equals(vodDescriptor));
        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    @Test
    public void CategoryAndPartitionIdEncoding() {
        InetAddress address = null;
        try {
            address = InetAddress.getByName("192.168.0.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        VodAddress vodAddress = new VodAddress(new Address(address, 8081, 1), VodAddress.encodePartitionAndCategoryIdAsInt(5,8), nat);
        assert vodAddress.getPartitionId() == 5;
    }

    @Test
    public void NumberOfEntries() {
        InetAddress address = null;
        try {
            address = InetAddress.getByName("192.168.0.1");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        long numberOfEntries = (long) Math.pow(2, 32) - 1;
        VodAddress vodAddress = new VodAddress(new Address(address, 8081, 1), VodConfig.SYSTEM_OVERLAY_ID, nat);
        VodDescriptor vodDescriptor = new VodDescriptor(vodAddress, numberOfEntries);

        List<VodDescriptor> items = new ArrayList<VodDescriptor>();
        items.add(vodDescriptor);
        LeaderLookupMessage.Response msg = new LeaderLookupMessage.Response(gSrc, gDest, UUID.nextUUID(), false, items);
        try {
            ByteBuf buffer = msg.toByteArray();
            opCodeCorrect(buffer, msg);
            LeaderLookupMessage.Response response = LeaderLookupMessageFactory.Response.fromBuffer(buffer);

            assert (response.getVodDescriptors().get(0).getNumberOfIndexEntries() == numberOfEntries);
        } catch (MessageDecodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        } catch (MessageEncodingException ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }
}
