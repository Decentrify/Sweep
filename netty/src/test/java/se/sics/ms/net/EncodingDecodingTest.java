///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package se.sics.ms.net;
//
//import io.netty.buffer.ByteBuf;
//import org.junit.*;
//import se.sics.gvod.address.Address;
//import se.sics.gvod.common.UtilityVod;
//import se.sics.gvod.common.hp.HPMechanism;
//import se.sics.gvod.common.hp.HPRole;
//import se.sics.gvod.common.msgs.*;
//import se.sics.gvod.config.VodConfig;
//import se.sics.gvod.net.Nat;
//import se.sics.gvod.net.VodAddress;
//import se.sics.gvod.timer.TimeoutId;
//import se.sics.gvod.timer.UUID;
//import se.sics.ms.configuration.MsConfig;
//import se.sics.ms.exceptions.IllegalSearchString;
//import se.sics.ms.messages.*;
//import se.sics.ms.types.*;
//import se.sics.ms.util.OverlayIdHelper;
//import se.sics.ms.util.PartitionHelper;
//
//import java.net.InetAddress;
//import java.net.InetSocketAddress;
//import java.net.UnknownHostException;
//import java.security.*;
//import java.util.*;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//
///**
// *
// * @author jdowling
// */
//public class EncodingDecodingTest {
//
//    private static Address src, dest;
//    private static InetSocketAddress inetSrc, inetDest;
//    private static VodAddress gSrc, gDest;
//    private static int overlay = 120;
//    private static UtilityVod utility = new UtilityVod(1, 12, 123);
//    private static TimeoutId id = UUID.nextUUID();
//    private static int age = 200;
//    private static int freshness = 100;
//    private static int remoteClientId = 12123454;
//    private static HPMechanism hpMechanism = HPMechanism.PRP_PRC;
//    private static HPRole hpRole = HPRole.PRC_RESPONDER;
//    private static Nat nat;
//    private static PeerDescriptor nodeDescriptor;
//    private static List<PeerDescriptor> descriptors = new ArrayList<PeerDescriptor>();
//    private static byte[] availableChunks = new byte[2031];
//    private static byte[][] availablePieces = new byte[52][19];
//    private PublicKey publicKey;
//    private PrivateKey privateKey;
//
//    public EncodingDecodingTest() {
//        System.setProperty("java.net.preferIPv4Stack", "true");
//    }
//
//    private void opCodeCorrect(ByteBuf buffer, Encodable msg) {
//        byte type = buffer.readByte();
//        assert (type == msg.getOpcode());
//    }
//    
//    
//    @BeforeClass
//    public static void setUpClass() throws Exception {
//        InetAddress self = InetAddress.getByName("localhost");
//        src = new Address(self, 58027, 123);
//        dest = new Address(self, 65535, 123);
//        inetSrc = new InetSocketAddress(self, 58027);
//        inetDest = new InetSocketAddress(self, 65535);
//        gSrc = new VodAddress(src, VodConfig.SYSTEM_OVERLAY_ID);
//        gDest = new VodAddress(dest, VodConfig.SYSTEM_OVERLAY_ID);
////        nodeDescriptor = new SearchDescriptor(gSrc);
//        nodeDescriptor = null;
//        descriptors.add(nodeDescriptor);
//        nat = new Nat(Nat.Type.NAT,
//                Nat.MappingPolicy.HOST_DEPENDENT,
//                Nat.AllocationPolicy.PORT_PRESERVATION,
//                Nat.FilteringPolicy.PORT_DEPENDENT,
//                1,
//                100 * 1000l);
//
//        DirectMsgNettyFactory.Base.setMsgFrameDecoder(MessageFrameDecoder.class);
//    }
//
//    @AfterClass
//    public static void tearDownClass() throws Exception {
//    }
//
//    @Before
//    public void setUp() {
//        KeyPairGenerator keyGen;
//        try {
//            keyGen = KeyPairGenerator.getInstance("RSA");
//            keyGen.initialize(2048);
//            final KeyPair key = keyGen.generateKeyPair();
//            publicKey = key.getPublic();
//            privateKey = key.getPrivate();
//        } catch (NoSuchAlgorithmException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @After
//    public void tearDown() {
//    }
//
//    @Test
//    public void searchRequest() {
//        SearchPattern pattern = new SearchPattern("abc", 1, 100, new Date(100L), new Date(200L), "language", MsConfig.Categories.Books, "booo");
//        TimeoutId timeoutId = UUID.nextUUID();
//        int partitionId = 1;
//        SearchMessage.Request msg = new SearchMessage.Request(gSrc, gDest, UUID.nextUUID(), timeoutId, pattern, partitionId);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            SearchMessage.Request request = SearchMessageFactory.Request.fromBuffer(buffer);
//            assert timeoutId.equals(request.getSearchTimeoutId());
//            assert (request.getPattern().equals(pattern));
//            assert msg.getPartitionId() == partitionId;
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void searchRequestWithNullDate() {
//        SearchPattern pattern = new SearchPattern("abc", 1, 100, null, null, "language", MsConfig.Categories.Books, "booo");
//        TimeoutId timeoutId = UUID.nextUUID();
//        int partitionId = 1;
//        SearchMessage.Request msg = new SearchMessage.Request(gSrc, gDest, UUID.nextUUID(), timeoutId, pattern, partitionId);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            SearchMessage.Request request = SearchMessageFactory.Request.fromBuffer(buffer);
//            assert timeoutId.equals(request.getSearchTimeoutId());
//            assert (request.getPattern().equals(pattern));
//            assert msg.getPartitionId() == partitionId;
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//
//
//    @Test
//    public void searchResponse() {
//        try {
//            UUID requestId = (UUID)UUID.nextUUID();
//            TimeoutId id = UUID.nextUUID();
//            int numResponses = 5, responseNum = 1;
//            String url = "url";
//            String fileName = "fileName";
//            Long size = 123L;
//            Date time = new Date();
//            String language = "language";
//            String description = "description";
//            IndexEntry entry = new IndexEntry(java.util.UUID.randomUUID().toString(), url, fileName, size, time, language, MsConfig.Categories.Music, description);
//            TimeoutId timeoutId = UUID.nextUUID();
//            Collection<IndexEntry> indexEntries = new ArrayList<IndexEntry>();
//            int partitionId = 1;
//            indexEntries.add(entry);
//            SearchMessage.Response msg = new SearchMessage.Response(gSrc, gDest, id, timeoutId, numResponses, responseNum, indexEntries, partitionId);
//            try {
//                ByteBuf buffer = msg.toByteArray();
//                opCodeCorrect(buffer, msg);
//                SearchMessage.Response response = SearchMessageFactory.Response.fromBuffer(buffer);
//                assert (id.equals(response.getTimeoutId()));
//                assert (response.getNumResponses() == numResponses);
//                assert (response.getResponseNumber() == responseNum);
//                assert (response.getResults().iterator().next().equals(entry));
//                assert timeoutId.equals(response.getSearchTimeoutId());
//                assert msg.getPartitionId() == partitionId;
//            } catch (MessageDecodingException ex) {
//                Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//                assert (false);
//            } catch (MessageEncodingException ex) {
//                Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//                assert (false);
//            }
//        } catch (IllegalSearchString ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//                assert (false);
//        }
//    }
//
//    @Test
//    public void AddIndexEntryRequest() {
//        String url = "url";
//        String fileName = "fileName";
//        Long size = 123L;
//        Date time = new Date();
//        String language = "language";
//        String description = "description";
//        IndexEntry entry = new IndexEntry(java.util.UUID.randomUUID().toString(), url, fileName, size, time, language, MsConfig.Categories.Music, description);
//        AddIndexEntryMessage.Request msg = new AddIndexEntryMessage.Request(gSrc, gDest, UUID.nextUUID(), entry);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            AddIndexEntryMessage.Request request = AddIndexEntryMessageFactory.Request.fromBuffer(buffer);
//            assert (request.getEntry().getUrl().equals(url));
//            assert (request.getEntry().getFileName().equals(fileName));
//            assert (request.getEntry().getFileSize() == size);
//            assert (request.getEntry().getUploaded().equals(time));
//            assert (request.getEntry().getLanguage().equals(language));
//            assert (request.getEntry().getDescription().equals(description));
//            assert (request.getEntry().getCategory() == MsConfig.Categories.Music);
//            assert (request.getEntry().getId().equals(Long.MIN_VALUE));
//
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void AddIndexEntryResponse() {;
//        AddIndexEntryMessage.Response msg = new AddIndexEntryMessage.Response(gSrc, gDest, UUID.nextUUID());
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            AddIndexEntryMessage.Response response = AddIndexEntryMessageFactory.Response.fromBuffer(buffer);
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void IndexExchangeRequest() {
//        Collection<Id> ids = new ArrayList<Id>();
//        ids.add(new Id(0, null));
//        IndexExchangeMessage.Request msg = new IndexExchangeMessage.Request(gSrc, gDest, UUID.nextUUID(), ids);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            IndexExchangeMessage.Request request = IndexExchangeMessageFactory.Request.fromBuffer(buffer);
//            Iterator<Id> i = ids.iterator();
//            for (Id id : ids) {
//                id.equals(i.next());
//            }
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void IndexExchangeResponse() {
//        String url = "url";
//        String fileName = "fileName";
//        Long size = 123L;
//        Date time = new Date();
//        String language = "language";
//        String description = "description";
//        IndexEntry entry;
//        entry = new IndexEntry(java.util.UUID.randomUUID().toString(), url, fileName, size, time, language, MsConfig.Categories.Music, description);
//        Collection<IndexEntry> items = new ArrayList<IndexEntry>();
//        items.add(entry);
//        int numResponses=1;
//        int responseNumber=2;
//        IndexExchangeMessage.Response msg = new IndexExchangeMessage.Response(gSrc, gDest, UUID.nextUUID(), items, numResponses, responseNumber);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            IndexExchangeMessage.Response response =
//                    IndexExchangeMessageFactory.Response.fromBuffer(buffer);
//
//            assert (response.getIndexEntries().iterator().next().equals(entry));
//            assert (response.getNumResponses() == numResponses);
//            assert (response.getResponseNumber() == responseNumber);
//
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void ElectionRequest() {
//        int counter = 1;
//        PeerDescriptor self =  null;
////        SearchDescriptor self = new SearchDescriptor(new Decnew BasicAddress(null, 0 , 0));
//        ElectionMessage.Request msg = new ElectionMessage.Request(gSrc, gDest, UUID.nextUUID(), counter, self);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            ElectionMessage.Request request = ElectionMessageFactory.Request.fromBuffer(buffer);
//
//            assert (request.getVoteID() == counter);
//            assert (request.getLeaderCandidateDescriptor().equals(self));
//
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void ElectionResponse() {
//        int voteId = 1;
//        boolean converged = true;
//        boolean vote = true;
//
//        InetAddress address1 = null;
//        try {
//            address1 = InetAddress.getByName("192.168.0.1");
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1), VodConfig.SYSTEM_OVERLAY_ID, nat);
//        PeerDescriptor searchDescriptor = null;
////        SearchDescriptor searchDescriptor = new SearchDescriptor(vodAddress1);
//        ElectionMessage.Response msg = new ElectionMessage.Response(gSrc, gDest, UUID.nextUUID(), voteId, converged, vote, searchDescriptor);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            ElectionMessage.Response response = ElectionMessageFactory.Response.fromBuffer(buffer);
//
//            assert (response.isConvereged() == converged);
//            assert (response.getVoteId() == voteId);
//            assert (response.isVote() == vote);
//            assert (response.getHighestUtilityNode().equals(searchDescriptor));
//
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void GradientShuffleRequest() {
//        InetAddress address1 = null;
//        try {
//            address1 = InetAddress.getByName("192.168.0.1");
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
//                VodConfig.SYSTEM_OVERLAY_ID, nat);
////        SearchDescriptor searchDescriptor = new SearchDescriptor(vodAddress1);
//        PeerDescriptor searchDescriptor = null;
//        Set<PeerDescriptor> set = new HashSet<PeerDescriptor>();
//        set.add(searchDescriptor);
//
//        GradientShuffleMessage.Request msg = new GradientShuffleMessage.Request(gSrc, gDest, UUID.nextUUID(), set);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            GradientShuffleMessage.Request request = GradientShuffleMessageFactory.Request.fromBuffer(buffer);
//            assert (request.getSearchDescriptors().iterator().next().equals(searchDescriptor));
//
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void GradientShuffleResponse() {
//        InetAddress address1 = null;
//        try {
//            address1 = InetAddress.getByName("192.168.0.1");
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
//                VodConfig.SYSTEM_OVERLAY_ID, nat);
////        SearchDescriptor searchDescriptor = new SearchDescriptor(vodAddress1);
//        PeerDescriptor searchDescriptor = null;
//        Set<PeerDescriptor> set = new HashSet<PeerDescriptor>();
//        set.add(searchDescriptor);
//
//        GradientShuffleMessage.Response msg = new GradientShuffleMessage.Response(gSrc, gDest, UUID.nextUUID(), set);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            GradientShuffleMessage.Response response =
//                    GradientShuffleMessageFactory.Response.fromBuffer(buffer);
//            assert (response.getSearchDescriptors().iterator().next().equals(searchDescriptor));
//
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void LeaderAnnouncement() {
//        InetAddress address1 = null;
//        try {
//            address1 = InetAddress.getByName("192.168.0.1");
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1), VodConfig.SYSTEM_OVERLAY_ID, nat);
////        SearchDescriptor searchDescriptor = new SearchDescriptor(vodAddress1);
//        PeerDescriptor searchDescriptor = null;
//
//        LeaderDeathAnnouncementMessage msg = new LeaderDeathAnnouncementMessage(gSrc, gDest, searchDescriptor);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            LeaderDeathAnnouncementMessage response = LeaderDeathAnnouncementMessageFactory.fromBuffer(buffer);
//            assert (response.getLeader().equals(searchDescriptor));
//
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void LeaderSuspectionRequest() {
//        InetAddress address1 = null;
//        try {
//            address1 = InetAddress.getByName("192.168.0.1");
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
//                VodConfig.SYSTEM_OVERLAY_ID, nat);
//
//        LeaderSuspicionMessage.Request msg = new LeaderSuspicionMessage.Request(gSrc, gDest, 1, 2, UUID.nextUUID(), vodAddress1);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            LeaderSuspicionMessage.Request request =
//                    LeaderSuspicionMessageFactory.Request.fromBuffer(buffer);
//
//            assert (request.getClientId() == 1);
//            assert (request.getRemoteId() == 2);
//            assert (request.getLeader().equals(vodAddress1));
//
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void LeaderSuspectionResponse() {
//        boolean isSuspected = true;
//        InetAddress address1 = null;
//        try {
//            address1 = InetAddress.getByName("192.168.0.1");
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
//                VodConfig.SYSTEM_OVERLAY_ID, nat);
//
//        LeaderSuspicionMessage.Response msg = new LeaderSuspicionMessage.Response(gSrc, gDest, 1, 2, gDest, UUID.nextUUID(), RelayMsgNetty.Status.OK, isSuspected, vodAddress1);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            LeaderSuspicionMessage.Response response =
//                    LeaderSuspicionMessageFactory.Response.fromBuffer(buffer);
//
//            assert (response.getClientId() == 1);
//            assert (response.getRemoteId() == 2);
//            assert (response.getStatus() == RelayMsgNetty.Status.OK);
//            assert (response.isSuspected() == isSuspected);
//            assert (response.getLeader().equals(vodAddress1));
//
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void VotingResultMessage() {
//        InetAddress address1 = null;
//        try {
//            address1 = InetAddress.getByName("192.168.0.1");
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        PeerDescriptor leaderSearchDescriptor = null;
////        SearchDescriptor leaderSearchDescriptor = new SearchDescriptor(gSrc);
//        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1), VodConfig.SYSTEM_OVERLAY_ID, nat);
//        PeerDescriptor searchDescriptor = null;
////        SearchDescriptor searchDescriptor = new SearchDescriptor(vodAddress1);
//        Set<PeerDescriptor> set = new HashSet<PeerDescriptor>();
//        set.add(searchDescriptor);
//
//        LeaderViewMessage msg = new LeaderViewMessage(gSrc, gDest, leaderSearchDescriptor, set, null);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            LeaderViewMessage response = VotingResultMessageFactory.fromBuffer(buffer);
//
//            assert (response.getVodDestination().equals(gDest));
//            assert (response.getVodSource().equals(gSrc));
//            assert (response.getLeaderSearchDescriptor().equals(leaderSearchDescriptor));
//            assert (response.getSearchDescriptors().iterator().next().equals(searchDescriptor));
//
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void RejectFollowerRequest() {
//
//        RejectFollowerMessage.Request msg = new RejectFollowerMessage.Request(gSrc, gDest, UUID.nextUUID());
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            RejectFollowerMessage.Request request =
//                    RejectFollowerMessageFactory.Request.fromBuffer(buffer);
//
//            assert (request.getVodDestination().equals(gDest));
//            assert (request.getVodSource().equals(gSrc));
//
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void RejectFollowerResponse() {
//        boolean inView = true;
//        RejectFollowerMessage.Response msg = new RejectFollowerMessage.Response(gSrc, gDest, UUID.nextUUID(), inView);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            RejectFollowerMessage.Response request =
//                    RejectFollowerMessageFactory.Response.fromBuffer(buffer);
//
//            assert (request.getVodDestination().equals(gDest));
//            assert (request.getVodSource().equals(gSrc));
//            assert (request.isNodeInView() == inView);
//
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void RejectLeaderMessage() {
//        InetAddress address1 = null;
//        try {
//            address1 = InetAddress.getByName("192.168.0.1");
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        VodAddress vodAddress1 = new VodAddress(new Address(address1, 8081, 1),
//                VodConfig.SYSTEM_OVERLAY_ID, nat);
//        PeerDescriptor searchDescriptor = null;
////        SearchDescriptor searchDescriptor = new SearchDescriptor(vodAddress1);
//
//        RejectLeaderMessage msg = new RejectLeaderMessage(gSrc, gDest, searchDescriptor);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            RejectLeaderMessage response =
//                    RejectLeaderMessageFactory.fromBuffer(buffer);
//
//            assert (response.getVodDestination().equals(gDest));
//            assert (response.getVodSource().equals(gSrc));
//            assert (response.getBetterLeader().equals(searchDescriptor));
//
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void LeaderLookupRequest() {
//        LeaderLookupMessage.Request msg = new LeaderLookupMessage.Request(gSrc, gDest, UUID.nextUUID());
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            LeaderLookupMessage.Request request = LeaderLookupMessageFactory.Request.fromBuffer(buffer);
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void LeaderLookupResponse() {
//        InetAddress address = null;
//        try {
//            address = InetAddress.getByName("192.168.0.1");
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        boolean terminated = false;
//        VodAddress vodAddress = new VodAddress(new Address(address, 8081, 1),
//                VodConfig.SYSTEM_OVERLAY_ID, nat);
//        PeerDescriptor searchDescriptor = null;
////        SearchDescriptor searchDescriptor = new SearchDescriptor(vodAddress);
//
//        List<PeerDescriptor> items = new ArrayList<PeerDescriptor>();
//        items.add(searchDescriptor);
//        LeaderLookupMessage.Response msg = new LeaderLookupMessage.Response(gSrc, gDest, UUID.nextUUID(), terminated, items);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            LeaderLookupMessage.Response response = LeaderLookupMessageFactory.Response.fromBuffer(buffer);
//
//            assert terminated == response.isLeader();
//            assert (response.getSearchDescriptors().get(0).equals(searchDescriptor));
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void RepairRequest() {
//        Long[] ids = new Long[]{1L};
//        RepairMessage.Request msg = new RepairMessage.Request(gSrc, gDest, UUID.nextUUID(), ids);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            RepairMessage.Request request = RepairMessageFactory.Request.fromBuffer(buffer);
//            assert (ids[0] == request.getMissingIds()[0]);
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void RepairResponse() {
//        String url = "url";
//        String fileName = "fileName";
//        Long size = 123L;
//        Date time = new Date();
//        String language = "language";
//        String description = "description";
//        String hash = "hash";
//
//        IndexEntry entry = new IndexEntry(null, 1, url, fileName, size, time, language, MsConfig.Categories.Music, description, hash, publicKey);
//        Collection<IndexEntry> indexEntries = new ArrayList<IndexEntry>();
//        indexEntries.add(entry);
//        RepairMessage.Response msg = new RepairMessage.Response(gSrc, gDest, UUID.nextUUID(), indexEntries);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            RepairMessage.Response request = RepairMessageFactory.Response.fromBuffer(buffer);
//            assert (entry.equals(request.getMissingEntries().iterator().next()));
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void PublicKeyMessage() {
//        PublicKeyMessage msg = new PublicKeyMessage(gSrc, gDest, publicKey);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            PublicKeyMessage request = PublicKeyMessageFactory.fromBuffer(buffer);
//            assert (request.getPublicKey().equals(publicKey));
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void PrepairCommitRequest() {
//        String url = "url";
//        String fileName = "fileName";
//        Long size = 123L;
//        Date time = new Date();
//        String language = "language";
//        String description = "description";
//        IndexEntry entry = new IndexEntry(java.util.UUID.randomUUID().toString(), url, fileName, size, time, language, MsConfig.Categories.Music, description);
//        ReplicationPrepareCommitMessage.Request msg = new ReplicationPrepareCommitMessage.Request(gSrc, gDest, UUID.nextUUID(), entry);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            ReplicationPrepareCommitMessage.Request request = ReplicationPrepairCommitMessageFactory.Request.fromBuffer(buffer);
//            assert (request.getEntry().getUrl().equals(url));
//            assert (request.getEntry().getFileName().equals(fileName));
//            assert (request.getEntry().getFileSize() == size);
//            assert (request.getEntry().getUploaded().equals(time));
//            assert (request.getEntry().getLanguage().equals(language));
//            assert (request.getEntry().getDescription().equals(description));
//            assert (request.getEntry().getCategory() == MsConfig.Categories.Music);
//            assert (request.getEntry().getId().equals(Long.MIN_VALUE));
//
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void PrepairCommitResponse() {
//        Long size = 123L;
//        ReplicationPrepareCommitMessage.Response msg = new ReplicationPrepareCommitMessage.Response(gSrc, gDest, UUID.nextUUID(), size);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            ReplicationPrepareCommitMessage.Response response = ReplicationPrepairCommitMessageFactory.Response.fromBuffer(buffer);
//            assert (response.getEntryId() == size);
//
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void CommitRequest() {
//        Long size = 123L;
//        String signature = "abc";
//        ReplicationCommitMessage.Request msg = new ReplicationCommitMessage.Request(gSrc, gDest, UUID.nextUUID(), size, signature);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            ReplicationCommitMessage.Request request = ReplicationCommitMessageFactory.Request.fromBuffer(buffer);
//            assert (request.getEntryId() == size);
//            assert (request.getSignature().equals(signature));
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void CommitResponse() {
//        Long size = 123L;
//        ReplicationCommitMessage.Response msg = new ReplicationCommitMessage.Response(gSrc, gDest, UUID.nextUUID(), size);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            ReplicationCommitMessage.Response response = ReplicationCommitMessageFactory.Response.fromBuffer(buffer);
//            assert (response.getEntryId() == size);
//
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void CategoryAndPartitionIdEncoding() {
//        InetAddress address = null;
//        try {
//            address = InetAddress.getByName("192.168.0.1");
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        VodAddress.PartitioningType type = VodAddress.PartitioningType.ONCE_BEFORE;
//        int depth = 3;
//        int partitionId = 4;
//        int categoryId = (int) Math.pow(2, 16) - 2;
//        OverlayId overlayId = new OverlayId(OverlayIdHelper.encodePartitionDataAndCategoryIdAsInt(type,
//                depth, partitionId, categoryId));
//
//        assert overlayId.getCategoryId() == categoryId;
//        assert overlayId.getPartitionId() == partitionId;
//        assert overlayId.getPartitionIdDepth() == depth;
//        assert overlayId.getPartitioningType() == type;
//    }
//
//    /* Number of entries in Search Descriptor is not used anywhere, that field is now removed.
//     * So this test is not valid anymore.
//    @Test
//    public void NumberOfEntries() {
//        InetAddress address = null;
//        try {
//            address = InetAddress.getByName("192.168.0.1");
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        long numberOfEntries = 5L;
//        VodAddress vodAddress = new VodAddress(new Address(address, 8081, 1), VodConfig.SYSTEM_OVERLAY_ID, nat);
//
//        SearchDescriptor searchDescriptor = new SearchDescriptor(vodAddress, numberOfEntries);
//
//        List<SearchDescriptor> items = new ArrayList<SearchDescriptor>();
//        items.add(searchDescriptor);
//        LeaderLookupMessage.Response msg = new LeaderLookupMessage.Response(gSrc, gDest, UUID.nextUUID(), false, items);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            LeaderLookupMessage.Response response = LeaderLookupMessageFactory.Response.fromBuffer(buffer);
//
//            assert (response.getSearchDescriptors().get(0).getNumberOfIndexEntries() == numberOfEntries);
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//*/
//    @Test
//    public void IndexHashExchangeRequest() {
//        long oldestMissingIndexValue = 1L;
//        Long[] existingEntries = new Long[]{1L, 2L};
//        IndexHashExchangeMessage.Request msg = new IndexHashExchangeMessage.Request(gSrc, gDest, UUID.nextUUID(), oldestMissingIndexValue, existingEntries);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            IndexHashExchangeMessage.Request request = IndexHashExchangeMessageFactory.Request.fromBuffer(buffer);
//            assert request.getOldestMissingIndexValue() == oldestMissingIndexValue;
//
//            for(int i = 0; i < request.getExistingEntries().length; i++) {
//                assert (request.getExistingEntries()[i] == existingEntries[i]);
//            }
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//    @Test
//    public void IndexHashExchangeResponse() {
//        IndexHash hash  = new IndexHash(new Id(0, null), "hash");
//        Collection<IndexHash> hashes = new ArrayList<IndexHash>();
//        hashes.add(hash);
//        IndexHashExchangeMessage.Response msg = new IndexHashExchangeMessage.Response(gSrc, gDest, UUID.nextUUID(), hashes);
//        try {
//            ByteBuf buffer = msg.toByteArray();
//            opCodeCorrect(buffer, msg);
//            IndexHashExchangeMessage.Response response = IndexHashExchangeMessageFactory.Response.fromBuffer(buffer);
//
//            Iterator<IndexHash> i = hashes.iterator();
//            for (IndexHash h : response.getHashes()) {
//                assert h.equals(i.next());
//            }
//        } catch (MessageDecodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        } catch (MessageEncodingException ex) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
//            assert (false);
//        }
//    }
//
//
//    // FIXME: Complete the Implementation of Test Cases.
//
//    @Test
//    public void PartitionPrepareRequestTest(){
//
//        long middleEntry = 1L;
//        TimeoutId requestId = UUID.nextUUID();
//        TimeoutId roundId = UUID.nextUUID();
//        int oId = 99;
//        OverlayId overlayId = new OverlayId(oId);
//
//        VodAddress.PartitioningType partitioningType = VodAddress.PartitioningType.NEVER_BEFORE;
//
//        PartitionHelper.PartitionInfo partitionInfo = new PartitionHelper.PartitionInfo(middleEntry, null , partitioningType);
//        PartitionPrepareMessage.Request partitionPrepareRequest = new PartitionPrepareMessage.Request(gSrc,gDest,overlayId,roundId,partitionInfo);
//
//        try{
//
//            ByteBuf buffer = partitionPrepareRequest.toByteArray();
//            opCodeCorrect(buffer, partitionPrepareRequest);
//            PartitionPrepareMessage.Request partitionPrepareRequestDecoded = PartitionPrepareMessageFactory.Request.fromBuffer(buffer);
//
//            assert(partitionPrepareRequestDecoded.getPartitionInfo().getMedianId() == middleEntry);
//            assert(partitionPrepareRequestDecoded.getPartitionInfo().getRequestId().equals(requestId));
//            assert(partitionPrepareRequestDecoded.getPartitionInfo().getPartitioningTypeInfo() == partitioningType);
//            assert(partitionPrepareRequestDecoded.getOverlayId().getId() == oId);
//
//
//        } catch (MessageEncodingException e) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, e);
//            assert (false);
//        } catch (MessageDecodingException e) {
//            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, e);
//            assert (false);
//        }
//
//
//    }
//
//    @Test
//    public void PartitionPrepareResponseTest(){
//
//        TimeoutId roundId = UUID.nextUUID();
//        TimeoutId requestId = UUID.nextUUID();
//
//        PartitionPrepareMessage.Response partitionPrepareResponse = new PartitionPrepareMessage.Response(gSrc,gDest,roundId , requestId);
//
//        try {
//            ByteBuf byteBuffer = partitionPrepareResponse.toByteArray();
//            opCodeCorrect(byteBuffer, partitionPrepareResponse);
//            PartitionPrepareMessage.Response partitionPrepareResponseDecoded = PartitionPrepareMessageFactory.Response.fromBuffer(byteBuffer);
//
//            assert(partitionPrepareResponseDecoded.getPartitionRequestId().equals(requestId));
//            assert(partitionPrepareResponseDecoded.getTimeoutId().equals(roundId));
//
//        } catch (MessageEncodingException e) {
//            e.printStackTrace();
//        } catch (MessageDecodingException e) {
//            e.printStackTrace();
//        }
//
//    }
//
//
//    @Test
//    public void PartitionCommitRequestTest(){
//
//        TimeoutId roundId = UUID.nextUUID();
//        TimeoutId requestId = UUID.nextUUID();
//
//        PartitionCommitMessage.Request partitionCommitRequest = new PartitionCommitMessage.Request(gSrc,gDest,roundId , requestId);
//
//        try {
//            ByteBuf byteBuffer = partitionCommitRequest.toByteArray();
//            opCodeCorrect(byteBuffer, partitionCommitRequest);
//            PartitionCommitMessage.Request partitionCommitRequestDecoded = PartitionCommitMessageFactory.Request.fromBuffer(byteBuffer);
//
//            assert(partitionCommitRequestDecoded.getPartitionRequestId().equals(requestId));
//            assert(partitionCommitRequestDecoded.getTimeoutId().equals(roundId));
//
//        } catch (MessageEncodingException e) {
//            e.printStackTrace();
//        } catch (MessageDecodingException e) {
//            e.printStackTrace();
//        }
//
//
//    }
//
//
//    @Test
//    public void PartitionCommitResponseTest(){
//
//
//        TimeoutId roundId = UUID.nextUUID();
//        TimeoutId requestId = UUID.nextUUID();
//
//        PartitionCommitMessage.Response partitionCommitResponse = new PartitionCommitMessage.Response(gSrc,gDest,roundId , requestId);
//
//        try {
//            ByteBuf byteBuffer = partitionCommitResponse.toByteArray();
//            opCodeCorrect(byteBuffer, partitionCommitResponse);
//            PartitionCommitMessage.Response partitionCommitResponseDecoded = PartitionCommitMessageFactory.Response.fromBuffer(byteBuffer);
//
//            assert(partitionCommitResponseDecoded.getPartitionRequestId().equals(requestId));
//            assert(partitionCommitResponseDecoded.getTimeoutId().equals(roundId));
//
//        } catch (MessageEncodingException e) {
//            e.printStackTrace();
//        } catch (MessageDecodingException e) {
//            e.printStackTrace();
//        }
//
//    }
//
//
//    @Test
//    public void ControlMessageRequestTest(){
//
//        TimeoutId roundId = UUID.nextUUID();
//        int oId = 99;//some random number
//        OverlayId overlayId = new OverlayId(oId);
//        ControlMessage.Request controlMessageRequest = new ControlMessage.Request(gSrc,gDest,overlayId,roundId);
//
//        try{
//            ByteBuf byteBuffer = controlMessageRequest.toByteArray();
//            opCodeCorrect(byteBuffer, controlMessageRequest);
//            ControlMessage.Request controlMessageRequestDecoded = ControlMessageFactory.Request.fromBuffer(byteBuffer);
//            assert(controlMessageRequestDecoded.getRoundId().equals(roundId));
//            assert(controlMessageRequestDecoded.getOverlayId().getId() == oId);
//
//        } catch (MessageEncodingException e) {
//            e.printStackTrace();
//        } catch (MessageDecodingException e) {
//            e.printStackTrace();
//        }
//
//    }
//
//
//}
