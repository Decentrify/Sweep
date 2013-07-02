/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.peersearch.net;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.config.BaseCommandLineConfig;
import se.sics.gvod.net.Nat;
import se.sics.gvod.address.Address;
import se.sics.gvod.common.UtilityVod;
import se.sics.gvod.config.VodConfig;
import se.sics.gvod.common.hp.HPMechanism;
import se.sics.gvod.common.hp.HPRole;
import se.sics.gvod.common.msgs.DirectMsgNettyFactory;
import se.sics.gvod.common.msgs.Encodable;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.peersearch.exceptions.IllegalSearchString;
import se.sics.peersearch.messages.AddIndexEntryMessage;
import se.sics.peersearch.messages.AddIndexEntryMessageFactory;
import se.sics.peersearch.messages.SearchMessage;
import se.sics.peersearch.messages.SearchMessageFactory;
import se.sics.peersearch.messages.*;
import se.sics.gvod.common.msgs.VodMsgNettyFactory;
import se.sics.gvod.timer.TimeoutId;
import se.sics.gvod.timer.UUID;
import se.sics.peersearch.types.IndexEntry;

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
        try {
            String query = "bbbbbbbbbbbbbbbbbbb";
            SearchMessage.Request msg = new SearchMessage.Request(gSrc, gDest, UUID.nextUUID(), query);
            try {
                ChannelBuffer buffer = msg.toByteArray();
                opCodeCorrect(buffer, msg);
                SearchMessage.Request request =
                        SearchMessageFactory.Request.fromBuffer(buffer);
                assert (query.equals(request.getQuery()));
            } catch (MessageDecodingException ex) {
                Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
                assert (false);
            } catch (MessageEncodingException ex) {
                Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
                assert (false);
            }
        } catch (SearchMessage.IllegalSearchString illegalSearchString) {
            illegalSearchString.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }    
    
    
    @Test
    public void searchResponse() {
        try {
            TimeoutId id = UUID.nextUUID();
            int numResponses = 5, responseNum = 1;
            String res = "day of the diesels day of the diesels day of the diesels"
                    + "day of the diesels" + "day of the diesels" + "day of the diesels" + "day of the diesels" + "day of the diesels"
                    + "day of the diesels" + "day of the diesels" + "day of the diesels"+ "day of the diesels" + "day of the diesels"
                    + "day of the diesels" + "day of the diesels" + "day of the diesels"+ "day of the diesels" + "day of the diesels"
                    + "day of the diesels" + "day of the diesels" + "day of the diesels"+ "day of the diesels" + "day of the diesels"
                    + "day of the diesels" + "day of the diesels" + "day of the diesels"+ "day of the diesels" + "day of the diesels"
                    + "day of the diesels" + "day of the diesels" + "day of the diesels"+ "day of the diesels" + "day of the diesels"
                    + "day of the diesels" + "day of the diesels" + "day of the diesels"+ "day of the diesels" + "day of the diesels"
                    + "day of the diesels" + "day of the diesels" + "day of the diesels"+ "day of the diesels" + "day of the diesels";
            SearchMessage.Response msg = new SearchMessage.Response(gSrc, gDest, id, numResponses, responseNum, res);
            try {
                ChannelBuffer buffer = msg.toByteArray();
                opCodeCorrect(buffer, msg);
                SearchMessage.Response response =
                        SearchMessageFactory.Response.fromBuffer(buffer);
                assert (id.equals(response.getTimeoutId()));
                assert (response.getNumResponses() == numResponses);
                assert (response.getResponseNumber() == responseNum);
                assert (res.compareTo(response.getResults()) == 0);
            } catch (MessageDecodingException ex) {
                Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
                assert (false);
            } catch (MessageEncodingException ex) {
                Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
                assert (false);
            }
        } catch (SearchMessage.IllegalSearchString illegalSearchString) {
            illegalSearchString.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
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
}
