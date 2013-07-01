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
import se.sics.gvod.common.msgs.Encodable;
import se.sics.gvod.common.msgs.MessageDecodingException;
import se.sics.gvod.common.msgs.MessageEncodingException;
import se.sics.peersearch.msgs.SearchMsgFactory;
import se.sics.gvod.common.msgs.VodMsgNettyFactory;
import se.sics.gvod.timer.TimeoutId;
import se.sics.gvod.timer.UUID;
import se.sics.peersearch.msgs.SearchMsg;

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

        VodMsgNettyFactory.setMsgFrameDecoder(PsMsgFrameDecoder.class);

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
            SearchMsg.Request msg = new SearchMsg.Request(gSrc, gDest, UUID.nextUUID(), query);
            try {
                ChannelBuffer buffer = msg.toByteArray();
                opCodeCorrect(buffer, msg);
                SearchMsg.Request request =
                        SearchMsgFactory.Request.fromBuffer(buffer);
                assert (query.equals(request.getQuery()));
            } catch (MessageDecodingException ex) {
                Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
                assert (false);
            } catch (MessageEncodingException ex) {
                Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
                assert (false);
            }
        } catch (SearchMsg.IllegalSearchString ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
            assert(false);
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
            SearchMsg.Response msg = new SearchMsg.Response(gSrc, gDest, id, numResponses, responseNum, res);
            try {
                ChannelBuffer buffer = msg.toByteArray();
                opCodeCorrect(buffer, msg);
                SearchMsg.Response response =
                        SearchMsgFactory.Response.fromBuffer(buffer);
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
        } catch (SearchMsg.IllegalSearchString ex) {
            Logger.getLogger(EncodingDecodingTest.class.getName()).log(Level.SEVERE, null, ex);
                assert (false);
        }
    }    
    
}
