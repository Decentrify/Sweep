package se.sics.p2ptoolbox.election.core;

import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.election.core.msg.LeaderExtensionRequest;
import se.sics.p2ptoolbox.election.core.msg.LeaderPromiseMessage;
import se.sics.p2ptoolbox.election.core.msg.LeaseCommitMessage;
import se.sics.p2ptoolbox.election.core.net.CommitMessageSerializer;
import se.sics.p2ptoolbox.election.core.net.ExtensionMessageSerializer;
import se.sics.p2ptoolbox.election.core.net.PromiseMessageSerializer;
import se.sics.p2ptoolbox.election.core.net.PublicKeySerializer;
import se.sics.p2ptoolbox.serialization.SerializationContext;
import se.sics.p2ptoolbox.serialization.msg.NetMsg;
import se.sics.p2ptoolbox.serialization.serializer.SerializerAdapter;

import java.security.PublicKey;

/**
 * Network Settings for the standalone Leader Election Protocol.
 *
 * Created by babbar on 2015-04-02.
 */
public class LENetworkSettings {

    private static SerializationContext context = null;

    private static final String PROMISE_REQUEST_ALIAS = "PROMISE_NET_REQUEST";
    private static final String PROMISE_RESPONSE_ALIAS = "PROMISE_NET_RESPONSE";

    private static final String EXTENSION_ONE_WAY = "EXTENSION_ONE_WAY";
    private static final String COMMIT_ONE_WAY = "COMMIT_ONE_WAY";


    public static void oneTimeSetup(SerializationContext setContext, byte promiseRequestAlias, byte promiseResponseAlias, byte extensionAlias, byte leaseCommitAlias){

        if(context != null) {
            throw new RuntimeException("croupier has already been setup - do not call this multiple times(for each croupier instance)");
        }
        context = setContext;

        registerNetworkMsg(promiseRequestAlias, promiseResponseAlias, extensionAlias, leaseCommitAlias);
        registerOthers();

        checkSetup();
    }


    private static void registerNetworkMsg(byte promiseRequestAlias, byte promiseResponseAlias, byte extensionAlias, byte leaseCommitAlias){

        try{
            context.registerAlias(DirectMsgNetty.Request.class, PROMISE_REQUEST_ALIAS, promiseRequestAlias);
            context.registerAlias(DirectMsgNetty.Response.class, PROMISE_RESPONSE_ALIAS, promiseResponseAlias);

            context.registerSerializer(LeaderPromiseMessage.Request.class, new PromiseMessageSerializer.Request());
            context.registerSerializer(LeaderPromiseMessage.Response.class, new PromiseMessageSerializer.Response());

            context.multiplexAlias(PROMISE_REQUEST_ALIAS, LeaderPromiseMessage.Request.class, (byte)0x01);
            context.multiplexAlias(PROMISE_RESPONSE_ALIAS, LeaderPromiseMessage.Response.class,(byte)0x01);

            context.registerAlias(DirectMsgNetty.Oneway.class, EXTENSION_ONE_WAY, extensionAlias);
            context.registerSerializer(LeaderExtensionRequest.class, new ExtensionMessageSerializer());
            context.multiplexAlias(EXTENSION_ONE_WAY, LeaderExtensionRequest.class, (byte)0x01);

            context.registerAlias(DirectMsgNetty.Oneway.class, COMMIT_ONE_WAY, leaseCommitAlias);
            context.registerSerializer(LeaseCommitMessage.class, new CommitMessageSerializer());
            context.multiplexAlias(COMMIT_ONE_WAY, LeaseCommitMessage.class, (byte)0x01);


        } catch (SerializationContext.DuplicateException e) {
            e.printStackTrace();
        } catch (SerializationContext.MissingException e) {
            e.printStackTrace();
        }

    }


    private static void registerOthers(){

        try{
            context.registerSerializer(PublicKey.class, new PublicKeySerializer());
        } catch (SerializationContext.DuplicateException e) {
            e.printStackTrace();
        }

    }



    private static void checkSetup() {
        if (context == null || !NetMsg.hasContext() || !SerializerAdapter.hasContext()) {
            throw new RuntimeException("serialization context not set");
        }

        try {
            for (OtherSerializers serializedClass : OtherSerializers.values()) {
                context.getSerializer(serializedClass.serializedClass);
            }
        } catch (SerializationContext.MissingException ex) {
            throw new RuntimeException(ex);
        }
    }


    public static enum OtherSerializers {
        UUID(java.util.UUID.class), VOD_ADDRESS(VodAddress.class);

        public final Class serializedClass;

        OtherSerializers(Class serializedClass) {
            this.serializedClass = serializedClass;
        }
    }

}
