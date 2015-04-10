package se.sics.p2ptoolbox.election.core;

import se.sics.gvod.common.msgs.DirectMsgNetty;
import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.election.core.data.ExtensionRequest;
import se.sics.p2ptoolbox.election.core.data.LeaseCommitUpdated;
import se.sics.p2ptoolbox.election.core.data.Promise;
import se.sics.p2ptoolbox.election.core.msg.LeaderExtensionRequest;
import se.sics.p2ptoolbox.election.core.msg.LeaderPromiseMessage;
import se.sics.p2ptoolbox.election.core.msg.LeaseCommitMessage;
import se.sics.p2ptoolbox.election.core.msg.LeaseCommitMessageUpdated;
import se.sics.p2ptoolbox.election.core.net.*;
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
    private static final String LEASE_COMMIT_REQUEST_ALIAS = "COMMIT_NET_REQUEST";
    private static final String LEASE_COMMIT_RESPONSE_ALIAS = "COMMIT_NET_RESPONSE";



    public static void oneTimeSetup(SerializationContext setContext, byte promiseRequestAlias, byte promiseResponseAlias, byte extensionAlias, byte leaseCommitRequestAlias, byte leaseCommitResponseAlias){

        if(context != null) {
            throw new RuntimeException("croupier has already been setup - do not call this multiple times(for each croupier instance)");
        }
        context = setContext;

        registerNetworkMsg(promiseRequestAlias, promiseResponseAlias, extensionAlias, leaseCommitRequestAlias, leaseCommitResponseAlias);
        registerOthers();

        checkSetup();
    }


    private static void registerNetworkMsg(byte promiseRequestAlias, byte promiseResponseAlias, byte extensionAlias, byte leaseCommitRequestAlias, byte leaseCommitResponseAlias){

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

            context.registerAlias(DirectMsgNetty.Request.class, LEASE_COMMIT_REQUEST_ALIAS, leaseCommitRequestAlias);
            context.registerAlias(DirectMsgNetty.Response.class, LEASE_COMMIT_RESPONSE_ALIAS, leaseCommitResponseAlias);

            context.registerSerializer(LeaseCommitMessageUpdated.Request.class, new LeaseCommitMessageUpdatedSerializer.Request());
            context.registerSerializer(LeaseCommitMessageUpdated.Response.class, new LeaseCommitMessageUpdatedSerializer.Response());

            context.multiplexAlias(LEASE_COMMIT_REQUEST_ALIAS, LeaseCommitMessageUpdated.Request.class, (byte)0x01);
            context.multiplexAlias(LEASE_COMMIT_RESPONSE_ALIAS, LeaseCommitMessageUpdated.Response.class, (byte)0x01);

        } catch (SerializationContext.DuplicateException e) {
            e.printStackTrace();
        } catch (SerializationContext.MissingException e) {
            e.printStackTrace();
        }

    }


    private static void registerOthers(){

        try{
            context.registerSerializer(PublicKey.class, new PublicKeySerializer());
            context.registerSerializer(Promise.Request.class, new PromiseSerializer.Request());
            context.registerSerializer(Promise.Response.class, new PromiseSerializer.Response());
            
            context.registerSerializer(LeaseCommitUpdated.Request.class, new LeaseCommitUpdatedSerializer.Request());
            context.registerSerializer(LeaseCommitUpdated.Response.class, new LeaseCommitUpdatedSerializer.Response());
            
            context.registerSerializer(ExtensionRequest.class, new ExtensionRequestSerializer());
            
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
