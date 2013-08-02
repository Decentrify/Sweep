package se.sics.ms.gradient;

import se.sics.gvod.common.Self;
import se.sics.kompics.Event;
import java.security.PublicKey;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/2/13
 * Time: 2:15 PM
 */
public class PublicKeyBroadcast extends Event {
    private final PublicKey publicKey;

    public PublicKeyBroadcast(PublicKey publicKey) {
        super();

        this.publicKey = publicKey;
    }

    public PublicKey getPublicKey() {
        return publicKey;
    }
}
