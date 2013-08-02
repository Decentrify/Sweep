package se.sics.ms.gradient;

import se.sics.kompics.PortType;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/2/13
 * Time: 2:09 PM
 */
public class PublicKeyPort extends PortType {
    {
        negative(PublicKeyBroadcast.class);
        positive(PublicKeyBroadcast.class);
    }
}
