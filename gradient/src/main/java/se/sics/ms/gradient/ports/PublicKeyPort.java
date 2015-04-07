package se.sics.ms.gradient.ports;

import se.sics.kompics.PortType;
import se.sics.ms.gradient.events.PublicKeyBroadcast;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/2/13
 * Time: 2:09 PM
 */
public class PublicKeyPort extends PortType {{
        negative(PublicKeyBroadcast.class);
        positive(PublicKeyBroadcast.class);
    }}
