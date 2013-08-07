package se.sics.ms.main;

import se.sics.gvod.common.Self;
import se.sics.kompics.Init;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/7/13
 * Time: 12:41 PM
 */
public class UiComponentInit extends Init {
    private final Self peerSelf;

    public UiComponentInit(Self peerSelf) {
        this.peerSelf = peerSelf;
    }

    public Self getPeerSelf() {
        return peerSelf;
    }
}
