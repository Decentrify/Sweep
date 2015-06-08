package se.sics.ms.gradient.misc;

import se.sics.p2ptoolbox.croupier.util.CroupierContainer;
import se.sics.p2ptoolbox.gradient.util.GradientLocalView;
import se.sics.p2ptoolbox.util.Container;
import se.sics.p2ptoolbox.util.network.impl.DecoratedAddress;

/**
 * Wrapper enclosing the container and the corresponding overlayid.
 * information.
 * *
 * Created by babbarshaer on 2015-06-08.
 */
public class CroupierContainerWrapper {
    
    
    public final Container<DecoratedAddress, GradientLocalView> container;
    public final int overlayId;

    public CroupierContainerWrapper(Container<DecoratedAddress, GradientLocalView> container, int overlayId) {
        this.container = container;
        this.overlayId = overlayId;
    }
}
