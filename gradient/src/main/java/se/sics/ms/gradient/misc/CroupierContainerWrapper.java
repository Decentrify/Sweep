package se.sics.ms.gradient.misc;

import se.sics.ktoolbox.gradient.util.GradientLocalView;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.other.Container;


/**
 * Wrapper enclosing the container and the corresponding overlayid.
 * information.
 * *
 * Created by babbarshaer on 2015-06-08.
 */
public class CroupierContainerWrapper {
    
    public final Container<KAddress, GradientLocalView> container;
    public final Identifier overlayId;

    public CroupierContainerWrapper(Container<KAddress, GradientLocalView> container, Identifier overlayId) {
        this.container = container;
        this.overlayId = overlayId;
    }
}
