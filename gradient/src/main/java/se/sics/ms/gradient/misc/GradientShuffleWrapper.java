package se.sics.ms.gradient.misc;

import se.sics.p2ptoolbox.gradient.msg.GradientShuffle;
import se.sics.p2ptoolbox.util.network.impl.DecoratedHeader;

/**
 * Wrapper class for the Gradient shuffle request.
 *
 * Created by babbarshaer on 2015-06-08.
 */
public class GradientShuffleWrapper {
    
    
    public final GradientShuffle.Request content;
    public final DecoratedHeader header;


    public GradientShuffleWrapper(GradientShuffle.Request content, DecoratedHeader header) {
        
        this.content = content;
        this.header = header;
    }
}
