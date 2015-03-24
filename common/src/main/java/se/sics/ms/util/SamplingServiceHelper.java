package se.sics.ms.util;

import se.sics.p2ptoolbox.croupier.api.util.CroupierPeerView;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Helper Class for the Peer Sampling Service.
 * Created by babbar on 2015-03-24.
 */
public class SamplingServiceHelper {

    public static Collection<CroupierPeerView> createCroupierSampleCopy(Collection<CroupierPeerView> cpvCollection){

        List<CroupierPeerView> newCroupierSample = new ArrayList<CroupierPeerView>();
        for(CroupierPeerView cpv : cpvCollection){
            newCroupierSample.add(new CroupierPeerView(cpv.pv, cpv.src, cpv.getAge()));
        }

        return newCroupierSample;
    }

}
