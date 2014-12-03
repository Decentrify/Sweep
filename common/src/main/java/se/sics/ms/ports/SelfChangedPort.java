package se.sics.ms.ports;

import se.sics.kompics.Event;
import se.sics.kompics.PortType;
import se.sics.ms.common.MsSelfImpl;

/**
 * Created by saira on 12/4/14.
 */
public class SelfChangedPort extends PortType {
    {
        positive(SelfChangedEvent.class);
    }

    public static class SelfChangedEvent extends Event {

        private MsSelfImpl self;

        public SelfChangedEvent(MsSelfImpl self) {
            this.self = self;
        }

        public MsSelfImpl getSelf() {
            return self;
        }
    }
}
