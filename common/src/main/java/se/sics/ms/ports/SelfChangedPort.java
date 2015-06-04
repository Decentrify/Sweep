package se.sics.ms.ports;

import se.sics.kompics.Event;
import se.sics.kompics.PortType;
import se.sics.ms.common.ApplicationSelf;

/**
 * Port containing event about the updated self information.
 * Created by saira on 12/4/14.
 */
public class SelfChangedPort extends PortType {
    {
        positive(SelfChangedEvent.class);
    }

    public static class SelfChangedEvent extends Event {

        private ApplicationSelf self;

        public SelfChangedEvent(ApplicationSelf self) {
            this.self = self;
        }

        public ApplicationSelf getSelf() {
            return self;
        }
    }
}
