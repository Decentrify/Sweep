package se.sics.ms.timeout;

import se.sics.gvod.timer.SchedulePeriodicTimeout;
import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.Timeout;
import se.sics.kompics.ChannelFilter;

/**
 *
 * @author: Steffen Grohsschmiedt
 */

public abstract class IndividualTimeout  extends Timeout {

    private final int id;

    public IndividualTimeout(SchedulePeriodicTimeout request, int id) {
        super(request);
        this.id = id;
    }

    public IndividualTimeout(ScheduleTimeout request, int id) {
        super(request);
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static final class IndividualTimeoutFilter extends ChannelFilter<IndividualTimeout, Integer> {

        public IndividualTimeoutFilter(int id) {
            super(IndividualTimeout.class, id, true);
        }

        @Override
        public Integer getValue(IndividualTimeout event) {
            return event.getId();
        }
    }
}