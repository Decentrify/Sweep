package se.sics.ms.search;

import se.sics.kompics.Event;
import se.sics.kompics.PortType;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/7/13
 * Time: 4:14 PM
 */
public class UiPort extends PortType {
    {
       positive(Event.class);
       negative(Event.class);
    }
}
