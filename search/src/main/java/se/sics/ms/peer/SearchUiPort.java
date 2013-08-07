package se.sics.ms.peer;

import se.sics.kompics.Event;
import se.sics.kompics.PortType;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/7/13
 * Time: 4:22 PM
 */
public class SearchUiPort extends PortType{
    {
        negative(Event.class);
        positive(Event.class);
    }
}
