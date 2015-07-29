package se.sics.ms.ports;

import se.sics.kompics.PortType;
import se.sics.ms.events.UiAddIndexEntryRequest;
import se.sics.ms.events.UiAddIndexEntryResponse;
import se.sics.ms.events.UiSearchRequest;
import se.sics.ms.events.UiSearchResponse;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/7/13
 * Time: 4:14 PM
 */
public class UiPort extends PortType {
    {

        positive(se.sics.ms.events.paginateAware.UiSearchRequest.class);
        positive(se.sics.ms.events.paginateAware.UiSearchResponse.class);

        negative(se.sics.ms.events.paginateAware.UiSearchRequest.class);
        negative(se.sics.ms.events.paginateAware.UiSearchResponse.class);

        positive(UiSearchResponse.class);
        negative(UiSearchResponse.class);
        negative(UiSearchRequest.class);
        positive(UiSearchRequest.class);
        positive(UiAddIndexEntryResponse.class);
        negative(UiAddIndexEntryRequest.class);
        negative(UiAddIndexEntryResponse.class);
        positive(UiAddIndexEntryRequest.class);
    }
}
