package se.sics.ms.ui;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.Self;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Start;
import se.sics.ms.events.UiAddIndexEntryRequest;
import se.sics.ms.events.UiAddIndexEntryResponse;
import se.sics.ms.events.UiSearchRequest;
import se.sics.ms.events.UiSearchResponse;
import se.sics.ms.ports.UiPort;
import se.sics.ms.types.IndexEntry;
import se.sics.ms.types.SearchPattern;

import javax.swing.*;
import java.awt.*;
import java.net.URL;
import java.util.ArrayList;


/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/7/13
 * Time: 12:35 PM
 */
public class UiComponent extends ComponentDefinition {
    private Logger logger = LoggerFactory.getLogger(UiComponent.class);
    Negative<UiPort> uiPort = negative(UiPort.class);

    private Self self;
    private UiComponent component;
    private TrayUI trayUI;

    public UiComponent(UiComponentInit init){
        component = this;
        doInit(init);
    }

    private void doInit(UiComponentInit init) {
        subscribe(startHandler, control);
        subscribe(searchResponseHandler, uiPort);
        subscribe(addIndexEntryUiResponseHandler, uiPort);

        self = init.getPeerSelf();
    }

    final Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start startInit) {
            if (!SystemTray.isSupported()) {
                throw new IllegalStateException("SystemTray is not supported");
            }

            trayUI = new TrayUI(createImage("search.png", "tray icon"),
                    component);
        }
    };

    /**
     * Handles results from the Search component
     */
    final Handler<UiSearchResponse> searchResponseHandler = new Handler<UiSearchResponse>() {
        @Override
        public void handle(UiSearchResponse searchResponse) {
            ArrayList<IndexEntry> results = searchResponse.getResults();

            trayUI.showSearchResults(results.toArray(new IndexEntry[results.size()]));
        }
    };

    /**
     * Handles results of adding to the index
     */
    final Handler<UiAddIndexEntryResponse> addIndexEntryUiResponseHandler = new Handler<UiAddIndexEntryResponse>() {
        @Override
        public void handle(UiAddIndexEntryResponse addIndexEntryUiResponse) {
            boolean result = addIndexEntryUiResponse.isSuccessful();
            trayUI.showAddResult(result);
        }
    };

    protected static Image createImage(String path, String description) {
        URL imageURL = UiComponent.class.getResource(path);

        if (imageURL == null) {
            System.err.println("Resource not found: " + path);
            return null;
        } else {
            return (new ImageIcon(imageURL, description)).getImage();
        }
    }

    public void search(SearchPattern pattern) {
        trigger(new UiSearchRequest(pattern), uiPort);
    }

    public void addIndexEntry(IndexEntry entry) {
        trigger(new UiAddIndexEntryRequest(entry), uiPort);
    }
}
