package se.sics.ms.ui;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.Self;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.ms.search.*;
import se.sics.peersearch.types.IndexEntry;
import se.sics.peersearch.types.SearchPattern;

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

    public UiComponent(){
        component = this;

        subscribe(uiComponentInitHandler, control);
        subscribe(searchResponseHandler, uiPort);
        subscribe(addIndexEntryUiResponseHandler, uiPort);
    }

    final Handler<UiComponentInit> uiComponentInitHandler = new Handler<UiComponentInit>() {
        @Override
        public void handle(UiComponentInit uiComponentInit) {
            self = uiComponentInit.getPeerSelf();


            if (!SystemTray.isSupported()) {
                throw new IllegalStateException("SystemTray is not supported");
            }


            trayUI = new TrayUI(createImage("search.png", "tray icon"),
                    component);
        }
    };

    final Handler<SearchResponse> searchResponseHandler = new Handler<SearchResponse>() {
        @Override
        public void handle(SearchResponse searchResponse) {
            ArrayList<IndexEntry> results = searchResponse.getResults();

            trayUI.showSearchResults(results.toArray(new IndexEntry[results.size()]));
        }
    };

    final Handler<AddIndexEntryUiResponse> addIndexEntryUiResponseHandler = new Handler<AddIndexEntryUiResponse>() {
        @Override
        public void handle(AddIndexEntryUiResponse addIndexEntryUiResponse) {
            // TODO implement ack parser for UI
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
        trigger(new SearchRequest(pattern), uiPort);
    }

    public void addIndexEntry(IndexEntry entry) {
        trigger(new AddIndexEntryUiRequest(entry), uiPort);
    }
}
