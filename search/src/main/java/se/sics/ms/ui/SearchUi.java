package se.sics.ms.ui;

import se.sics.ms.ui.UiComponent;
import se.sics.peersearch.types.IndexEntry;
import se.sics.peersearch.types.SearchPattern;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/6/13
 * Time: 4:57 PM
 */
public class SearchUi {
    private final UiComponent component;

    private static DefaultListModel model = new DefaultListModel();

    public SearchUi(final UiComponent component) {
        this.component = component;
        searchResultsList.setModel(model);

//        IndexEntry entry0 = new IndexEntry("http://hp.com/", "Harry Potter 1", new Date(), IndexEntry.Category.Books, "English", null, null);
//        IndexEntry entry1 = new IndexEntry("http://hp.com/", "Harry Potter 2", new Date(), IndexEntry.Category.Books, "English", null, null);
//        IndexEntry entry2 = new IndexEntry("http://hp.com/", "Harry Potter 3", new Date(), IndexEntry.Category.Books, "English", null, null);
//        IndexEntry entry3 = new IndexEntry("http://hp.com/", "Harry Potter 4", new Date(), IndexEntry.Category.Books, "English", null, null);
//        IndexEntry entry4 = new IndexEntry("http://hp.com/", "Harry Potter 5", new Date(), IndexEntry.Category.Books, "English", null, null);
//        IndexEntry entry5 = new IndexEntry("http://hp.com/", "Harry Potter 6", new Date(), IndexEntry.Category.Books, "English", null, null);
//        IndexEntry entry6 = new IndexEntry("http://hp.com/", "Harry Potter 7", new Date(), IndexEntry.Category.Books, "English", null, null);
//        showSearchResults(new IndexEntry[]{entry0, entry1, entry2, entry3, entry4, entry5, entry6});

        searchButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                String title = searchField.getText();
                SearchPattern pattern = new SearchPattern(title);
                component.search(pattern);
            }
        });
    }

    public JPanel root;
    private JCheckBox videoCheckBox;
    private JCheckBox musicCheckBox;
    private JCheckBox booksCheckBox;
    private JButton searchButton;
    private JTextField searchField;
    private JList searchResultsList;

    public void showSearchResults(final IndexEntry[] results) {
        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                model.removeAllElements();
                for (IndexEntry entry : results)
                    model.addElement(indexEntryToString(entry));
                root.updateUI();
            }
        });
    }

    private String indexEntryToString(IndexEntry entry) {
        StringBuilder sb = new StringBuilder(entry.getFileName());
        sb.append(", ");
        sb.append(entry.getCategory());
        sb.append(", ");
        sb.append(entry.getLanguage());
        sb.append(", ");
        sb.append(entry.getFileSize());
        sb.append(", ");
        sb.append(entry.getUrl());
        sb.append(", ");
        sb.append(entry.getUploaded());

        return sb.toString();
    }
}
