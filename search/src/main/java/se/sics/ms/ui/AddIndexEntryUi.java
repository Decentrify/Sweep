package se.sics.ms.ui;

import se.sics.peersearch.types.IndexEntry;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 8/8/13
 * Time: 4:43 PM
 */
public class AddIndexEntryUi {
    private final UiComponent component;

    public JPanel root;
    private JTextField urlField;
    private JTextField fileNameField;
    private JTextField fileSizeField;
    private JTextField languageField;
    private JComboBox categoryBox = new JComboBox(IndexEntry.Category.values());
    private JTextArea descriptionTextArea;
    private JButton addIndexEntryButton;

    public AddIndexEntryUi(final UiComponent component) {
        this.component = component;

//        categoryBox = new JComboBox(IndexEntry.Category.values());

        addIndexEntryButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                if(urlField.getText() == null) return;
                if(fileNameField.getText() == null) return;
                if(fileSizeField.getText() == null) return;
                if(languageField.getText() == null) return;
                //if(categoryBox.getSelectedItem() == null) return;
                if(descriptionTextArea.getText() == null) return;

                IndexEntry entry = new IndexEntry(urlField.getText(), fileNameField.getText(), Long.parseLong(fileSizeField.getText()),
                        new Date(), languageField.getText(), IndexEntry.Category.Books, descriptionTextArea.getText());

                component.addIndexEntry(entry);
            }
        });
    }

    public static void main(String[] args) {
        JFrame frame = new JFrame("AddIndexEntryUi");
        frame.setContentPane(new AddIndexEntryUi(null).root);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }
}
