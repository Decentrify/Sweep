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
    private JComboBox categoryBox;
    private JTextArea descriptionTextArea;
    private JButton addIndexEntryButton;

    public AddIndexEntryUi(final UiComponent component) {
        this.component = component;

        addIndexEntryButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                if(urlField.getText() == null) return;
                if(fileNameField.getText() == null) return;
                if(fileSizeField.getText() == null) return;
                if(languageField.getText() == null) return;
                if(descriptionTextArea.getText() == null) return;

                addIndexEntryButton.setEnabled(false);

                int selectedCategory = categoryBox.getSelectedIndex();

                IndexEntry entry = new IndexEntry(urlField.getText(), fileNameField.getText(), Long.parseLong(fileSizeField.getText()),
                        new Date(), languageField.getText(), IndexEntry.Category.values()[selectedCategory], descriptionTextArea.getText());

                component.addIndexEntry(entry);
            }
        });
    }

    public void showAddResult(boolean isSuccessful) {
        String result;
        if(isSuccessful)
            result = "Entry was successfully added";
        else
            result = "Failed to add an entry, try again";

        Object[] options = {"OK"};
        JOptionPane.showOptionDialog(root,
                result,"Result on Add Index Entry",
                JOptionPane.PLAIN_MESSAGE,
                JOptionPane.QUESTION_MESSAGE,
                null,
                options,
                options[0]);

        addIndexEntryButton.setEnabled(true);

    }

    public static void main(String[] args) {
        JFrame frame = new JFrame("AddIndexEntryUi");
        frame.setContentPane(new AddIndexEntryUi(null).root);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }
}
