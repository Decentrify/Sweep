package search.simulator.core;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import se.sics.peersearch.data.types.IndexEntry;

/**
 * Provides functionality to access the entries of an xml file with magnet links
 * one by one.
 */
public class MagnetFileIterator {
	private BufferedReader br;
	private MagnetParser parser;

	/**
	 * Creates a new instance for the file at the given path.
	 * 
	 * @param path
	 *            the path of the xml file to parse
	 * @throws FileNotFoundException
	 *             if the file at the given path does not exist
	 * @throws ParserConfigurationException
	 *             {@link MagnetParser#MagneticParser()}
	 * @throws SAXException
	 *             {@link MagnetParser#MagneticParser()}
	 */
	public MagnetFileIterator(String path) throws FileNotFoundException,
			ParserConfigurationException, SAXException {
		this.parser = new MagnetParser();
		this.br = new BufferedReader(new FileReader(path));
	}

	/**
	 * Read the next entry in the supplied file.
	 * 
	 * @return an {@link IndexEntry} representing the next entry in the parsed
	 *         file or null if the end of the file was reached
	 * @throws IOException
	 *             {@link BufferedReader#readLine()}
	 * @throws SAXException
	 *             {@link MagnetParser#parse(String)}
	 */
	public IndexEntry next() throws IOException, SAXException {
		String xmlEntry = "";
		String line;

		while ((line = br.readLine()) != null) {
			xmlEntry += line;
			if (line.equalsIgnoreCase("</torrent>")) {
				break;
			}
		}

		if (line == null) {
			return null;
		}

		return parser.parse(xmlEntry);
	}
}
