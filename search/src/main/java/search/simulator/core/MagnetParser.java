package search.simulator.core;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import common.entities.IndexEntry;

/**
 * Parser for an xml string describing a magnetic link. Some xml files entries
 * need to be prepared to be read by this parser because they include invalid
 * characters.
 */
public class MagnetParser {
	private SAXParser saxParser;
	private IndexEntry entry = new IndexEntry();

	/**
	 * Handler implementing the callback functions triggered by the parser.
	 */
	private DefaultHandler handler = new DefaultHandler() {
		boolean title = false;
		boolean magnet = false;

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String,
		 * java.lang.String, java.lang.String, org.xml.sax.Attributes)
		 */
		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes)
				throws SAXException {

			if (qName.equalsIgnoreCase("title")) {
				title = true;
			}

			if (qName.equalsIgnoreCase("magnet")) {
				magnet = true;
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
		 */
		@Override
		public void characters(char[] ch, int start, int length) throws SAXException {
			if (title) {
				entry.setTitle(new String(ch, start, length));
				title = false;
			}

			if (magnet) {
				entry.setMagneticLink(new String(ch, start, length));
				magnet = false;
			}
		}
	};

	/**
	 * Creates a new instance.
	 * 
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 */
	public MagnetParser() throws ParserConfigurationException, SAXException {
		SAXParserFactory factory = SAXParserFactory.newInstance();
		saxParser = factory.newSAXParser();
	}

	/**
	 * Create an {@link IndexEntry} representing the given xml string.
	 * 
	 * @param entry
	 *            xml string describing a magnetic link
	 * @return an {@link IndexEntry} representing the given xml string
	 * @throws SAXException
	 * @throws IOException
	 */
	public IndexEntry parse(String entry) throws SAXException, IOException {
		saxParser.parse(new InputSource(new StringReader(entry)), handler);
		return this.entry;
	}
}
