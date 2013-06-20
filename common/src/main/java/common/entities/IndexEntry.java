package common.entities;

import java.io.Serializable;

/**
 * Representation of one entry in the search database.
 */
public class IndexEntry implements Serializable {
	private static final long serialVersionUID = 5710212524130900513L;
	private String title;
	private int id;
	private String magneticLink;

	/**
	 * Creates a new IndexEntry.
	 */
	public IndexEntry() {
	}

	/**
	 * Creates a new title with the given title.
	 * 
	 * @param title
	 *            the text describing the content
	 */
	public IndexEntry(String title) {
		super();
		this.title = title;
		this.magneticLink = "no magnetic link";
	}

	/**
	 * Creates a new title with the given title and magnetic link.
	 * 
	 * @param title
	 *            the text describing the content
	 * @param magneticLink
	 *            the magnetic link of the index entry
	 */
	public IndexEntry(String title, String magneticLink) {
		super();
		this.title = title;
		this.magneticLink = magneticLink;
	}

	/**
	 * Creates a new title with the given title, magnetic link and id.
	 * 
	 * @param title
	 *            the text describing the content
	 * @param magneticLink
	 *            the magnetic link of the index entry
	 * @param id
	 *            the internal id in the search index
	 */
	public IndexEntry(String title, String magneticLink, int id) {
		super();
		this.title = title;
		this.magneticLink = magneticLink;
		this.id = id;
	}

	/**
	 * @param title
	 *            the text describing the content
	 */
	public void setTitle(String title) {
		this.title = title;
	}

	/**
	 * @return the text describing the content
	 */
	public String getTitle() {
		return title;
	}

	/**
	 * @return the internal id in the search index
	 */
	public int getId() {
		return id;
	}

	/**
	 * @param id
	 *            the internal id in the search index
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * @return the magnetic link of the index entry
	 */
	public String getMagneticLink() {
		return magneticLink;
	}

	/**
	 * @param magnet
	 *            the magnetic link of the index entry
	 */
	public void setMagneticLink(String magnet) {
		this.magneticLink = magnet;
	}

	@Override
	public String toString() {
		return "IndexEntry [title=" + title + ", id=" + id + ", magneticLink=" + magneticLink + "]";
	}
}
