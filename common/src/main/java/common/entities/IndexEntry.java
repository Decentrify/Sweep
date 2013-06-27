package common.entities;

import java.io.Serializable;
import java.util.Date;

/**
 * Representation of one entry in the search database.
 */
public class IndexEntry implements Serializable {
	private static final long serialVersionUID = -1043774025075199568L;

	public static final String ID = "id";
	public static final String URL = "url";
	public static final String FILE_NAME = "file_name";
	public static final String FILE_SIZE = "file_size";
	public static final String UPLOADED = "uploaded";
	public static final String LANGUAGE = "language";
	public static final String CATEGORY = "category";
	public static final String DESCRIPTION = "description";
	public static final String HASH = "hash";
	public static final String LEADER_ID = "leader_id";

	public static enum Category {
		Video, Music, Books
	};

	private long id;
	private String url;
	private String fileName;
	private long fileSize;
	private Date uploaded;
	private String language;
	private Category category;
	private String description;
	private String hash;
	private String leaderId;

	public long getId() {
		return id;
	}

	public void setId(long indexId) {
		this.id = indexId;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public Date getUploaded() {
		return uploaded;
	}

	public void setUploaded(Date uploaded) {
		this.uploaded = uploaded;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public Category getCategory() {
		return category;
	}

	public void setCategory(Category category) {
		this.category = category;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getHash() {
		return hash;
	}

	public void setHash(String hash) {
		this.hash = hash;
	}

	public String getLeaderId() {
		return leaderId;
	}

	public void setLeaderId(String leaderId) {
		this.leaderId = leaderId;
	}

	@Override
	public String toString() {
		return "IndexEntry [indexId=" + id + ", url=" + url + ", fileName=" + fileName
				+ ", fileSize=" + fileSize + ", uploaded=" + uploaded + ", language=" + language
				+ ", category=" + category + ", description=" + description + ", hash=" + hash
				+ ", leaderId=" + leaderId + "]";
	}

	/**
	 * 
	 */
	public IndexEntry() {

	}

	/**
	 * @param url
	 * @param fileName
	 * @param category
	 * @param description
	 * @param hash
	 */
	public IndexEntry(String url, String fileName, Category category, String description,
			String hash) {
		super();
		this.url = url;
		this.fileName = fileName;
		this.category = category;
		this.description = description;
		this.hash = hash;
	}

	/**
	 * @param indexId
	 * @param url
	 * @param fileName
	 * @param category
	 * @param description
	 * @param hash
	 * @param leaderId
	 */
	public IndexEntry(long indexId, String url, String fileName, Category category,
			String description, String hash, String leaderId) {
		super();
		this.id = indexId;
		this.url = url;
		this.fileName = fileName;
		this.category = category;
		this.description = description;
		this.hash = hash;
		this.leaderId = leaderId;
	}

	/**
	 * @param url
	 * @param fileName
	 * @param fileSize
	 * @param uploaded
	 * @param language
	 * @param category
	 * @param description
	 * @param hash
	 */
	public IndexEntry(String url, String fileName, long fileSize, Date uploaded, String language,
			Category category, String description, String hash) {
		this.url = url;
		this.fileName = fileName;
		this.fileSize = fileSize;
		this.uploaded = uploaded;
		this.language = language;
		this.category = category;
		this.description = description;
		this.hash = hash;
	}

	/**
	 * @param indexId
	 * @param url
	 * @param fileName
	 * @param fileSize
	 * @param uploaded
	 * @param language
	 * @param category
	 * @param description
	 * @param hash
	 * @param leaderId
	 */
	public IndexEntry(long indexId, String url, String fileName, long fileSize, Date uploaded,
			String language, Category category, String description, String hash, String leaderId) {
		this.id = indexId;
		this.url = url;
		this.fileName = fileName;
		this.fileSize = fileSize;
		this.uploaded = uploaded;
		this.language = language;
		this.category = category;
		this.description = description;
		this.hash = hash;
		this.leaderId = leaderId;
	}

	/**
	 * Create a tombstone for the given indexId
	 * 
	 * @param indexId
	 *            the id of the entry
	 */
	public IndexEntry(long indexId) {
		this.id = indexId;
		this.fileName = "";
	}
}
