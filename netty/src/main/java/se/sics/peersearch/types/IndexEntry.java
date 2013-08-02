package se.sics.peersearch.types;

import java.io.Serializable;
import java.security.PublicKey;
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

	private Long id;
	private String url;
	private String fileName;
	private long fileSize;
	private Date uploaded;
	private String language;
	private Category category;
	private String description;
	private String hash;
	private PublicKey leaderId;

	public Long getId() {
		return id;
	}

	public void setId(long indexId) {
		this.id = indexId;
	}

	public String getUrl() {
		return url;
	}

	public String getFileName() {
		return fileName;
	}

	public long getFileSize() {
		return fileSize;
	}

	public Date getUploaded() {
		return uploaded;
	}

	public String getLanguage() {
		return language;
	}

	public Category getCategory() {
		return category;
	}

	public String getDescription() {
		return description;
	}

	public String getHash() {
		return hash;
	}

	public PublicKey getLeaderId() {
		return leaderId;
	}

	public void setLeaderId(PublicKey leaderId) {
		this.leaderId = leaderId;
	}

    public void setUrl(String url) {
        this.url = url;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public void setUploaded(Date uploaded) {
        this.uploaded = uploaded;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public void setCategory(Category category) {
        this.category = category;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setHash(String hash) {
        this.hash = hash;
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
     * @param url
     * @param fileName
     * @param fileSize
     * @param uploaded
     * @param language
     * @param category
     * @param description
     * @param hash
     */
    public IndexEntry(String url, String fileName, long fileSize, Date uploaded, String language, Category category, String description, String hash) {
        this.url = url;
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.uploaded = uploaded;
        this.language = language;
        this.category = category;
        this.description = description;
        this.hash = hash;

        this.id = Long.MIN_VALUE;
        this.leaderId = null;
    }

    /**
     *
     * @param id
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
    public IndexEntry(long id, String url, String fileName, long fileSize, Date uploaded, String language, Category category, String description, String hash, PublicKey leaderId) {
        this.id = id;
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
     *
     * @param id
     */
    public IndexEntry(long id) {

        this.id = id;
        this.fileName = "";
    }

    /**
     * @param url
     * @param fileName
     * @param category
     * @param description
     * @param hash
     */
    public IndexEntry(String url, String fileName, Date uploaded, Category category, String language, String description,
                      String hash) {
        super();
        this.url = url;
        this.fileName = fileName;
        this.uploaded = uploaded;
        this.category = category;
        this.language = language;
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
                      String description, String hash, PublicKey leaderId) {
        super();
        this.id = indexId;
        this.url = url;
        this.fileName = fileName;
        this.category = category;
        this.description = description;
        this.hash = hash;
        this.leaderId = leaderId;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexEntry that = (IndexEntry) o;

        if (fileSize != that.fileSize) return false;
        if (category != that.category) return false;
        if (!description.equals(that.description)) return false;
        if (!fileName.equals(that.fileName)) return false;
        if (!hash.equals(that.hash)) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (!language.equals(that.language)) return false;
        if (leaderId != null ? !leaderId.equals(that.leaderId) : that.leaderId != null) return false;
        if (!uploaded.equals(that.uploaded)) return false;
        if (!url.equals(that.url)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + hash.hashCode();
        result = 31 * result + (leaderId != null ? leaderId.hashCode() : 0);
        return result;
    }
}
