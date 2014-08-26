package se.sics.ms.types;

import se.sics.ms.configuration.MsConfig;

import java.io.Serializable;
import java.security.PublicKey;
import java.util.Date;

/**
 * Representation of one entry in the search database.
 */
public class IndexEntry implements Serializable {
	private static final long serialVersionUID = -1043774025075199568L;

    public static final String GLOBAL_ID = "gid";
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

    private String globalId;
	private Long id;
	private String url;
	private String fileName;
	private long fileSize;
	private Date uploaded;
	private String language;
	private MsConfig.Categories category;
	private String description;
	private String hash;
	private PublicKey leaderId;

    public String getGlobalId() { return globalId; }

    public void setGlobalId(String globalId) { this.globalId = globalId; }

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

	public MsConfig.Categories getCategory() {
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

    public void setCategory(MsConfig.Categories category) {
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
		return "IndexEntry [indexId=" + id + ",globalId=" + globalId + ", url=" + url + ", fileName=" + fileName
				+ ", fileSize=" + fileSize + ", uploaded=" + uploaded + ", language=" + language
				+ ", category=" + category + ", description=" + description + ", hash=" + hash
				+ ", leaderId=" + leaderId + "]";
	}

    public IndexEntry(String globalId, long id, String url, String fileName, long fileSize, Date uploaded,
                      String language, MsConfig.Categories category, String description, String hash,
                      PublicKey leaderId) {

        this.url = url;
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.uploaded = uploaded;
        this.language = language;
        this.category = category;
        this.description = description;
        this.globalId = globalId;
        this.id = id;
        this.hash = hash;
        this.leaderId = leaderId;
    }

    public IndexEntry(String url, String fileName, long fileSize, Date uploaded, String language, MsConfig.Categories category, String description) {

        this(null, Long.MIN_VALUE, url, fileName, fileSize, uploaded, language, category, description, null, null);
    }

    public IndexEntry(String url, String fileName, long fileSize, Date uploaded, String language,
                      MsConfig.Categories category, String description, String globalId) {

        this(globalId, Long.MIN_VALUE, url, fileName, fileSize, uploaded, language, category, description, null, null);
    }

    public IndexEntry(String url, String fileName, Date uploaded, MsConfig.Categories category, String language,
                      String description, String hash) {

        this(null, Long.MIN_VALUE, url, fileName, 0, uploaded, language, category, description, hash, null);
    }

    public IndexEntry(String globalId, long indexId, String url, String fileName, MsConfig.Categories category,
                      String description, String hash, PublicKey leaderId) {

        this(globalId, indexId, url, fileName, 0, null, "", category, description, hash, leaderId);
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
        if (hash != null ? !hash.equals(that.hash) : that.hash != null) return false;
        if (globalId != null ? !globalId.equals(that.globalId) : that.globalId != null) return false;
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
