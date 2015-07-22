package se.sics.ms.types;

import com.google.common.io.BaseEncoding;
import org.apache.lucene.document.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.ms.configuration.MsConfig;
import se.sics.ms.util.ApplicationConst;

import java.io.Serializable;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Date;

/**
 * Representation of one entry in the search database.
 */
public class IndexEntry implements Serializable {

    public static final IndexEntry DEFAULT_ENTRY = new IndexEntry("globalid", ApplicationConst.LANDING_ENTRY_ID, "landing-entry", "none", 0, new Date(0), "none", MsConfig.Categories.Default, "none", "none", null);
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

    public IndexEntry(String globalId, String url, String fileName, long fileSize, Date uploaded, String language, MsConfig.Categories category, String description) {
        this(globalId, Long.MIN_VALUE, url, fileName, fileSize, uploaded, language, category, description, null, null);
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
        if (uploaded!= null ? !uploaded.equals(that.uploaded): that.uploaded != null) return false;
        if (!url.equals(that.url)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = globalId != null ? globalId.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (url != null ? url.hashCode() : 0);
        result = 31 * result + (fileName != null ? fileName.hashCode() : 0);
        result = 31 * result + (int) (fileSize ^ (fileSize >>> 32));
        result = 31 * result + (uploaded != null ? uploaded.hashCode() : 0);
        result = 31 * result + (language != null ? language.hashCode() : 0);
        result = 31 * result + (category != null ? category.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (hash != null ? hash.hashCode() : 0);
        result = 31 * result + (leaderId != null ? leaderId.hashCode() : 0);
        return result;
    }

    /**
     * Helper class for the index entry addition.
     */
    public static class IndexEntryHelper{
        
        private static Logger logger = LoggerFactory.getLogger(IndexEntryHelper.class);


        /**
         *
         * @param entry
         * @return
         */
        public static Document addIndexEntryToDocument(Document doc , IndexEntry entry){

            doc.add(new StringField(IndexEntry.GLOBAL_ID, entry.getGlobalId(), Field.Store.YES));
            doc.add(new LongField(IndexEntry.ID, entry.getId(), Field.Store.YES));
            doc.add(new StoredField(IndexEntry.URL, entry.getUrl()));
            doc.add(new TextField(IndexEntry.FILE_NAME, entry.getFileName(), Field.Store.YES));
            doc.add(new IntField(IndexEntry.CATEGORY, entry.getCategory().ordinal(), Field.Store.YES));
            doc.add(new TextField(IndexEntry.DESCRIPTION, entry.getDescription(), Field.Store.YES));
            doc.add(new StoredField(IndexEntry.HASH, entry.getHash()));
            doc.add(new LongField(IndexEntry.FILE_SIZE, entry.getFileSize(), Field.Store.YES));
            
            if (entry.getLeaderId() == null)
                doc.add(new StringField(IndexEntry.LEADER_ID, new String(), Field.Store.YES));
            else
                doc.add(new StringField(IndexEntry.LEADER_ID, BaseEncoding.base64().encode(entry.getLeaderId().getEncoded()), Field.Store.YES));

            if (entry.getUploaded() != null) {
                doc.add(new LongField(IndexEntry.UPLOADED, entry.getUploaded().getTime(), Field.Store.YES));
            }
            else {
                doc.add(new LongField(IndexEntry.UPLOADED, (new Date()).getTime(), Field.Store.YES));
            }

            if (entry.getLanguage() != null) {
                doc.add(new StringField(IndexEntry.LANGUAGE, entry.getLanguage(), Field.Store.YES));
            }
            else{
                doc.add(new StringField(IndexEntry.LANGUAGE, "english", Field.Store.YES));
            }

            return doc;
        }



        /**
         * Read the entries from the Lucene Document and create Index Entry.
         * @param d Lucene Document
         * @return Index Entry.
         */
        public static IndexEntry createIndexEntry(Document d){

            String leaderId = d.get(IndexEntry.LEADER_ID);
            if (leaderId.isEmpty())
                return createIndexEntryInternal(d, null);

            KeyFactory keyFactory;
            PublicKey pub = null;
            try {
                keyFactory = KeyFactory.getInstance("RSA");
                byte[] decode = BaseEncoding.base64().decode(leaderId);
                X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(decode);
                pub = keyFactory.generatePublic(publicKeySpec);
            } catch (NoSuchAlgorithmException e) {
                logger.error(e.getMessage());
            } catch (InvalidKeySpecException e) {
                logger.error(e.getMessage());
            }

            return createIndexEntryInternal(d, pub);
        }

        /**
         * Create Index Entry Internal.
         * @param d lucene document.
         * @param pub instance of public key.
         * @return IndexEntry.
         */
        public static IndexEntry createIndexEntryInternal(Document d, PublicKey pub) {


            String globalId = d.get(IndexEntry.GLOBAL_ID) != null ? d.get(IndexEntry.GLOBAL_ID) : "" ;
            long id = d.get(IndexEntry.ID) != null ? Long.valueOf(d.get(IndexEntry.ID)) : Long.MIN_VALUE;
            String url = d.get(IndexEntry.URL) != null ? d.get(IndexEntry.URL) : "";
            String fileName = d.get(IndexEntry.FILE_NAME) != null ? d.get(IndexEntry.FILE_NAME) : "";
            MsConfig.Categories category = d.get(IndexEntry.CATEGORY) != null ? MsConfig.Categories.values()[Integer.valueOf(d.get(IndexEntry.CATEGORY))] : MsConfig.Categories.Default;
            String description = d.get(IndexEntry.DESCRIPTION) != null ? d.get(IndexEntry.DESCRIPTION) : "";
            String hash = d.get(IndexEntry.HASH) != null ? d.get(IndexEntry.HASH) : "";
            long fileSize = d.get(IndexEntry.FILE_SIZE) != null ? Long.valueOf(d.get(IndexEntry.FILE_SIZE)) : 0;
            Date uploadedDate = d.get(IndexEntry.UPLOADED) != null ? new Date(Long.valueOf(d.get(IndexEntry.UPLOADED))) : new Date();
            String language = d.get(IndexEntry.LANGUAGE) != null ? d.get(IndexEntry.LANGUAGE) : "";

            return new IndexEntry(globalId, id, url, fileName, fileSize, uploadedDate, language, category, description, hash, pub);
        }

    }
    
    
}
