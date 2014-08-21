/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.ms.webservicemodel;

import com.fasterxml.jackson.annotation.JsonProperty;
import se.sics.ms.configuration.MsConfig;

import java.util.Date;

/**
 *
 * @author alidar
 */
public class SearchIndexResultJSON {
    private String globalId;
    private String url;
    private String fileName;
    private long fileSize;
    private Date uploaded;
    private String language;
    private MsConfig.Categories category;
    private String description;

    public SearchIndexResultJSON(String globalId, String url, String fileName, long fileSize, Date uploaded, String language, MsConfig.Categories category, String description) {
        this.globalId = globalId;
        this.url = url;
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.uploaded = uploaded;
        this.language = language;
        this.category = category;
        this.description = description;
    }

    /**
     * @return the globalId
     */
    public String getGlobalId() { return globalId; };

    /**
     * @param globalId the globalId to set
     */
    public void setGlobalId(String globalId) { this.globalId = globalId; }

    /**
     * @return the url
     */
    @JsonProperty
    public String getUrl() {
        return url;
    }

    /**
     * @param url the url to set
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * @return the fileName
     */
    @JsonProperty
    public String getFileName() {
        return fileName;
    }

    /**
     * @param fileName the fileName to set
     */
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    /**
     * @return the fileSize
     */
    @JsonProperty
    public long getFileSize() {
        return fileSize;
    }

    /**
     * @param fileSize the fileSize to set
     */
    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    /**
     * @return the uploaded
     */
    @JsonProperty
    public Date getUploaded() {
        return uploaded;
    }

    /**
     * @param uploaded the uploaded to set
     */
    public void setUploaded(Date uploaded) {
        this.uploaded = uploaded;
    }

    /**
     * @return the language
     */
    @JsonProperty
    public String getLanguage() {
        return language;
    }

    /**
     * @param language the language to set
     */
    public void setLanguage(String language) {
        this.language = language;
    }

    /**
     * @return the category
     */
    @JsonProperty
    public MsConfig.Categories getCategory() {
        return category;
    }

    /**
     * @param category the category to set
     */
    public void setCategory(MsConfig.Categories category) {
        this.category = category;
    }

    /**
     * @return the description
     */
    @JsonProperty
    public String getDescription() {
        return description;
    }

    /**
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
    }
}