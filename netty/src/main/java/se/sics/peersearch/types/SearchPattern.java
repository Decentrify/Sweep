package se.sics.peersearch.types;

import java.io.Serializable;
import java.util.Date;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/**
 * Created with IntelliJ IDEA.
 * User: kazarindn
 * Date: 7/5/13
 * Time: 3:57 PM
 */
public class SearchPattern implements Serializable {
    private static final long serialVersionUID = -8499646226474655358L;

    private final String fileNamePattern;
    private final int minFileSize;
    private final int maxFileSize;
    private final Date minUploadDate;
    private final Date maxUploadDate;
    private final String language;
    private final IndexEntry.Category category;
    private final String descriptionPattern;

    /**
     * Creates a new search pattern. Fields can be set to null or 0 in case of
     * sizes to be ignored.
     *
     * @param fileNamePattern
     *            a string with keywords to match the file name
     * @param minFileSize
     *            the minimum file size
     * @param maxFileSize
     *            the maximum file size
     * @param minUploadDate
     *            the minimum upload date
     * @param maxUploadDate
     *            the maximum upload date
     * @param language
     *            a string representing the language
     * @param category
     *            the category
     * @param descriptionPattern
     *            keywords to match the description text
     */
    public SearchPattern(String fileNamePattern, int minFileSize, int maxFileSize,
                         Date minUploadDate, Date maxUploadDate, String language, IndexEntry.Category category,
                         String descriptionPattern) {
        this.fileNamePattern = fileNamePattern;
        this.minFileSize = minFileSize;
        this.maxFileSize = maxFileSize;
        this.minUploadDate = minUploadDate;
        this.maxUploadDate = maxUploadDate;
        this.language = language;
        this.category = category;
        this.descriptionPattern = descriptionPattern;
    }

    /**
     * @return the file name pattern
     */
    public String getFileNamePattern() {
        return fileNamePattern;
    }

    /**
     * @return the minimum file size
     */
    public int getMinFileSize() {
        return minFileSize;
    }

    /**
     * @return the maximum file size
     */
    public int getMaxFileSize() {
        return maxFileSize;
    }

    /**
     * @return the minimum upload date
     */
    public Date getMinUploadDate() {
        return minUploadDate;
    }

    /**
     * @return the maximum upload date
     */
    public Date getMaxUploadDate() {
        return maxUploadDate;
    }

    /**
     * @return the language
     */
    public String getLanguage() {
        return language;
    }

    /**
     * @return the category
     */
    public IndexEntry.Category getCategory() {
        return category;
    }

    /**
     * @return the description pattern
     */
    public String getDescriptionPattern() {
        return descriptionPattern;
    }

    /**
     * Create a query from the search pattern.
     *
     * @return the query created from the search pattern
     */
    public Query getQuery() {
        BooleanQuery booleanQuery = new BooleanQuery();

        if (fileNamePattern != null) {
            Query query = new TermQuery(new Term(IndexEntry.FILE_NAME, fileNamePattern));
            booleanQuery.add(query, BooleanClause.Occur.MUST);
        }

        if (minFileSize >= 0 && maxFileSize > 0) {
            Query query = NumericRangeQuery.newIntRange(IndexEntry.FILE_SIZE, minFileSize,
                    maxFileSize, true, true);
            booleanQuery.add(query, BooleanClause.Occur.MUST);
        } else if (minFileSize > 0 && maxFileSize == 0) {
            Query query = NumericRangeQuery.newIntRange(IndexEntry.FILE_SIZE, minFileSize,
                    Integer.MAX_VALUE, true, true);
            booleanQuery.add(query, BooleanClause.Occur.MUST);
        }

        if (minUploadDate != null && maxUploadDate != null) {
            Query query = NumericRangeQuery.newLongRange(IndexEntry.UPLOADED,
                    minUploadDate.getTime(), maxUploadDate.getTime(), true, true);
            booleanQuery.add(query, BooleanClause.Occur.MUST);
        } else if (minUploadDate != null) {
            Query query = NumericRangeQuery.newLongRange(IndexEntry.UPLOADED,
                    minUploadDate.getTime(), Long.MAX_VALUE, true, true);
            booleanQuery.add(query, BooleanClause.Occur.MUST);
        } else if (maxUploadDate != null) {
            Query query = NumericRangeQuery.newLongRange(IndexEntry.UPLOADED, 0l,
                    maxUploadDate.getTime(), true, true);
            booleanQuery.add(query, BooleanClause.Occur.MUST);
        }

        if (language != null) {
            Query query = new TermQuery(new Term(IndexEntry.LANGUAGE, language));
            booleanQuery.add(query, BooleanClause.Occur.MUST);
        }

        if (category != null) {
            Query query = NumericRangeQuery.newIntRange(IndexEntry.CATEGORY, category.ordinal(),
                    category.ordinal(), true, true);
            booleanQuery.add(query, BooleanClause.Occur.MUST);
        }

        if (descriptionPattern != null) {
            Query query = new TermQuery(new Term(IndexEntry.DESCRIPTION, descriptionPattern));
            booleanQuery.add(query, BooleanClause.Occur.MUST);
        }

        return booleanQuery;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SearchPattern that = (SearchPattern) o;

        if (maxFileSize != that.maxFileSize) return false;
        if (minFileSize != that.minFileSize) return false;
        if (category != that.category) return false;
        if (descriptionPattern != null ? !descriptionPattern.equals(that.descriptionPattern) : that.descriptionPattern != null)
            return false;
        if (fileNamePattern != null ? !fileNamePattern.equals(that.fileNamePattern) : that.fileNamePattern != null)
            return false;
        if (language != null ? !language.equals(that.language) : that.language != null) return false;
        if (maxUploadDate != null ? !maxUploadDate.equals(that.maxUploadDate) : that.maxUploadDate != null)
            return false;
        if (minUploadDate != null ? !minUploadDate.equals(that.minUploadDate) : that.minUploadDate != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = fileNamePattern != null ? fileNamePattern.hashCode() : 0;
        result = 31 * result + minFileSize;
        result = 31 * result + maxFileSize;
        result = 31 * result + (minUploadDate != null ? minUploadDate.hashCode() : 0);
        result = 31 * result + (maxUploadDate != null ? maxUploadDate.hashCode() : 0);
        result = 31 * result + (language != null ? language.hashCode() : 0);
        result = 31 * result + (category != null ? category.hashCode() : 0);
        result = 31 * result + (descriptionPattern != null ? descriptionPattern.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SearchPattern{" +
                "fileNamePattern='" + fileNamePattern + '\'' +
                ", minFileSize=" + minFileSize +
                ", maxFileSize=" + maxFileSize +
                ", minUploadDate=" + minUploadDate +
                ", maxUploadDate=" + maxUploadDate +
                ", language='" + language + '\'' +
                ", category=" + category +
                ", descriptionPattern='" + descriptionPattern + '\'' +
                '}';
    }
}
