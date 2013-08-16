package se.sics.ms.types;

public class IndexHash {
    private final Id id;
    private final String hash;

    public IndexHash(Id id, String hash) {
        this.id = id;
        this.hash = hash;
    }

    public IndexHash(IndexEntry indexEntry) {
        this(new Id(indexEntry.getId(), indexEntry.getLeaderId()), indexEntry.getHash());
    }

    public Id getId() {
        return id;
    }

    public String getHash() {
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexHash that = (IndexHash) o;

        if (hash != null ? !hash.equals(that.hash) : that.hash != null) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (hash != null ? hash.hashCode() : 0);
        return result;
    }
}
