package se.sics.ms.types;

import java.security.PublicKey;

public class Id {
    private long id;
    private PublicKey leaderId;

    public Id(long id, PublicKey leaderId) {
        this.id = id;
        this.leaderId = leaderId;
    }

    public long getId() {
        return id;
    }

    public PublicKey getLeaderId() {
        return leaderId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Id id1 = (Id) o;

        if (id != id1.id) return false;
        if (leaderId != null ? !leaderId.equals(id1.leaderId) : id1.leaderId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (leaderId != null ? leaderId.hashCode() : 0);
        return result;
    }
}
