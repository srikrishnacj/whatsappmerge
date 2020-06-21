package xyz.cjcj.whatsappmerge.model;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RecordDE implements GenericRecord, Comparable {
    private Map<String, String> values;
    private int valuesHash = 0;

    private String id;

    public RecordDE(Map<String, String> values) {
        this.values = values;
        List temp = new LinkedList<>();
        temp.addAll(values.keySet());
        temp.addAll(values.values());
        valuesHash += Objects.hash(temp.toArray());
        this.id = values.get("_id");
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecordDE recordDE = (RecordDE) o;
        return id.equals(recordDE.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean hasConflict(GenericRecord o) {
        if (o == null || getClass() != o.getClass())
            throw new RuntimeException("Unknown record given to conflict detection");
        RecordDE recordDE = (RecordDE) o;
        return recordDE.valuesHash != valuesHash;
    }

    @Override
    public String toString() {
        return "MessageDE{" +
                "values=" + values +
                ", id='" + id + '\'' +
                '}';
    }

    @Override
    public int compareTo(Object o) {
        int intId = Integer.parseInt(id);
        int otherId = Integer.parseInt(((RecordDE) o).id);
        if (intId == otherId)
            return 0;
        else if (intId > otherId)
            return 1;
        else
            return -1;
    }
}
