package xyz.cjcj.whatsappmerge.jdbc;

public class Table {
    private String name;
    private String idColumn;
    private String idColumnType;
    private String idPostFix;
    private String diffColumn;

    public Table(String name, String idColumn, String idColumnType, String idPostFix, String diffColumn) {
        this.name = name;
        this.idColumn = idColumn;
        this.idColumnType = idColumnType;
        this.idPostFix = idPostFix;
        this.diffColumn = diffColumn;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIdColumn() {
        return idColumn;
    }

    public void setIdColumn(String idColumn) {
        this.idColumn = idColumn;
    }

    public String getIdColumnType() {
        return idColumnType;
    }

    public void setIdColumnType(String idColumnType) {
        this.idColumnType = idColumnType;
    }

    public String getIdPostFix() {
        return idPostFix;
    }

    public void setIdPostFix(String idPostFix) {
        this.idPostFix = idPostFix;
    }

    public String getDiffColumn() {
        return diffColumn;
    }

    public void setDiffColumn(String diffColumn) {
        this.diffColumn = diffColumn;
    }
}
