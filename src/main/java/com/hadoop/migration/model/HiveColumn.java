package com.hadoop.migration.model;

public class HiveColumn {
    private String name;
    private String type;
    private String comment;
    private int position;
    private boolean partition;

    public HiveColumn() {}

    private HiveColumn(Builder builder) {
        this.name = builder.name;
        this.type = builder.type;
        this.comment = builder.comment;
        this.position = builder.position;
        this.partition = builder.partition;
    }

    public static Builder builder() { return new Builder(); }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getComment() { return comment; }
    public void setComment(String comment) { this.comment = comment; }

    public int getPosition() { return position; }
    public void setPosition(int position) { this.position = position; }

    public boolean isPartition() { return partition; }
    public void setPartition(boolean partition) { this.partition = partition; }

    public static class Builder {
        private String name;
        private String type;
        private String comment;
        private int position;
        private boolean partition;

        public Builder name(String name) { this.name = name; return this; }
        public Builder type(String type) { this.type = type; return this; }
        public Builder comment(String comment) { this.comment = comment; return this; }
        public Builder position(int position) { this.position = position; return this; }
        public Builder isPartition(boolean partition) { this.partition = partition; return this; }

        public HiveColumn build() { return new HiveColumn(this); }
    }
}