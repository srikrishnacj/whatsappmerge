package xyz.cjcj.whatsappmerge.jdbc;

public interface PostOffsetIdCallback {
    void postOffset(String originalId, String offsetId);
}
