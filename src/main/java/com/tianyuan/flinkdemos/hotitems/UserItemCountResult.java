package com.tianyuan.flinkdemos.hotitems;

public class UserItemCountResult {
    public long itemId;     // 商品ID
    public long windowEnd;  // 窗口结束时间戳
    public long viewCount;  // 商品的点击量

    public UserItemCountResult(long itemId, long windowEnd, long viewCount) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.viewCount = viewCount;
    }

    public UserItemCountResult() {
    }

    @Override
    public String toString() {
        return "UserItemCountResult{" +
                "itemId=" + itemId +
                ", windowEnd=" + windowEnd +
                ", viewCount=" + viewCount +
                '}';
    }
}
