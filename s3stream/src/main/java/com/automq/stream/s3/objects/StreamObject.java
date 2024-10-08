/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.objects;


public class StreamObject {
    private long objectId;
    private long objectSize;
    private long streamId;
    private long startOffset;
    private long endOffset;
    private int attributes = ObjectAttributes.UNSET.attributes();

    public long getObjectId() {
        return objectId;
    }

    public void setObjectId(long objectId) {
        this.objectId = objectId;
    }

    public long getObjectSize() {
        return objectSize;
    }

    public void setObjectSize(long objectSize) {
        this.objectSize = objectSize;
    }

    public long getStreamId() {
        return streamId;
    }

    public void setStreamId(long streamId) {
        this.streamId = streamId;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    public int getAttributes() {
        return attributes;
    }

    public void setAttributes(int attributes) {
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "so{" +
            "oi=" + objectId +
            ", size=" + objectSize +
            ", si=" + streamId +
            ", so=" + startOffset +
            ", eo=" + endOffset +
            ", attr=" + attributes +
            '}';
    }
}
