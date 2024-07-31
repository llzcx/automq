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
package com.automq.shell.model;

public enum EndpointProtocol {

    HTTP("http"),
    HTTPS("https");

    EndpointProtocol(String key) {
        this.name = key;
    }

    private final String name;

    public String getName() {
        return name;
    }

    public static EndpointProtocol getByName(String protocolName) {
        for (EndpointProtocol protocol : EndpointProtocol.values()) {
            if (protocol.getName().equals(protocolName)) {
                return protocol;
            }
        }
        throw new IllegalArgumentException("Invalid protocol: " + protocolName);
    }
}
