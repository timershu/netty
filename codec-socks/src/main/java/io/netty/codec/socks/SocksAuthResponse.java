/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.codec.socks;

import io.netty.buffer.ByteBuf;

/**
 * An socks auth response.
 *
 * @see SocksAuthRequest
 * @see SocksAuthResponseDecoder
 */
public final class SocksAuthResponse extends SocksResponse {

    private final AuthStatus authStatus;

    /**
     *
     * @param authStatus
     * @throws NullPointerException
     */

    public SocksAuthResponse(AuthStatus authStatus) {
        super(SocksResponseType.AUTH);
        if (authStatus == null) {
            throw new NullPointerException("authStatus");
        }
        this.authStatus = authStatus;
    }

    /**
     * Returns the {@link AuthStatus} of this {@link SocksAuthResponse}
     *
     * @return The {@link AuthStatus} of this {@link SocksAuthResponse}
     */
    public AuthStatus getAuthStatus() {
        return authStatus;
    }

    @Override
    public void encodeAsByteBuf(ByteBuf byteBuf) {
        byteBuf.writeByte(getProtocolVersion().getByteValue());
        byteBuf.writeByte(authStatus.getByteValue());
    }
}