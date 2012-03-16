/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.nio;

import com.hazelcast.logging.ILogger;

import java.io.IOException;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;

public class SocketAcceptor implements Runnable {
    private final ServerSocketChannel serverSocketChannel;
    private final ConnectionManager connectionManager;
    private Selector selector;
    private final ILogger logger;

    public SocketAcceptor(ServerSocketChannel serverSocketChannel, ConnectionManager connectionManager) {
        this.serverSocketChannel = serverSocketChannel;
        this.connectionManager = connectionManager;
        this.logger = connectionManager.ioService.getLogger(this.getClass().getName());
    }

    public void run() {
        try {
            connectionManager.ioService.onIOThreadStart();
            selector = Selector.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            while (connectionManager.isLive()) {
                final int keyCount = selector.select(); // block until new connection or interrupt.
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                if (keyCount == 0) {
                    continue;
                }
                final Set<SelectionKey> setSelectedKeys = selector.selectedKeys();
                final Iterator<SelectionKey> it = setSelectedKeys.iterator();
                while (it.hasNext()) {
                    final SelectionKey sk = it.next();
                    it.remove();
                    if (sk.isValid() && sk.isAcceptable()) {  // of course it is acceptable!
                        acceptSocket();
                    }
                }
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            connectionManager.ioService.getSystemLogService()
                    .logConnection(e.getClass().getName() + ": " + e.getMessage());
        } finally {
            try {
                logger.log(Level.FINEST, "Closing selector " + Thread.currentThread().getName());
                selector.close();
            } catch (final Exception ignored) {
            }
        }
    }

    private void acceptSocket() {
        if (!connectionManager.isLive()) return;
        SocketChannelWrapper socketChannelWrapper = null;
        try {
            final SocketChannel socketChannel = serverSocketChannel.accept();
            if (socketChannel != null) {
                socketChannelWrapper = connectionManager.wrapSocketChannel(socketChannel, false);
            }
        } catch (Exception e) {
            if (e instanceof ClosedChannelException && !connectionManager.isLive()) {
                // ClosedChannelException
                // or AsynchronousCloseException
                // or ClosedByInterruptException
                logger.log(Level.FINEST, "Terminating socket acceptor thread...", e);
            } else {
                logger.log(Level.WARNING, "Unexpected error while accepting connection!", e);
                try {
                    serverSocketChannel.close();
                } catch (Exception ignore) {
                }
                connectionManager.ioService.onFatalError(e);
            }
        }
        if (socketChannelWrapper != null) {
            final SocketChannelWrapper socketChannel = socketChannelWrapper;
            connectionManager.executeAsync(new Runnable() {
                public void run() {
                    String message = socketChannel.socket().getLocalPort()
                            + " is accepting socket connection from "
                            + socketChannel.socket().getRemoteSocketAddress();
                    logger.log(Level.INFO, message);
                    connectionManager.ioService.getSystemLogService().logConnection(message);
                    try {
                        MemberSocketInterceptor memberSocketInterceptor = connectionManager.getMemberSocketInterceptor();
                        if (memberSocketInterceptor != null) {
                            memberSocketInterceptor.onAccept(socketChannel.socket());
                        }
                        socketChannel.configureBlocking(false);
                        connectionManager.initSocket(socketChannel.socket());
                        connectionManager.assignSocketChannel(socketChannel);
                    } catch (Exception e) {
                        logger.log(Level.WARNING, e.getMessage(), e);
                        connectionManager.ioService.getSystemLogService().logConnection(e.getMessage());
                        if (socketChannel != null) {
                            try {
                                socketChannel.close();
                            } catch (IOException ignored) {
                            }
                        }
                    }
                }
            });
        }
    }

}
