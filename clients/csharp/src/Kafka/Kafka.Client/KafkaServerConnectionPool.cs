/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Net;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;
using Kafka.Client.Exceptions;


namespace Kafka.Client
{
    public class KafkaServerConnectionPool
    {
        private ConcurrentQueue<KafkaConnection> availableConnections = null;
        private  int maxPoolSize;
        private  int socketCounter = 0;
        private  TimeSpan lifespan;
        private  string host;
        private  int port;
        private  int bufferSize;
        private  int socketTimeout;
        private  long idleTimeToKeepAlive;
        private  long keepAliveInterval;
        private  long socketPollingIntervalTimeout;
        private  SocketPollingLevel socketPollingLevel;

        public KafkaServerConnectionPool(int maxConnections, 
                                TimeSpan connectionLifespan,
                                string server, 
                                int port, 
                                int bufferSize, 
                                int socketTimeout, 
                                long idleTimeToKeepAlive, 
                                long keepAliveInterval, 
                                long socketPollingTimeout, 
                                SocketPollingLevel socketPollingLevel)                    
        {
            this.maxPoolSize = maxConnections;
            this.lifespan = connectionLifespan;
                
            this.host = server;
            this.port = port;
            this.bufferSize = bufferSize;
            this.socketTimeout = socketTimeout;
            this.idleTimeToKeepAlive = idleTimeToKeepAlive;
            this.keepAliveInterval = keepAliveInterval;
            this.socketPollingIntervalTimeout = socketPollingTimeout;
            this.socketPollingLevel = socketPollingLevel;
            this.availableConnections = new ConcurrentQueue<KafkaConnection>();
        }

        /// <summary>
        /// Returns an existing or new kafka connection
        /// </summary>
        /// <param name="server">The server to connect to.</param>
        /// <param name="port">The port to connect to.</param>
        /// <param name="bufferSize"></param>
        /// <param name="socketTimeout"></param>
        /// <param name="idleTimeToKeepAlive">idle time until keepalives (ms)</param>
        /// <param name="keepAliveInterval">interval between keepalives(ms)</param>
        /// <param name="socketPollingTimeout">socket polling timeout(usec)</param>
        /// <param name="keepAliveInterval">interval between keepalives(ms)</param>
        public  KafkaConnection GetConnection()
        {
            KafkaConnection kafkaConnection = null;
          
            while (availableConnections.TryDequeue(out kafkaConnection))
            {
                //validate
                if (IsConnectionValid(kafkaConnection))
                {
                    return kafkaConnection;
                }
                else
                {
                    System.Threading.Interlocked.Decrement(ref socketCounter);
                    kafkaConnection.Dispose();
                }
            }

            return BuildConnection();
        }

        /// <summary>
        /// returns connection back to pool
        /// </summary>
        /// <param name="kafkaConnection"></param>
        public  void ReleaseConnection(KafkaConnection kafkaConnection)
        {
            lock (availableConnections)
            {

                if (availableConnections.Count < maxPoolSize
                    && IsConnectionValid(kafkaConnection))
                {
                    availableConnections.Enqueue(kafkaConnection);
                    return;
                }

                System.Threading.Interlocked.Decrement(ref socketCounter);
                kafkaConnection.Dispose();
            }
        }

        private  KafkaConnection BuildConnection()
        { 
            KafkaConnection newConnection = null;

            if (socketCounter < maxPoolSize)
            {
                newConnection = new KafkaConnection(host,
                                                    port,
                                                    bufferSize,
                                                    socketTimeout,
                                                    idleTimeToKeepAlive,
                                                    keepAliveInterval,
                                                    socketPollingIntervalTimeout,
                                                    socketPollingLevel);

                System.Threading.Interlocked.Increment(ref socketCounter);
            }
            else
            {
                throw new KafkaConnectionPoolException("Kafka Connection Pool reached its limit");
            }

            return newConnection;
        }

        private bool IsConnectionValid(KafkaConnection kafkaConnection)
        {
            bool isConnectionValid = true;

            isConnectionValid = lifespan > DateTime.Now.Subtract(kafkaConnection.TimeCreated);

            if (isConnectionValid)
            {
                isConnectionValid = kafkaConnection.IsSocketConnected();     
            }

            return isConnectionValid;
        }


    }
}
