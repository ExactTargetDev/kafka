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

namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Reflection;
    using Kafka.Client.Cfg;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Messages;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;
    using System.Linq;
    using log4net;

    /// <summary>
    /// The low-level API of consumer of Kafka messages
    /// </summary>
    /// <remarks>
    /// Maintains a connection to a single broker and has a close correspondence
    /// to the network requests sent to the server.
    /// Also, is completely stateless.
    /// </remarks>
    public class Consumer : IConsumer
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly ConsumerConfiguration config;
        private readonly string host;
        private readonly int port;

        /// <summary>
        /// Initializes a new instance of the <see cref="Consumer"/> class.
        /// </summary>
        /// <param name="config">
        /// The consumer configuration.
        /// </param>
        public Consumer(ConsumerConfiguration config):this(config, config.Broker.Host, config.Broker.Port)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Consumer"/> class.
        /// </summary>
        /// <param name="config">
        /// The consumer configuration.
        /// </param>
        /// <param name="host"></param>
        /// <param name="port"></param>
        public Consumer(ConsumerConfiguration config, string host, int port)
        {
            Guard.NotNull(config, "config");

            this.config = config;
            this.host = host;
            this.port = port;

            KafkaClusterConnectionPool.Init(this.config.MaxConnectionPoolSize,
                         new TimeSpan(0, 0, this.config.ConnectionLifeSpan),
                         this.config.BufferSize,
                         this.config.SocketTimeout,
                         this.config.IdleTimeToKeepAlive,
                         this.config.KeepAliveInterval,
                         this.config.SocketPollingTimeout,
                         this.config.SocketPollingLevel);
        }

        /// <summary>
        /// Fetch a set of messages from a topic.
        /// </summary>
        /// <param name="request">
        /// Specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
        /// </param>
        /// <returns>
        /// A set of fetched messages.
        /// </returns>
        /// <remarks>
        /// Offset is passed in on every request, allowing the user to maintain this metadata
        /// however they choose.
        /// </remarks>
        public BufferedMessageSet Fetch(FetchRequest request)
        {
            short tryCounter = 1;
            KafkaConnection conn = null;
            int size = 0;
            BufferedMessageSet messageSet = null;

            while (tryCounter <= this.config.NumberOfTries)
            {
                try
                {
                    conn = KafkaClusterConnectionPool.GetConnection(this.host, this.port);
                    conn.Write(request);
                    size = conn.Reader.ReadInt32();
                    messageSet = BufferedMessageSet.ParseFrom(conn.Reader, size, request.Offset);
                    break;
                }
                catch (Exception ex)
                {
                    //// if maximum number of tries reached
                    if (tryCounter == this.config.NumberOfTries)
                    {
                        throw;
                    }

                    tryCounter++;
                    Logger.InfoFormat(CultureInfo.CurrentCulture, "Fetch reconnect due to {0}", ex);
                }
                finally
                {
                    KafkaClusterConnectionPool.ReleaseConnection(conn);    
                }
            }

            return messageSet;
        }

        /// <summary>
        /// Combine multiple fetch requests in one call.
        /// </summary>
        /// <param name="request">
        /// The list of fetch requests.
        /// </param>
        /// <returns>
        /// A list of sets of fetched messages.
        /// </returns>
        /// <remarks>
        /// Offset is passed in on every request, allowing the user to maintain this metadata 
        /// however they choose.
        /// </remarks>
        public IList<BufferedMessageSet> MultiFetch(MultiFetchRequest request)
        {
            KafkaConnection conn = null;
            int size = 0;
            var result = new List<BufferedMessageSet>();
            short tryCounter = 1;
            while (tryCounter <= this.config.NumberOfTries)
            {
                try
                {
                    conn = KafkaClusterConnectionPool.GetConnection(this.host, this.port);        
                    conn.Write(request);
                    size = conn.Reader.ReadInt32();
                    result = (BufferedMessageSet.ParseMultiFrom(conn.Reader, 
                                                                size, 
                                                                request.ConsumerRequests.Count, 
                                                                request.ConsumerRequests.Select(x => x.Offset).ToList())) as List<BufferedMessageSet>;
                    break;
                }
                catch (Exception ex)
                {
                    // if maximum number of tries reached
                    if (tryCounter == this.config.NumberOfTries)
                    {
                        throw;
                    }

                    tryCounter++;
                    Logger.InfoFormat(CultureInfo.CurrentCulture, "MultiFetch reconnect due to {0}", ex);
                }
                finally
                {
                    KafkaClusterConnectionPool.ReleaseConnection(conn);
                }
            }

            return result;
        }

        /// <summary>
        /// Gets a list of valid offsets (up to maxSize) before the given time.
        /// </summary>
        /// <param name="request">
        /// The offset request.
        /// </param>
        /// <returns>
        /// The list of offsets, in descending order.
        /// </returns>
        public IList<long> GetOffsetsBefore(OffsetRequest request)
        {
            KafkaConnection conn = null;
            int size = 0;
            var result = new List<long>();
            short tryCounter = 1;
            while (tryCounter <= this.config.NumberOfTries)
            {
                try
                {
                    conn = KafkaClusterConnectionPool.GetConnection(this.host, this.port);
                    conn.Write(request);
                    size = conn.Reader.ReadInt32();
                    if (size > 0)
                    {
                        short errorCode = conn.Reader.ReadInt16();
                        if (errorCode != ErrorMapping.NoError)
                        {
                            throw new KafkaException(errorCode);
                        }

                        int count = conn.Reader.ReadInt32();
                        for (int i = 0; i < count; i++)
                        {
                            result.Add(conn.Reader.ReadInt64());
                        }
                    }
                    break;
                }
                catch (Exception ex)
                {
                    //// if maximum number of tries reached
                    if (tryCounter == this.config.NumberOfTries)
                    {
                        throw;
                    }

                    tryCounter++;
                    Logger.InfoFormat(CultureInfo.CurrentCulture, "GetOffsetsBefore reconnect due to {0}", ex);
                }
                finally
                {
                    KafkaClusterConnectionPool.ReleaseConnection(conn);
                }
            }

            return result;
        }
    }
}
