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

namespace Kafka.Client.Producers.Sync
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using Exceptions;
    using Kafka.Client.Cfg;
    using Kafka.Client.Messages;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;
    using log4net;

    /// <summary>
    /// Sends messages encapsulated in request to Kafka server synchronously
    /// </summary>
    public class SyncProducer : ISyncProducer
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private const int MAX_CONNECT_BACKOFF_MS = 60 * 1000;
        private KafkaConnection connection;
        private DateTime lastConnectionTime;

        private volatile bool disposed;

        /// <summary>
        /// Gets producer config
        /// </summary>
        public SyncProducerConfiguration Config { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncProducer"/> class.
        /// </summary>
        /// <param name="config">
        /// The producer config.
        /// </param>
        public SyncProducer(SyncProducerConfiguration config)
        {
            Guard.NotNull(config, "config");
            Config = config;

            Logger.Debug("Instantiating Sync Producer");
            // make time-based reconnect starting at a random time
            var randomReconnectInterval = (new Random()).NextDouble() * config.ReconnectInterval;
            lastConnectionTime = DateTime.Now - new TimeSpan(Convert.ToInt32(randomReconnectInterval) * TimeSpan.TicksPerMillisecond);
        }

        /// <summary>
        /// Constructs producer request and sends it to given broker partition synchronously
        /// </summary>
        /// <param name="topic">
        /// The topic.
        /// </param>
        /// <param name="partition">
        /// The partition.
        /// </param>
        /// <param name="messages">
        /// The list of messages messages.
        /// </param>
        public void Send(string topic, int partition, IEnumerable<Message> messages)
        {
            Guard.NotNullNorEmpty(topic, "topic");
            Guard.NotNull(messages, "messages");
            Guard.AllNotNull(messages, "messages.items");
            Guard.Assert<ArgumentOutOfRangeException>(
                () => messages.All(
                    x => x.PayloadSize <= Config.MaxMessageSize));
            Send(new ProducerRequest(topic, partition, messages));
        }

        /// <summary>
        /// Sends request to Kafka server synchronously
        /// </summary>
        /// <param name="request">
        /// The request.
        /// </param>
        public void Send(ProducerRequest request)
        {
            Guard.NotNull(request, "request");
            SendRequest(request);
        }

        private void SendRequest(AbstractRequest request)
        {
            lock (this)
            {
                var conn = GetOrMakeConnection();
                try
                {
                    conn.Write(request);
                }
                catch (KafkaConnectionException)
                {
                    Disconnect();
                    throw;
                }

                if (Config.ReconnectTimeInterval >= 0 && (DateTime.Now - lastConnectionTime).TotalMilliseconds >= Config.ReconnectTimeInterval)
                {
                    Disconnect();
                    Connect();
                    lastConnectionTime = DateTime.Now;
                }
            }
        }

        /// <summary>
        /// Sends the data to a multiple topics on Kafka server synchronously
        /// </summary>
        /// <param name="requests">
        /// The requests.
        /// </param>
        public void MultiSend(IEnumerable<ProducerRequest> requests)
        {
            Guard.NotNull(requests, "requests");
            Guard.Assert<ArgumentNullException>(
                () => requests.All(
                    x => x != null && x.MessageSet != null && x.MessageSet.Messages != null));
            Guard.Assert<ArgumentNullException>(
                () => requests.All(
                    x => x.MessageSet.Messages.All(
                        y => y != null && y.PayloadSize <= Config.MaxMessageSize)));
            var multiRequest = new MultiProducerRequest(requests);
            SendRequest(multiRequest);
        }

        private KafkaConnection GetOrMakeConnection()
        {
            EnsuresNotDisposed();
            return connection ?? (connection = Connect());
        }

        private KafkaConnection Connect()
        {
            TimeSpan connectBackoff = new TimeSpan(1 * TimeSpan.TicksPerMillisecond);
            DateTime beginTime = DateTime.Now;

            while (connection == null && !disposed)
            {
                try
                {
                    connection = new KafkaConnection(Config.Host, Config.Port, Config.BufferSize, Config.SocketTimeout);
                    Logger.Info("Connected to " + Config.Host + ":" + Config.Port + " for producing");
                }
                catch (Exception e)
                {
                    Disconnect();
                    DateTime endTime = DateTime.Now;
                    // Throw because the connection timeout has expired
                    if (((endTime - beginTime) + connectBackoff).TotalMilliseconds > Config.ConnectTimeout)
                    {
                        Logger.Error("Producer connection to " + Config.Host + ":" + Config.Port + " timing out after " +
                            Config.ConnectTimeout + " ms", e);
                        throw;
                    }
                    int backoff = Convert.ToInt32(connectBackoff.TotalMilliseconds);
                    Logger.Error("Connection attempt to " + Config.Host + ":" + Config.Port + " failed, next attempt in " + backoff + " ms", e);
                    System.Threading.Thread.Sleep(backoff);
                    backoff = Math.Min(10 * backoff, MAX_CONNECT_BACKOFF_MS);
                    connectBackoff = new TimeSpan(backoff * TimeSpan.TicksPerMillisecond);
                }
            }
            return connection;
        }

        /// <summary>
        /// Disconnect from current channel, closing connection.
        /// Side effect: channel field is set to null on successful disconnect
        /// </summary>
        private void Disconnect()
        {
            try
            {
                if (connection != null)
                {
                    Logger.Info("Disconnecting from " + Config.Host + ":" + Config.Port);
                    KafkaConnection temp = connection;
                    connection = null;
                    temp.Dispose();
                }
            }
            catch (Exception e)
            {
                Logger.Error("Error on disconnect: ", e);
            }
        }

        /// <summary>
        /// Releases all unmanaged and managed resources
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }

            if (disposed)
            {
                return;
            }

            disposed = true;
            lock (this)
            {
                Disconnect();
            }
        }

        /// <summary>
        /// Ensures that object was not disposed
        /// </summary>
        private void EnsuresNotDisposed()
        {
            if (disposed)
            {
                throw new ObjectDisposedException(GetType().Name);
            }
        }
    }
}
