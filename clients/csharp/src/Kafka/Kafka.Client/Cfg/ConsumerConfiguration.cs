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
namespace Kafka.Client.Cfg
{
    using Kafka.Client.Exceptions;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;
    using System.Configuration;
    using System.Net;
    using System.Globalization;
    using System.Text;
    using System.Xml.Linq;
    using System.Collections.Generic;


    /// <summary>
    /// Configuration used by the consumer
    /// </summary>
    public class ConsumerConfiguration
    {
        public const short DefaultNumberOfTries = 2;

        public const int DefaultTimeout = -1;

        public const bool DefaultAutoCommit = true;

        public const int DefaultAutoCommitInterval = 10 * 1000;

        public const int DefaultFetchSize = 300 * 1024;

        public const int DefaultBackOffIncrement = 1000;

        public const int DefaultSocketTimeout = 30 * 1000;

        public const int DefaultBufferSize = 64 * 1024;

        public const string DefaultSection = "kafkaConsumer";

        public const int DefaultMaxQueuedChunks = 10;

        public const long DefaultIdleTimeToKeepAlive = 900 * 1000;

        public const long DefaultKeepAliveInterval = 75 * 1000;

        public const long DefaultSocketPollingTimeout = 1000;

        public const SocketPollingLevel DefaultSocketPollingLevel = SocketPollingLevel.NONE;

        public ConsumerConfiguration()
        {
            this.NumberOfTries = DefaultNumberOfTries;
            this.Timeout = DefaultTimeout;
            this.AutoOffsetReset = OffsetRequest.SmallestTime;
            this.AutoCommit = DefaultAutoCommit;
            this.AutoCommitInterval = DefaultAutoCommitInterval;
            this.FetchSize = DefaultFetchSize;
            this.BackOffIncrement = DefaultBackOffIncrement;
            this.MaxQueuedChunks = DefaultMaxQueuedChunks;
            this.IdleTimeToKeepAlive = DefaultIdleTimeToKeepAlive;
            this.KeepAliveInterval = DefaultKeepAliveInterval;
            this.SocketPollingTimeout = DefaultSocketPollingTimeout;
            this.SocketPollingLevel = DefaultSocketPollingLevel;
        }

        public ConsumerConfiguration(string host, int port)
            : this()
        {
            this.Broker = new BrokerConfiguration { Host = host, Port = port };
        }

        public ConsumerConfiguration(ConsumerConfigurationSection config)
        {
            Validate(config);
            this.NumberOfTries = config.NumberOfTries;
            this.GroupId = config.GroupId;
            this.Timeout = config.Timeout;
            this.AutoOffsetReset = config.AutoOffsetReset;
            this.AutoCommit = config.AutoCommit;
            this.AutoCommitInterval = config.AutoCommitInterval;
            this.FetchSize = config.FetchSize;
            this.BackOffIncrement = config.BackOffIncrement;
            this.SocketTimeout = config.SocketTimeout;
            this.BufferSize = config.BufferSize;
            this.MaxQueuedChunks = config.MaxQueuedChunks;
            this.IdleTimeToKeepAlive = config.IdleTimeToKeepAlive;
            this.KeepAliveInterval = config.KeepAliveInterval;
            this.SocketPollingTimeout = config.SocketPollingTimeout;
            this.SocketPollingLevel = config.SocketPollingLevel;
            if (config.Broker.ElementInformation.IsPresent)
            {
                this.SetBrokerConfiguration(config.Broker);
            }
            else
            {
                this.SetZooKeeperConfiguration(config.ZooKeeperServers);
            }
        }

        public ConsumerConfiguration(XElement xmlElement)
            : this(ConsumerConfigurationSection.FromXml(xmlElement))
        {
        }

        public static ConsumerConfiguration Configure(string section)
        {
            var config = ConfigurationManager.GetSection(section) as ConsumerConfigurationSection;
            return new ConsumerConfiguration(config);
        }

        public short NumberOfTries { get; set; }

        public string GroupId { get; set; }

        public int Timeout { get; set; }

        public string AutoOffsetReset { get; set; }

        public bool AutoCommit { get; set; }

        public int AutoCommitInterval { get; set; }

        public int FetchSize { get; set; }

        public int BackOffIncrement { get; set; }

        public int SocketTimeout { get; set; }

        public int BufferSize { get; set; }

        public int MaxQueuedChunks { get; set; }

        public int MaxFetchSize
        {
            get
            {
                return this.FetchSize * 10;
            }
        }

        public ZooKeeperConfiguration ZooKeeper { get; set; }

        public BrokerConfiguration Broker { get; set; }

        public long IdleTimeToKeepAlive { get; set; }

        public long KeepAliveInterval { get; set; }

        public long SocketPollingTimeout { get; set; }

        public SocketPollingLevel SocketPollingLevel { get; set; }

        private static void Validate(ConsumerConfigurationSection config)
        {
            if (config.Broker.ElementInformation.IsPresent
                && config.ZooKeeperServers.ElementInformation.IsPresent)
            {
                throw new ConfigurationErrorsException("ZooKeeper configuration cannot be set when brokers configuration is used");
            }

            if (!config.ZooKeeperServers.ElementInformation.IsPresent
                && !config.Broker.ElementInformation.IsPresent)
            {
                throw new ConfigurationErrorsException("ZooKeeper server or Kafka broker configuration must be set");
            }

            if (config.ZooKeeperServers.ElementInformation.IsPresent
                && config.ZooKeeperServers.Servers.Count == 0)
            {
                throw new ConfigurationErrorsException("At least one ZooKeeper server address is required");
            }
        }

        private static string GetIpAddress(string host)
        {
            IPAddress ipAddress;
            if (!IPAddress.TryParse(host, out ipAddress))
            {
                IPHostEntry ip = Dns.GetHostEntry(host);
                if (ip.AddressList.Length > 0)
                {
                    return ip.AddressList[0].ToString();
                }

                throw new ConfigurationErrorsException(string.Format(CultureInfo.CurrentCulture, "Could not resolve the zookeeper server address: {0}.", host));
            }

            return host;
        }

        private void SetBrokerConfiguration(BrokerConfigurationElement config)
        {
            this.Broker = new BrokerConfiguration
            {
                BrokerId = config.Id,
                Host = GetIpAddress(config.Host),
                Port = config.Port
            };
        }

        private void SetZooKeeperConfiguration(ZooKeeperConfigurationElement config)
        {
            if (config.Servers.Count == 0)
            {
                throw new ConfigurationErrorsException();
            }

            //Need to shuffle servers to prevent hotspotting individual zookeeper nodes
            var shuffledServers = config.Servers.Shuffle();

            var sb = new StringBuilder();
            foreach (ZooKeeperServerConfigurationElement server in shuffledServers)
            {
                sb.Append(GetIpAddress(server.Host));
                sb.Append(':');
                sb.Append(server.Port);
                sb.Append(',');
            }

            sb.Remove(sb.Length - 1, 1);
            this.ZooKeeper = new ZooKeeperConfiguration(
                sb.ToString(),
                config.SessionTimeout,
                config.ConnectionTimeout,
                config.SyncTime);
        }
    }
}
