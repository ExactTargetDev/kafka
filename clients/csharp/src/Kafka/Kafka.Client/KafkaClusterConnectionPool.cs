using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using Kafka.Client.Utils;

namespace Kafka.Client
{
    public static class KafkaClusterConnectionPool
    {
        private static object locker = new object();
        private static ConcurrentDictionary<string, KafkaServerConnectionPool> kafkaServerConnections = null;
        private static bool isInitialized = false;

        private static int _maxPoolSize;

        private static TimeSpan _lifespan;
        private static int _bufferSize;
        private static int _socketTimeout;
        private static long _idleTimeToKeepAlive;
        private static long _keepAliveInterval;
        private static long _socketPollingIntervalTimeout;
        private static SocketPollingLevel _socketPollingLevel;

        public static void Init(int maxPoolSize,
                                TimeSpan lifespan,
                                int bufferSize,
                                int socketTimeout,
                                long idleTimeToKeepAlive,
                                long keepAliveInterval,
                                long socketPollingIntervalTimeout,
                                SocketPollingLevel socketPollingLevel)
        {
            if (!isInitialized)
            {
                kafkaServerConnections = new ConcurrentDictionary<string, KafkaServerConnectionPool>();
                _lifespan = lifespan;
                _maxPoolSize = maxPoolSize;
                _bufferSize = bufferSize;
                _socketTimeout = socketTimeout;
                _idleTimeToKeepAlive = idleTimeToKeepAlive;
                _keepAliveInterval = keepAliveInterval;
                _socketPollingIntervalTimeout = socketPollingIntervalTimeout;
                _socketPollingLevel = socketPollingLevel;
                isInitialized = true;
            }
        }

        public static KafkaConnection GetConnection(string host, int port) 
        {
            KafkaConnection kafkaConnection = null;
            
            lock (locker)
            {
                string connectionKey = GetConnectionKey(host, port);

                KafkaServerConnectionPool serverConnections = null;
                if (kafkaServerConnections.ContainsKey(connectionKey))
                {
                    kafkaConnection = kafkaServerConnections[connectionKey].GetConnection();
                }
                else
                {
                    serverConnections = new KafkaServerConnectionPool(_maxPoolSize,
                                                                   _lifespan,
                                                                   host,
                                                                   port,
                                                                   _bufferSize,
                                                                   _socketTimeout,
                                                                   _idleTimeToKeepAlive,
                                                                   _keepAliveInterval,
                                                                   _socketPollingIntervalTimeout,
                                                                   _socketPollingLevel);

                    kafkaConnection = serverConnections.GetConnection();
                    kafkaServerConnections[connectionKey] = serverConnections;
                }
            }

            return kafkaConnection;
        }

        public static void ReleaseConnection(KafkaConnection kafkaConnection)
        {
            Guard.NotNull(kafkaConnection, "kafkaConnection");
            lock (locker)
            {
                string connectionKey = GetConnectionKey(kafkaConnection.Server, kafkaConnection.Port);

                if (kafkaServerConnections.ContainsKey(connectionKey))
                {
                    kafkaServerConnections[connectionKey].ReleaseConnection(kafkaConnection);
                }
                else
                {
                    kafkaConnection.Dispose();
                }
            }
        }

        private static string GetConnectionKey(string host, int port)
        {
            return string.Concat(host, ":", port.ToString());
        }
    }
}
