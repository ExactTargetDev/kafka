using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Kafka.Client.Exceptions;

namespace Kafka.Client.IntegrationTests
{
    public class ConnectionPoolTests : IntegrationFixtureBase
    {
        private const int CONNECTION_POOL_SIZE = 10;
        private List<KafkaConnection> connections = new List<KafkaConnection>();

        [SetUp]
        public void Init()
        {
            var consumerConfig1 = this.ConsumerConfig1;
            var consumerConfig2 = this.ConsumerConfig2;
            var consumerConfig3 = this.ConsumerConfig3;

            KafkaClusterConnectionPool.Init(CONNECTION_POOL_SIZE,
                                new TimeSpan(0, 0, consumerConfig1.ConnectionLifeSpan),
                                consumerConfig1.BufferSize,
                                consumerConfig1.SocketTimeout,
                                consumerConfig1.IdleTimeToKeepAlive,
                                consumerConfig1.KeepAliveInterval,
                                consumerConfig1.SocketPollingTimeout,
                                consumerConfig1.SocketPollingLevel);
        }

        //test overload
        [Test]
        [ExpectedException(typeof(Kafka.Client.Exceptions.KafkaConnectionPoolException))]
        public void TestPoolOverload()
        {
            var consumerConfig1 = this.ConsumerConfig1;
            var connectionCount = CONNECTION_POOL_SIZE + 1;
            
        
            for(int i = 0; i < connectionCount; i++)
            {
                connections.Add(KafkaClusterConnectionPool.GetConnection(consumerConfig1.Broker.Host, consumerConfig1.Broker.Port));
            }

            try
            {
                for (int i = 0; i < connectionCount; i++)
                {
                    KafkaClusterConnectionPool.ReleaseConnection(connections[i]);
                }
            }
            catch (KafkaConnectionPoolException ex)
            {
                //This is the expected result    
            }
        }

        //test getting connections to correct broker
        [Test]
        public void TestCorrectConnectionRetrieval()
        {
            var consumerConfig1 = this.ConsumerConfig1;
            var consumerConfig2 = this.ConsumerConfig2;
            var consumerConfig3 = this.ConsumerConfig3;
            
            //popluate pool
            KafkaClusterConnectionPool.ReleaseConnection(KafkaClusterConnectionPool.GetConnection(consumerConfig1.Broker.Host, consumerConfig1.Broker.Port));
            KafkaClusterConnectionPool.ReleaseConnection(KafkaClusterConnectionPool.GetConnection(consumerConfig2.Broker.Host, consumerConfig2.Broker.Port));
            KafkaClusterConnectionPool.ReleaseConnection(KafkaClusterConnectionPool.GetConnection(consumerConfig3.Broker.Host, consumerConfig3.Broker.Port));

            //Retrieve connections
            var pooledConnection1 = KafkaClusterConnectionPool.GetConnection(consumerConfig1.Broker.Host, consumerConfig1.Broker.Port);
            var pooledConnection2 = KafkaClusterConnectionPool.GetConnection(consumerConfig2.Broker.Host, consumerConfig2.Broker.Port);
            var pooledConnection3 = KafkaClusterConnectionPool.GetConnection(consumerConfig3.Broker.Host, consumerConfig3.Broker.Port);

            Assert.AreEqual(consumerConfig1.Broker.Host, pooledConnection1.Server);
            Assert.AreEqual(consumerConfig1.Broker.Port, pooledConnection1.Port);
            Assert.AreEqual(consumerConfig2.Broker.Host, pooledConnection2.Server);
            Assert.AreEqual(consumerConfig2.Broker.Port, pooledConnection2.Port);
            Assert.AreEqual(consumerConfig3.Broker.Host, pooledConnection3.Server);
            Assert.AreEqual(consumerConfig3.Broker.Port, pooledConnection3.Port);
        }

        [TearDown]
        public void Dispose()
        {
            foreach (KafkaConnection k in connections)
            {
                k.Dispose();
            }
        }
    }
}
