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

namespace Kafka.Client
{
    using System;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;

    using Kafka.Client.Exceptions;
    using Kafka.Client.Producers.Async;
    using Kafka.Client.Requests;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;

    /// <summary>
    /// Manages connections to the Kafka.
    /// </summary>
    public class KafkaConnection : IDisposable
    {
        private readonly int bufferSize;

        private readonly int socketTimeout;

        private readonly ulong idleTimeToKeepAlive;

        private readonly ulong keepAliveInterval;

        private readonly long socketPollingTimeout;

        private readonly SocketPollingLevel socketPollingLevel;

        private readonly string server;

        private readonly int port;

        private readonly TcpClient client;

        private readonly DateTime timeCreated;

        private volatile bool disposed;

        /// <summary>
        /// Initializes a new instance of the KafkaConnection class.
        /// </summary>
        /// <param name="server">The server to connect to.</param>
        /// <param name="port">The port to connect to.</param>
        /// <param name="bufferSize"></param>
        /// <param name="socketTimeout"></param>
        /// <param name="idleTimeToKeepAlive">idle time until keepalives (ms)</param>
        /// <param name="keepAliveInterval">interval between keepalives(ms)</param>
        /// <param name="socketPollingTimeout">socket polling timeout(usec)</param>
        /// <param name="keepAliveInterval">interval between keepalives(ms)</param>
        public KafkaConnection(string server, int port, int bufferSize, int socketTimeout, long idleTimeToKeepAlive, long keepAliveInterval, long socketPollingTimeout, SocketPollingLevel socketPollingLevel)
        {
            this.server = server;
            this.port = port;
            this.timeCreated = DateTime.Now;
            this.bufferSize = bufferSize;
            this.socketTimeout = socketTimeout;
            this.idleTimeToKeepAlive = (ulong)idleTimeToKeepAlive;
            this.keepAliveInterval = (ulong)keepAliveInterval;
            this.socketPollingTimeout = socketPollingTimeout;
            this.socketPollingLevel = socketPollingLevel;

            try
            {
                // connection opened
                this.client = new TcpClient(server, port)
                    {
                        ReceiveTimeout = socketTimeout,
                        SendTimeout = socketTimeout,
                        ReceiveBufferSize = bufferSize,
                        SendBufferSize = bufferSize
                    };

                var stream = this.client.GetStream();

                SetKeepAlive();

                this.Reader = new KafkaBinaryReader(stream);
            }
            catch (Exception e)
            {
                Dispose();
                throw new KafkaConnectionException(e);
            }
        }

        public KafkaBinaryReader Reader { get; private set; }

        internal string Server { get { return this.server; } }

        internal int Port { get { return this.port; } }

        internal DateTime TimeCreated { get { return this.timeCreated; } }

        private IPEndPoint RemoteEndPoint
        {
            get
            {
                return (IPEndPoint)this.client.Client.RemoteEndPoint;
            }
        }


        /// <summary>
        /// Writes a producer request to the server asynchronously.
        /// </summary>
        /// <param name="request">The request to make.</param>
        public void BeginWrite(AbstractRequest request)
        {
            this.EnsuresNotDisposed();
            Guard.NotNull(request, "request");

            try
            {
                NetworkStream stream = client.GetStream();
                byte[] data = request.RequestBuffer.GetBuffer();
                stream.BeginWrite(data, 0, data.Length, asyncResult => ((NetworkStream)asyncResult.AsyncState).EndWrite(asyncResult), stream);
            }
            catch (InvalidOperationException e)
            {
                throw new KafkaConnectionException(e);
            }
            catch (IOException e)
            {
                throw new KafkaConnectionException(e);
            }
        }

        /// <summary>
        /// Writes a producer request to the server asynchronously.
        /// </summary>
        /// <param name="request">The request to make.</param>
        /// <param name="callback">The code to execute once the message is completely sent.</param>
        /// <remarks>
        /// Do not dispose connection till callback is invoked, 
        /// otherwise underlying network stream will be closed.
        /// </remarks>
        public void BeginWrite(ProducerRequest request, MessageSent<ProducerRequest> callback)
        {
            this.EnsuresNotDisposed();
            Guard.NotNull(request, "request");
            if (callback == null)
            {
                this.BeginWrite(request);
                return;
            }

            try
            {
                NetworkStream stream = client.GetStream();
                var ctx = new RequestContext<ProducerRequest>(stream, request);

                byte[] data = request.RequestBuffer.GetBuffer();

                if (this.socketPollingLevel == SocketPollingLevel.DOUBLE)
                {
                    PollSocket();
                }

                stream.BeginWrite(
                    data,
                    0,
                    data.Length,
                    delegate(IAsyncResult asyncResult)
                    {
                        var context = (RequestContext<ProducerRequest>)asyncResult.AsyncState;
                        callback(context);
                        context.NetworkStream.EndWrite(asyncResult);
                    },
                    ctx);

                if (this.socketPollingLevel == SocketPollingLevel.SINGLE ||
                    this.socketPollingLevel == SocketPollingLevel.DOUBLE)
                {
                    PollSocket();
                }

            }
            catch (InvalidOperationException e)
            {
                throw new KafkaConnectionException(e);
            }
            catch (IOException e)
            {
                throw new KafkaConnectionException(e);
            }
        }

        /// <summary>
        /// Writes a producer request to the server.
        /// </summary>
        /// <remarks>
        /// Write timeout is defaulted to infitite.
        /// </remarks>
        /// <param name="request">The <see cref="ProducerRequest"/> to send to the server.</param>
        public void Write(AbstractRequest request)
        {
            this.EnsuresNotDisposed();
            Guard.NotNull(request, "request");
            if (this.socketPollingLevel == SocketPollingLevel.DOUBLE)
            {
                PollSocket();
            }

            this.Write(request.RequestBuffer.GetBuffer());

            if (this.socketPollingLevel == SocketPollingLevel.SINGLE ||
                    this.socketPollingLevel == SocketPollingLevel.DOUBLE)
            {
                PollSocket();
            }
        }

        /// <summary>
        /// Writes data to the server.
        /// </summary>
        /// <param name="data">The data to write to the server.</param>
        private void Write(byte[] data)
        {
            try
            {
                NetworkStream stream = this.client.GetStream();
                //// Send the message to the connected TcpServer. 
                stream.Write(data, 0, data.Length);      
            }
            catch (InvalidOperationException e)
            {
                throw new KafkaConnectionException(e);
            }
            catch (IOException e)
            {
                throw new KafkaConnectionException(e);
            }
        }

        /// <summary>
        /// Close the connection to the server.
        /// </summary>
        public void Dispose()
        {
            if (this.disposed)
            {
                return;
            }

            this.disposed = true;
            if (this.client != null)
            {
                this.client.Close();
            }
        }

        /// <summary>
        /// Ensures that object was not disposed
        /// </summary>
        private void EnsuresNotDisposed()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException(this.GetType().Name);
            }
        }

        /// <summary>
        /// Ensures that socket is available for writing
        /// </summary>
        private void PollSocket()
        { 
            if(!IsSocketConnected())
            {
                throw new KafkaConnectionException(String.Format("Socket {0}:{1} is no longer available.", RemoteEndPoint.Address.ToString(), RemoteEndPoint.Port.ToString()));    
            }
        }

        internal bool IsSocketConnected()
        {
            bool isSocketOpen = !this.client.Client.Poll(this.socketTimeout, SelectMode.SelectRead);
            bool isSocketAvailable = !(this.client.Client.Available == 0);
            bool isClientConnected = this.client.Connected;
            return isSocketOpen || isSocketAvailable && isClientConnected;
        }

        /// <summary>
        /// Sets socket options on TCP client to enable keepalive 
        /// </summary>
        private void SetKeepAlive()
        {
            int BytesPerLong = 4;
            int BitsPerByte = 8;

            var input = new[] {
                (this.idleTimeToKeepAlive == 0 || this.keepAliveInterval == 0) ? 0UL : 1UL, // on or off
                                                    this.idleTimeToKeepAlive,
                                                    this.keepAliveInterval
            };

            byte[] inValue = new byte[3 * BytesPerLong];

            for (int i = 0; i < input.Length; i++)
            {
                inValue[i * BytesPerLong + 3] = (byte)(input[i] >> ((BytesPerLong - 1) * BitsPerByte) & 0xff);
                inValue[i * BytesPerLong + 2] = (byte)(input[i] >> ((BytesPerLong - 2) * BitsPerByte) & 0xff);
                inValue[i * BytesPerLong + 1] = (byte)(input[i] >> ((BytesPerLong - 3) * BitsPerByte) & 0xff);
                inValue[i * BytesPerLong + 0] = (byte)(input[i] >> ((BytesPerLong - 4) * BitsPerByte) & 0xff);
            }

            byte[] outValue = BitConverter.GetBytes(0);

            this.client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            this.client.Client.IOControl(IOControlCode.KeepAliveValues, inValue, outValue);
        }
    }
}
