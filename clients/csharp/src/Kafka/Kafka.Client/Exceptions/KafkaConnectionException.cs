namespace Kafka.Client.Exceptions
{
    using System;

    public class KafkaConnectionException : Exception
    {
        public KafkaConnectionException()
        {
        }

        public KafkaConnectionException(Exception exception)
            : base("Connection problem occured.", exception)
        {
        }

        public KafkaConnectionException(string message, Exception exception)
            : base(message, exception)
        {
        }
    } 
}
