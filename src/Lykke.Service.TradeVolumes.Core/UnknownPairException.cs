using System;
using System.Runtime.Serialization;

namespace Lykke.Service.TradeVolumes.Core
{
    public class UnknownPairException : Exception
    {
        public UnknownPairException()
        {
        }

        public UnknownPairException(string message) : base(message)
        {
        }

        public UnknownPairException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected UnknownPairException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
