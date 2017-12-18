using System;
using System.Runtime.Serialization;

namespace Lykke.Service.TradeVolumes.Core
{
    public class UnknownAssetException : Exception
    {
        public UnknownAssetException()
        {
        }

        public UnknownAssetException(string message) : base(message)
        {
        }

        public UnknownAssetException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected UnknownAssetException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
