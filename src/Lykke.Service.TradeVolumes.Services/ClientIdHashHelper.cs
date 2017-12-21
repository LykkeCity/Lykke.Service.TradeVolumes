using System.Security.Cryptography;
using System.Text;

namespace Lykke.Service.TradeVolumes.Services
{
    public static class ClientIdHashHelper
    {
        private static readonly SHA1 _sha = SHA1.Create();

        public static string GetClientIdHash(string clientId)
        {
            if (string.IsNullOrWhiteSpace(clientId))
                return clientId;

            if (clientId.IndexOf('-') == -1) // Is it a GUID?
                return clientId;

            var stringBytes = Encoding.ASCII.GetBytes(clientId);
            var shaHash = _sha.ComputeHash(stringBytes);
            var sb = new StringBuilder();
            foreach (var @byte in shaHash)
            {
                sb.Append(@byte.ToString("X2"));
            }
            return sb.ToString();
        }
    }
}
