using System.Threading.Tasks;

namespace Lykke.Service.TradeVolumes.Core.Services
{
    public interface IShutdownManager
    {
        Task StopAsync();
    }
}