using Common;
using Lykke.Service.TradeVolumes.Core.Services;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Lykke.Service.TradeVolumes.Services
{
    public class ShutdownManager : IShutdownManager
    {
        private readonly List<IStopable> _stopables;

        public ShutdownManager(IEnumerable<IStopable> stopables)
        {
            _stopables = stopables.ToList();
        }

        public Task StopAsync()
        {
            Parallel.ForEach(_stopables, i => i.Stop());

            return Task.CompletedTask;
        }
    }
}
