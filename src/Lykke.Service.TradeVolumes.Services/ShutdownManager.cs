using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using Lykke.Sdk;
using Lykke.Service.TradeVolumes.Core.Services;

namespace Lykke.Service.TradeVolumes.Services
{
    public class ShutdownManager : IShutdownManager
    {
        private readonly List<IStopable> _stopables = new List<IStopable>();

        public ShutdownManager(IEnumerable<IStartStop> stopables)
        {
            _stopables.AddRange(stopables);
        }

        public Task StopAsync()
        {
            Parallel.ForEach(_stopables, i => i.Stop());

            return Task.CompletedTask;
        }
    }
}
