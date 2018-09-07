using System.Collections.Generic;
using System.Threading.Tasks;
using Autofac;
using Lykke.Service.TradeVolumes.Core.Services;

namespace Lykke.Service.TradeVolumes.Services
{
    public class StartupManager : IStartupManager
    {
        private readonly List<IStartable> _startables = new List<IStartable>();

        public StartupManager(IEnumerable<IStartStop> startables)
        {
            _startables.AddRange(startables);
        }

        public async Task StartAsync()
        {
            foreach (var startable in _startables)
            {
                startable.Start();
            }

            await Task.CompletedTask;
        }
    }
}
