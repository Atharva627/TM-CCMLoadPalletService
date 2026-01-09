using log4net;
using log4net.Config;
using System;
using System.ServiceProcess;
using System.Threading;
using System.Threading.Tasks;

namespace AtsWmsCCMCountReportService
{
    public partial class AtsWmsCCMCountReportService : ServiceBase
    {
        static string className = "AtsWmsCCMCountReportService";
        private static readonly ILog Log = LogManager.GetLogger(className);

        private CancellationTokenSource _cts;
        private Task _workerTask;
        public AtsWmsCCMCountReportService()
        {
            InitializeComponent();
        }
        protected override void OnStart(string[] args)
        {
            XmlConfigurator.Configure();
            Log.Debug("Service starting...");

            _cts = new CancellationTokenSource();
            try
            {
                _workerTask = StartWorkerAsync(_cts.Token);

                Log.Debug("Service started.");
            }
            catch (Exception ex)
            {
                Log.Error("OnStart :: Failed Exception Occured " + ex);
                throw;
            }
        }

        protected override void OnStop()
        {
            Log.Debug("Service Stopping...");

            try
            {
                _cts?.Cancel();

                if(_workerTask != null)
                {
                    var timeout = TimeSpan.FromSeconds(30);
                    if (!Task.WaitAll(new[] { _workerTask}, timeout))
                    {
                        Log.Warn("Worker did not stop within timeout");
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error("OnStop :: Error occured in OnStop :: "+ ex);
            }
            finally
            {
                _cts?.Dispose();
                _cts = null;
                _workerTask = null;
                Log.Debug("Service Stopped.");
            }
        }

        //Worker Function
        private async Task StartWorkerAsync(CancellationToken cancellationToken)
        {
            try
            {
                var details = new AtsWmsCCMCountReportServiceDetails();

                while (!cancellationToken.IsCancellationRequested)
                {
                    await details.StartOperationAsync(cancellationToken).ConfigureAwait(false);
                    await Task.Delay(TimeSpan.FromMinutes(1), cancellationToken).ConfigureAwait(false);
                }
            }
            catch(OperationCanceledException)
            {
                Log.Debug("Worker cancelled");
            }
            catch(Exception ex)
            {
                Log.Error("Worker crached", ex);
            }
        }
    }
}
