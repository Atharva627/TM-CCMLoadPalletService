using System.ServiceProcess;


namespace AtsWmsCCMCountReportService
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[]
            {
                new AtsWmsCCMCountReportService()
            };
            ServiceBase.Run(ServicesToRun);
        }
    }
}
