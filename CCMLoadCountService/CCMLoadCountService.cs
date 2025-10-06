using log4net;
using log4net.Config;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace CCMLoadCountService
{
    partial class CCMLoadCountService : ServiceBase
    {
        static string className = "CCMLoadCountService";
        private static readonly ILog Log = LogManager.GetLogger(className);
        public CCMLoadCountService()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            // TODO: Add code here to start your service.
            try
            {
                Log.Debug("OnStart :: CCMLoadCountService in OnStart....");

                try
                {
                    XmlConfigurator.Configure();
                    try
                    {
                        CCMLoadCountServiceTaskThread();
                    }
                    catch (Exception ex)
                    {
                        Log.Error("OnStart :: Exception occured while CCMLoadCountServiceTaskThread  threads task :: " + ex.Message);
                    }
                    Log.Debug("OnStart :: CCMLoadCountServiceTaskThread in OnStart ends..!!");
                }
                catch (Exception ex)
                {
                    Log.Error("OnStart :: Exception occured in OnStart :: " + ex.Message);
                }
            }
            catch (Exception ex)
            {
                Log.Error("OnStart :: Exception occured in OnStart :: " + ex.Message);
            }
        }

            public async void CCMLoadCountServiceTaskThread()
        {
            await Task.Run(() =>
            {
                try
                {

                    CCMLoadCountServiceDetails CCMLoadCountServiceDetailsInstance = new CCMLoadCountServiceDetails();
                    CCMLoadCountServiceDetailsInstance.startOperation();
                }
                catch (Exception ex)
                {
                    Log.Error("Service :: Exception in CCMLoadCountServiceTaskThread :: " + ex.Message);
                }

            });
        }
    

        protected override void OnStop()
        {
            // TODO: Add code here to perform any tear-down necessary to stop your service.
            try
            {
                Log.Debug("OnStop :: AtsWmsDispatchOrderService in OnStop ends..!!");
            }
            catch (Exception ex)
            {
                Log.Error("OnStop :: Exception occured in OnStop :: " + ex.Message);
            }
        }
    }
}
