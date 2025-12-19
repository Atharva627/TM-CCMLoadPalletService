using log4net;
using System;
using System.Threading;
using System.Threading.Tasks;
using static AtsWmsCCMCountReportService.ats_tata_metallics_dbDataSet;
using AtsWmsCCMCountReportService.ats_tata_metallics_dbDataSetTableAdapters;
using OPCAutomation;
using System.Net.NetworkInformation;
using System.Runtime.ExceptionServices;

namespace AtsWmsCCMCountReportService
{
    class AtsWmsCCMCountReportServiceDetails
    {
        #region Data Tables
        ats_wms_ccm_pallet_count_detailsDataTable ats_wms_ccm_pallet_count_detailsDataTableDT = null;
        //ats_wms_ccm_pallet_count_report_detailsDataTable ats_wms_ccm_pallet_count_report_detailsDataTableDT = null;
        //ats_wms_ccm_detailsDataTable ats_wms_ccm_detailsDataTableDT = null;
        //ats_wms_master_shift_detailsDataTable ats_wms_master_shift_detailsDataTableDT = null;
        ats_wms_master_plc_connection_detailsDataTable ats_wms_master_plc_connection_detailsDataTableDT = null;
        #endregion

        #region Table Adapters
        //ats_wms_ccm_detailsTableAdapter ats_wms_ccm_detailsTableAdapterInstance = new ats_wms_ccm_detailsTableAdapter();
        ats_wms_ccm_pallet_count_detailsTableAdapter ats_wms_ccm_pallet_count_detailsTableAdapterInstance = new ats_wms_ccm_pallet_count_detailsTableAdapter();
        //ats_wms_ccm_pallet_count_report_detailsTableAdapter ats_wms_ccm_pallet_count_report_detailsTableAdapterInstance = new ats_wms_ccm_pallet_count_report_detailsTableAdapter();
        //ats_wms_master_shift_detailsTableAdapter ats_wms_master_shift_detailsTableAdapterInstance = new ats_wms_master_shift_detailsTableAdapter();
        ats_wms_master_plc_connection_detailsTableAdapter ats_wms_master_plc_connection_detailsTableAdapterInstance = new ats_wms_master_plc_connection_detailsTableAdapter();
        #endregion

        #region PLC PING VARIABLE   
        //private string IP_ADDRESS = System.Configuration.ConfigurationManager.AppSettings["IP_ADDRESS"]; //2
        private Ping pingSenderForThisConnection = null;
        private PingReply replyForThisConnection = null;
        private Boolean pingStatus = false;
        //private int serverPingStatusCount = 0;
        #endregion

        #region KEPWARE VARIABLES

        /* Kepware variable*/

        OPCServer ConnectedOpc = new OPCServer();

        Array OPCItemIDs = Array.CreateInstance(typeof(string), 100);
        Array ItemServerHandles = Array.CreateInstance(typeof(Int32), 100);
        Array ItemServerErrors = Array.CreateInstance(typeof(Int32), 100);
        Array ClientHandles = Array.CreateInstance(typeof(Int32), 100);
        Array RequestedDataTypes = Array.CreateInstance(typeof(Int16), 100);
        Array AccessPaths = Array.CreateInstance(typeof(string), 100);
        Array ItemServerValues = Array.CreateInstance(typeof(string), 100);
        OPCGroup OpcGroupNames;
        //object yDIR;
        //object yDIR;
        object tDIR11;
        object yDIR11;
        // Connection string
        static string plcServerConnectionString = null;

        #endregion

        #region Golbal Variables
        static string className = "AtsWmsCCMCountReportServiceDetails";
        private static readonly ILog Log = LogManager.GetLogger(className);
        string IP_ADDRESS = "";
        #endregion
        public async Task StartOperationAsync(CancellationToken cancellationToken)
        {
            Log.Debug("1 :: StartOperationAsync called.");
            try
            {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        Log.Debug("1.1 :: Operation cancelled by user");
                        return;
                    }

                try
                {
                    ats_wms_master_plc_connection_detailsDataTableDT = ats_wms_master_plc_connection_detailsTableAdapterInstance.GetData();
                    IP_ADDRESS = ats_wms_master_plc_connection_detailsDataTableDT[0].PLC_CONNECTION_IP_ADDRESS;
                    Log.Debug("2 :: IP Address :: " + IP_ADDRESS);
                }
                catch (Exception ex)
                {
                    Log.Error("2 :: Exception Occured while getting IP Address", ex);
                }

                pingStatus = checkPlcPingRequest();
                if(pingStatus == true)
                {
                    Log.Debug("3 :: Ping Successfull");

                    try
                    {
                        ats_wms_ccm_pallet_count_detailsDataTableDT = ats_wms_ccm_pallet_count_detailsTableAdapterInstance.GetData();
                        var ccmCount = readTag(ats_wms_ccm_pallet_count_detailsDataTableDT[0].CCM_LOAD_COUNT_TAG);
                        Log.Debug($"4 :: CCM Count = {ccmCount}");
                    }
                    catch (Exception ex)
                    {
                        Log.Error("4 :: Failed to read CCM Load Count Tag ", ex);
                    }

                    try
                    {
                        var machineCount = readTag(ats_wms_ccm_pallet_count_detailsDataTableDT[0].CCM_MACHINE_LOAD_COUNT_TAG);
                        Log.Debug($"5 :: Machine Count = {machineCount}");
                    }
                    catch (Exception ex)
                    {
                        Log.Error("5 :: Failed to read CCM Machine Count Tag ", ex);
                    }

                }
                else
                {
                    Log.Debug("3 :: Ping unsuccessful, IP may be wrong");
                }
                    
                    await Task.Delay(1000, cancellationToken); 

                Log.Debug("Operation finished successfully.");
            }
            catch (OperationCanceledException)
            {
                Log.Debug("Operation Stopped gracefully");
            }
            catch (Exception ex)
            {
                Log.Error("Unexpected error in StartOperationAsync " + ex);
            }
        }

        #region Ping funcationality

        public Boolean checkPlcPingRequest()
        {
            //Log.Debug("IprodPLCMachineXmlGenOperation :: Inside checkServerPingRequest");

            try
            {
                try
                {
                    pingSenderForThisConnection = new Ping();
                    replyForThisConnection = pingSenderForThisConnection.Send(IP_ADDRESS);
                }
                catch (Exception ex)
                {
                    Log.Error("checkPlcPingRequest :: for IP :: " + IP_ADDRESS + " Exception occured while sending ping request :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
                    replyForThisConnection = null;
                }

                if (replyForThisConnection != null && replyForThisConnection.Status == IPStatus.Success)
                {
                    //Log.Debug("checkPlcPingRequest :: for IP :: " + IP_ADDRESS + " Ping success :: " + replyForThisConnection.Status.ToString());
                    return true;
                }
                else
                {
                    //Log.Debug("checkPlcPingRequest :: for IP :: " + IP_ADDRESS + " Ping failed. ");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Log.Error("checkPlcPingRequest :: for IP :: " + IP_ADDRESS + " Exception while checking ping request :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
                return false;
            }
        }

        #endregion

        #region Read and Write PLC tag

        [HandleProcessCorruptedStateExceptions]
        public string readTag(string tagName)
        {

            try
            {
                //Log.Debug("IprodPLCCommunicationOperation :: Inside readTag.");

                // Set PLC tag
                OPCItemIDs.SetValue(tagName, 1);
                //Log.Debug("readTag :: Plc tag is configured for plc group.");

                // remove all group
                ConnectedOpc.OPCGroups.RemoveAll();
                //Log.Debug("readTag :: Remove all group.");

                // Kepware configuration                
                OpcGroupNames = ConnectedOpc.OPCGroups.Add("AtsWmsS1MasterGiveMissionServiceDetailsGroup");
                OpcGroupNames.DeadBand = 0;
                OpcGroupNames.UpdateRate = 500;
                OpcGroupNames.IsSubscribed = true;
                OpcGroupNames.IsActive = true;
                OpcGroupNames.OPCItems.AddItems(1, ref OPCItemIDs, ref ClientHandles, out ItemServerHandles, out ItemServerErrors, RequestedDataTypes, AccessPaths);
                //Log.Debug("readTag :: Kepware properties configuration is complete.");

                // Read tag
                OpcGroupNames.SyncRead((short)OPCAutomation.OPCDataSource.OPCDevice, 1, ref
                   ItemServerHandles, out ItemServerValues, out ItemServerErrors, out tDIR11, out yDIR11);

                //Log.Debug("readTag ::  tag name :: " + tagName + " tag value :: " + Convert.ToString(ItemServerValues.GetValue(1)));

                if (Convert.ToString(ItemServerValues.GetValue(1)).Equals("True"))
                {
                    //Log.Debug("readTag :: Found and Return True");
                    return "True";
                }
                else if (Convert.ToString(ItemServerValues.GetValue(1)).Equals("False"))
                {
                    //Log.Debug("readTag :: Found and Return False");
                    return "False";
                }
                else
                {
                    return Convert.ToString(ItemServerValues.GetValue(1));
                }

            }
            catch (Exception ex)
            {
                Log.Error("readTag :: Exception while reading plc tag :: " + tagName + " :: " + ex.Message);
            }

            Log.Debug("readTag :: Return False.. retun null.");

            return null;
        }

        [HandleProcessCorruptedStateExceptions]
        public Boolean writeTag(string tagName, string tagValue)
        {

            try
            {
                Log.Debug("IprodGiveMissionToStacker :: Inside writeTag.");

                // Set PLC tag
                OPCItemIDs.SetValue(tagName, 1);
                //Log.Debug("writeTag :: Plc tag is configured for plc group.");

                // remove all group
                ConnectedOpc.OPCGroups.RemoveAll();
                //Log.Debug("writeTag :: Remove all group.");

                // Kepware configuration                  
                OpcGroupNames = ConnectedOpc.OPCGroups.Add("AtsWmsS1MasterGiveMissionServiceDetailsGroup");
                OpcGroupNames.DeadBand = 0;
                OpcGroupNames.UpdateRate = 500;
                OpcGroupNames.IsSubscribed = true;
                OpcGroupNames.IsActive = true;
                OpcGroupNames.OPCItems.AddItems(1, ref OPCItemIDs, ref ClientHandles, out ItemServerHandles, out ItemServerErrors, RequestedDataTypes, AccessPaths);
                //Log.Debug("writeTag :: Kepware properties configuration is complete.");

                // read plc tags
                OpcGroupNames.SyncRead((short)OPCAutomation.OPCDataSource.OPCDevice, 1, ref
                   ItemServerHandles, out ItemServerValues, out ItemServerErrors, out tDIR11, out yDIR11);

                // Add tag value
                ItemServerValues.SetValue(tagValue, 1);

                // Write tag
                OpcGroupNames.SyncWrite(1, ref ItemServerHandles, ref ItemServerValues, out ItemServerErrors);

                return true;

            }
            catch (Exception ex)
            {
                Log.Error("writeTag :: Exception while writing mission data in the plc tag :: " + tagName + " :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
                OnConnectPLC();
            }

            return false;

        }

        #endregion

        #region Connect and Disconnect PLC

        private void OnConnectPLC()
        {

            Log.Debug("OnConnectPLC :: inside OnConnectPLC");

            try
            {
                // Connection url
                if (!((ConnectedOpc.ServerState.ToString()).Equals("1")))
                {
                    ConnectedOpc.Connect(plcServerConnectionString, "");
                    Log.Debug("OnConnectPLC :: PLC connection successful and OPC server state is :: " + ConnectedOpc.ServerState.ToString());
                }
                else
                {
                    Log.Debug("OnConnectPLC :: Already connected with the plc.");
                }

            }
            catch (Exception ex)
            {
                Log.Error("OnConnectPLC :: Exception while connecting to plc :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
            }
        }

        private void OnDisconnectPLC()
        {
            Log.Debug("inside OnDisconnectPLC");

            try
            {
                ConnectedOpc.Disconnect();
                Log.Debug("OnDisconnectPLC :: Connection with the plc is disconnected.");
            }
            catch (Exception ex)
            {
                Log.Error("OnDisconnectPLC :: Exception while disconnecting to plc :: " + ex.Message);
            }

        }


        #endregion
    }
}
