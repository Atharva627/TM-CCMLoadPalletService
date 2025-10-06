using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.NetworkInformation;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CCMLoadCountService.ats_tata_metallics_dbDataSetTableAdapters;
using static CCMLoadCountService.ats_tata_metallics_dbDataSet;
using log4net;
using OPCAutomation;
using System.Timers;

namespace CCMLoadCountService
{
    class CCMLoadCountServiceDetails
    {
        #region Data Tables
        ats_wms_master_plc_connection_detailsDataTable ats_wms_master_plc_connection_detailsDataTableDT = null;
        ats_wms_ccm_pallet_count_detailsDataTable ats_wms_ccm_pallet_count_detailsDataTableDT = null;
        //ats_wms_station_tag_detailsDataTable ats_wms_station_tag_detailsDataTable = null;

        #endregion

        #region Table Aadaptor
        ats_wms_master_plc_connection_detailsTableAdapter ats_wms_master_plc_connection_detailsTableAdapterInstance = new ats_wms_master_plc_connection_detailsTableAdapter();
        ats_wms_ccm_pallet_count_detailsTableAdapter ats_wms_ccm_pallet_count_detailsTableAdapterInstance = new ats_wms_ccm_pallet_count_detailsTableAdapter();
        ats_wms_station_tag_detailsTableAdapter ats_wms_station_tag_detailsTableAdapterInstance = new ats_wms_station_tag_detailsTableAdapter();


        // ats_wms_buffer_detailsTableAdapter ats_wms_buffer_detailsTableAdapterInstance = new ats_wms_buffer_detailsTableAdapter();
        //ats_wms_tempreture_alarm_mission_runtime_detailsTableAdapter ats_wms_tempreture_alarm_mission_runtime_detailsTableAdapterInstance = new ats_wms_tempreture_alarm_mission_runtime_detailsTableAdapter();
        #endregion

        #region Global Variables
        static string className = "CCMLoadCountServiceDetails";
        private static readonly ILog Log = LogManager.GetLogger(className);
        private System.Timers.Timer AtsWmsStackerLiveDataServiceDetailsTimer = null;
        //public string IP_ADDRESS = "172.16.67.60";

        string currentDate = "";
        string currentTime = "";
        int areaId = 1;
        int positionNumberInRack = 0;
        public int stackerAreaSide = 0;
        public int stackerFloor = 0;
        public int stackerColumn = 0;
        public int destinationPositionNumberInRack = 1;
        int palletPresentOnStackerPickupPosition = 0;
        string palletCodeOnStackerPickupPosition = "";
        int stackerRightSide = 2;
        int stackerLeftSide = 1;
        int sourcePositionTagType = 0;
        int destinationPositionTagType = 1;
        int feedbackTagType = 2;
        #endregion

        #region PLC PING VARIABLE   
        //private string IP_ADDRESS = System.Configuration.ConfigurationManager.AppSettings["IP_ADDRESS"]; //2
        string IP_ADDRESS = "";
        private Ping pingSenderForThisConnection = null;
        private PingReply replyForThisConnection = null;
        private Boolean pingStatus = false;
        private int serverPingStatusCount = 0;
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
        OPCGroup OpcGroupNamesA1T;
        object aTC55;
        object bTC55;

        // Connection string
        static string plcServerConnectionString = null;

        #endregion

        public void startOperation()
        {
            try
            {
                Log.Debug("startOperation");
                AtsWmsStackerLiveDataServiceDetailsTimer = new System.Timers.Timer();
                //Running the function after 1 sec 
                AtsWmsStackerLiveDataServiceDetailsTimer.Interval = (3000);
                //After 1 sec timer will elapse and DataFetchDetailsOperation function will be called 
                AtsWmsStackerLiveDataServiceDetailsTimer.Elapsed += new System.Timers.ElapsedEventHandler(AtsWmsCCMLoadCountDetailsOperation);
                AtsWmsStackerLiveDataServiceDetailsTimer.AutoReset = false;
                AtsWmsStackerLiveDataServiceDetailsTimer.Start();
            }
            catch (Exception ex)
            {
                Log.Error("startOperation :: Exception Occure in AtsWmsStackerLiveDataServiceDetailsTimer" + ex.Message);
            }
        }

        private void AtsWmsCCMLoadCountDetailsOperation(object sender, ElapsedEventArgs e)
        {
            try
            {

                try
                {
                    AtsWmsStackerLiveDataServiceDetailsTimer.Stop();

                }
                catch (Exception ex)
                {
                    Log.Error("AtsWmsCCMLoadCountDetails :: Exception occure while stopping the timer :: " + ex.Message + "StackTrace  :: " + ex.StackTrace);
                }
                try
                {
                    //Fetching PLC data from DB by sending PLC connection IP address
                    try
                    {
                        ats_wms_master_plc_connection_detailsDataTableDT = ats_wms_master_plc_connection_detailsTableAdapterInstance.GetData();
                    }
                    catch (Exception ex)
                    {
                        Log.Error("AtsWmsCCMLoadCountDetails :: Exception Occure while reading machine datasource connection details from the database :: " + ex.Message + "StackTrace :: " + ex.StackTrace);
                    }
                    Log.Debug("0.0.0.");
                    if (ats_wms_master_plc_connection_detailsDataTableDT != null && ats_wms_master_plc_connection_detailsDataTableDT.Count > 0)
                    {
                        Log.Debug("2.0 :: IP_ADDRESS ::" + ats_wms_master_plc_connection_detailsDataTableDT[0].PLC_CONNECTION_IP_ADDRESS);
                        IP_ADDRESS = ats_wms_master_plc_connection_detailsDataTableDT[0].PLC_CONNECTION_IP_ADDRESS;
                        Log.Debug("2.1 :: IP_ADDRESS ::" + IP_ADDRESS);

                    }
                    else
                    {
                        Log.Debug("2.2 :: PLC Data Not found in Table");
                    }
                }
                catch (Exception ex)
                {

                    Log.Error("AtsWmsCCMLoadCountDetails :: Exception Occure while reading machine datasource connection details from the database :: " + ex.Message + "StackTrace :: " + ex.StackTrace);
                }


                // Check PLC Ping Status
                try
                {
                    // Checking the PLC ping status by a method
                    pingStatus = checkPlcPingRequest();
                    Log.Debug("ping" + pingStatus);
                }
                catch (Exception ex)
                {
                    Log.Error("AtsWmsCCMLoadCountDetails :: Exception while checking plc ping status :: " + ex.Message + " stactTrace :: " + ex.StackTrace);
                }

                if (pingStatus == true)

                //if (true)
                {

                    if (ats_wms_master_plc_connection_detailsDataTableDT != null & ats_wms_master_plc_connection_detailsDataTableDT.Count != 0)
                    {
                        //if (true)
                        {
                            Log.Debug("plc connect");
                            try
                            {
                                plcServerConnectionString = ats_wms_master_plc_connection_detailsDataTableDT[0].PLC_CONNECTION_URL;
                            }
                            catch (Exception ex)
                            {
                                Log.Error("AtsWmsCCMLoadCountDetails :: Exception while Checking plcServerConnectionString :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
                            }
                            try
                            {
                                // Calling the connection method for PLC connection

                                OnConnectPLC();
                                Log.Debug("ONConnectPLC");

                            }
                            catch (Exception ex)
                            {
                                Log.Error("AtsWmsCCMLoadCountDetails :: Exception while connecting to plc :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
                            }

                            // Check the PLC connected status
                            Log.Debug("0.0");
                            if (ConnectedOpc.ServerState.ToString().Equals("1"))
                            //if (true)
                            {
                                try
                                {
                                    Log.Debug("Step 1: Starting AtsWmsCCMLoadCountDetails process");

                                    try
                                    {
                                        Log.Debug("Step 2: Attempting to retrieve data from ats_wms_ccm_pallet_count_details");
                                        ats_wms_ccm_pallet_count_detailsDataTableDT = ats_wms_ccm_pallet_count_detailsTableAdapterInstance.GetData();
                                        Log.Debug("Step 3: Data retrieval successful");
                                    }
                                    catch (Exception ex)
                                    {
                                        Log.Error("Step 3: Data retrieval failed - " + ex.Message + " Stacktrace: " + ex.StackTrace);
                                    }

                                    Log.Debug("Step 4: Checking if data table is not null and has rows");

                                    if (ats_wms_ccm_pallet_count_detailsDataTableDT != null && ats_wms_ccm_pallet_count_detailsDataTableDT.Count > 0)
                                    {
                                        Log.Debug("Step 5: Data table contains " + ats_wms_ccm_pallet_count_detailsDataTableDT.Count + " rows");
                                        int loadCount = 0, machineLoadCount=0;

                                        for (int i = 0; i < ats_wms_ccm_pallet_count_detailsDataTableDT.Count; i++)
                                        {
                                            Log.Debug("Step 6: Processing row index " + i);

                                            //if (ats_wms_ccm_pallet_count_detailsDataTableDT[i].CCM_PALLET_COUNT != 0)
                                            //{
                                            Log.Debug("Step 7: Checking for Pallet Load Count");

                                            try
                                            {
                                                loadCount = int.Parse(readTag(ats_wms_ccm_pallet_count_detailsDataTableDT[i].CCM_LOAD_COUNT_TAG));
                                                Log.Debug("Step 8: Read tag successful, loadCount = " + loadCount);
                                            }
                                            catch (Exception ex)
                                            {
                                                Log.Error("Step 8: Error reading Load Count Tag - " + ex.Message + " Stacktrace: " + ex.StackTrace);
                                            }

                                            

                                            if (ats_wms_ccm_pallet_count_detailsDataTableDT[i].CCM_PALLET_COUNT != loadCount)
                                            {
                                                Log.Debug("Step 9: Mismatch found, updating CCM_PALLET_COUNT in database");

                                                try
                                                {
                                                    ats_wms_ccm_pallet_count_detailsTableAdapterInstance.UpdateCCM_PALLET_COUNTWhereCCM_ID(
                                                        loadCount,
                                                        ats_wms_ccm_pallet_count_detailsDataTableDT[i].CCM_ID
                                                    );
                                                    Log.Debug("Step 10: Update successful for CCM_ID = " + ats_wms_ccm_pallet_count_detailsDataTableDT[i].CCM_ID);
                                                }
                                                catch (Exception ex)
                                                {
                                                    Log.Error("Step 10: Error updating Load Count - " + ex.Message + " Stacktrace: " + ex.StackTrace);
                                                }
                                            }
                                            else
                                            {
                                                Log.Debug("Step 9: No mismatch found, no update needed for CCM_ID = " + ats_wms_ccm_pallet_count_detailsDataTableDT[i].CCM_ID);
                                            }
                                            //}
                                            //else
                                            //{
                                            //    Log.Debug("Step 7: Skipping row as CCM_PALLET_COUNT is zero");
                                            //}
                                            Log.Debug("Step 11: Checking for Pallet Machine Load Count");

                                            try
                                            {
                                                machineLoadCount = int.Parse(readTag(ats_wms_ccm_pallet_count_detailsDataTableDT[i].CCM_MACHINE_LOAD_COUNT_TAG));
                                                Log.Debug("Step 12: Read tag successful, machineLoadCount = " + machineLoadCount);
                                            }
                                            catch (Exception ex)
                                            {
                                                Log.Error("Step 12: Error reading Load Count Tag - " + ex.Message + " Stacktrace: " + ex.StackTrace);
                                            }



                                            if (ats_wms_ccm_pallet_count_detailsDataTableDT[i].CCM_MACHINE_PALLET_COUNT != machineLoadCount)
                                            {
                                                Log.Debug("Step 13: Mismatch found, updating CCM_MACHINE_PALLET_COUNT in database");

                                                try
                                                {
                                                    ats_wms_ccm_pallet_count_detailsTableAdapterInstance.UpdateCCM_MACHINE_PALLET_COUNTWhereCCM_ID(
                                                        machineLoadCount,
                                                        ats_wms_ccm_pallet_count_detailsDataTableDT[i].CCM_ID
                                                    );
                                                    Log.Debug("Step 14: Update successful for CCM_ID = " + ats_wms_ccm_pallet_count_detailsDataTableDT[i].CCM_ID);
                                                }
                                                catch (Exception ex)
                                                {
                                                    Log.Error("Step 14: Error updating Load Count - " + ex.Message + " Stacktrace: " + ex.StackTrace);
                                                }
                                            }
                                            else
                                            {
                                                Log.Debug("Step 13: No mismatch found, no update needed for CCM_ID = " + ats_wms_ccm_pallet_count_detailsDataTableDT[i].CCM_ID);
                                            }
                                        }
                                    }
                                    else
                                    {
                                        Log.Debug("Step 5: Data table is null or empty");
                                    }

                                    Log.Debug("Step 15: Sleeping for 500ms before next operation");
                                    Thread.Sleep(500);
                                }
                                catch (Exception ex)
                                {
                                    Log.Error("Step 16: Exception while connecting to PLC - " + ex.Message + " Stacktrace: " + ex.StackTrace);
                                }



                            }
                            else
                            {
                                //Reconnect to plc, Check Ip address, url
                            }
                        }
                    }
                }
                else
                {
                    Log.Error("ping status false :");
                }
            }
            catch (Exception ex)
            {

                Log.Error("startOperation :: Exception occured while stopping timer :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
            }
            finally
            {
                try
                {
                    AtsWmsStackerLiveDataServiceDetailsTimer.Start();
                }
                catch (Exception ex1)
                {
                    Log.Error("startOperation :: Exception occured while stopping timer :: " + ex1.Message + " stackTrace :: " + ex1.StackTrace);
                }


            }
        }

        #region Ping funcationality

        public Boolean checkPlcPingRequest()
        {
            Log.Debug("IprodPLCMachineXmlGenOperation :: Inside checkServerPingRequest");

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
                    Log.Debug("checkPlcPingRequest :: for IP :: " + IP_ADDRESS + " Ping success :: " + replyForThisConnection.Status.ToString());
                    return true;
                }
                else
                {
                    Log.Debug("checkPlcPingRequest :: for IP :: " + IP_ADDRESS + " Ping failed. ");
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
                OpcGroupNamesA1T = ConnectedOpc.OPCGroups.Add("AtsWmsStackerLiveDataServiceDetailsGroup");
                OpcGroupNamesA1T.DeadBand = 0;
                OpcGroupNamesA1T.UpdateRate = 100;
                OpcGroupNamesA1T.IsSubscribed = true;
                OpcGroupNamesA1T.IsActive = true;
                OpcGroupNamesA1T.OPCItems.AddItems(1, ref OPCItemIDs, ref ClientHandles, out ItemServerHandles, out ItemServerErrors, RequestedDataTypes, AccessPaths);
                OpcGroupNamesA1T.SyncRead((short)OPCAutomation.OPCDataSource.OPCDevice, 1, ref
                   ItemServerHandles, out ItemServerValues, out ItemServerErrors, out aTC55, out bTC55);

                //Log.Debug("readTag ::  tag name :: " + tagName + " tag value :: " + Convert.ToString(ItemServerValues.GetValue(1)));

                if (Convert.ToString(ItemServerValues.GetValue(1)).Equals("True"))
                {
                    //Log.Debug("readTag :: Found and Return True");
                    //ConnectedOpc.OPCGroups.Remove("AtsWmsStationWorkDoneDetailsGroup");
                    return "True";
                }
                else if (Convert.ToString(ItemServerValues.GetValue(1)).Equals("False"))
                {
                    //Log.Debug("readTag :: Found and Return False");
                    //ConnectedOpc.OPCGroups.Remove("AtsWmsStationWorkDoneDetailsGroup");
                    return "False";
                }
                else
                {
                    //ConnectedOpc.OPCGroups.Remove("AtsWmsStationWorkDoneDetailsGroup");
                    return Convert.ToString(ItemServerValues.GetValue(1));
                }

            }
            catch (Exception ex)
            {
                Log.Error("readTag :: Exception while reading plc tag :: " + tagName + " :: " + ex.Message);
            }

            Log.Debug("readTag :: Return False.. retun null.");

            return "False";
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
                OpcGroupNamesA1T = ConnectedOpc.OPCGroups.Add("AtsWmsStackerLiveDataServiceDetailsGroup");
                OpcGroupNamesA1T.DeadBand = 0;
                OpcGroupNamesA1T.UpdateRate = 100;
                OpcGroupNamesA1T.IsSubscribed = true;
                OpcGroupNamesA1T.IsActive = true;
                OpcGroupNamesA1T.OPCItems.AddItems(1, ref OPCItemIDs, ref ClientHandles, out ItemServerHandles, out ItemServerErrors, RequestedDataTypes, AccessPaths);
                //Log.Debug("writeTag :: Kepware properties configuration is complete.");

                // read plc tags
                OpcGroupNamesA1T.SyncRead((short)OPCAutomation.OPCDataSource.OPCDevice, 1, ref
                   ItemServerHandles, out ItemServerValues, out ItemServerErrors, out aTC55, out bTC55);

                // Add tag value
                ItemServerValues.SetValue(tagValue, 1);

                // Write tag
                OpcGroupNamesA1T.SyncWrite(1, ref ItemServerHandles, ref ItemServerValues, out ItemServerErrors);
                //ConnectedOpc.OPCGroups.Remove("AtsWmsStationWorkDoneDetailsGroup");
                return true;

            }
            catch (Exception ex)
            {
                Log.Error("writeTag :: Exception while writing mission data in the plc tag :: " + tagName + " :: " + ex.Message + " stackTrace :: " + ex.StackTrace);

                OnConnectPLC();
                Thread.Sleep(1000);

                Log.Debug("writing again :: tagName" + tagName + " tagValue :: " + tagValue);
                writeTag(tagName, tagValue);
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

