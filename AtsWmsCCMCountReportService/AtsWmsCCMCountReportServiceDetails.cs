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
        ats_wms_ccm_pallet_count_detailsDataTable ats_wms_ccm_pallet_count_detailsDataTableDT1 = null;
       // ats_wms_ccm_pallet_count_detailsDataTable ats_wms_ccm_pallet_count_detailsDataTableDT2 = null;
        ats_wms_ccm_pallet_count_detailsDataTable ats_wms_ccm_pallet_count_detailsDataTableDT3 = null;
        //ats_wms_ccm_pallet_count_detailsDataTable ats_wms_ccm_pallet_count_detailsDataTableGetValues = null;
        ats_wms_ccm_pallet_count_report_detailsDataTable ats_wms_ccm_pallet_count_report_detailsDataTableDT = null;
        //ats_wms_ccm_detailsDataTable ats_wms_ccm_detailsDataTableDT = null;
        ats_wms_master_shift_detailsDataTable ats_wms_master_shift_detailsDataTableDT = null;
        ats_wms_master_plc_connection_detailsDataTable ats_wms_master_plc_connection_detailsDataTableDT = null;
        #endregion

        #region Table Adapters
        //ats_wms_ccm_detailsTableAdapter ats_wms_ccm_detailsTableAdapterInstance = new ats_wms_ccm_detailsTableAdapter();
        ats_wms_ccm_pallet_count_detailsTableAdapter ats_wms_ccm_pallet_count_detailsTableAdapterInstance = new ats_wms_ccm_pallet_count_detailsTableAdapter();
        ats_wms_ccm_pallet_count_report_detailsTableAdapter ats_wms_ccm_pallet_count_report_detailsTableAdapterInstance = new ats_wms_ccm_pallet_count_report_detailsTableAdapter();
        ats_wms_master_shift_detailsTableAdapter ats_wms_master_shift_detailsTableAdapterInstance = new ats_wms_master_shift_detailsTableAdapter();
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
        bool connectionStatus = false;
        //string SHIFT = "2";
        #endregion

        public void loadCount()
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
                    int loadCount = 0, machineLoadCount = 0;

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

                //Log.Debug("Step 15: Sleeping for 500ms before next operation");
                //Thread.Sleep(500);
            }
            catch (Exception ex)
            {
                Log.Error("Step 16: Exception while Executing Load Count logic - " + ex.Message + " Stacktrace: " + ex.StackTrace);
            }

        }
        public bool change(int CCM_ID)
        {
            Log.Debug("Inside change()");
            try
            {
                ats_wms_ccm_pallet_count_detailsDataTableDT1 = ats_wms_ccm_pallet_count_detailsTableAdapterInstance.GetDataByCCM_ID(CCM_ID);

                if (ats_wms_ccm_pallet_count_detailsDataTableDT1 == null || ats_wms_ccm_pallet_count_detailsDataTableDT1.Count == 0)
                {
                    Log.Debug($"No entry for CCM_ID {CCM_ID} found in the table");
                    return false;
                }

                var prevCCMCount = ats_wms_ccm_pallet_count_detailsDataTableDT1[0].CCM_PREV_PALLET_COUNT;
                var currentCCMCount = ats_wms_ccm_pallet_count_detailsDataTableDT1[0].CCM_PALLET_COUNT;
                var prevMachineCount = ats_wms_ccm_pallet_count_detailsDataTableDT1[0].CCM_PREV_MACHINE_PALLET_COUNT;
                var currentMachineCount = ats_wms_ccm_pallet_count_detailsDataTableDT1[0].CCM_MACHINE_PALLET_COUNT;

                Log.Debug($"prevCCMCount={prevCCMCount}, currentCCMCount={currentCCMCount}, prevMachineCount={prevMachineCount}, currentMachineCount={currentMachineCount}");

                if ((prevCCMCount != currentCCMCount) || (prevMachineCount != currentMachineCount))
                {
                    Log.Debug("One of the two Tag values are out of sync");
                    try
                    {
                        ats_wms_ccm_pallet_count_detailsTableAdapterInstance.UpdateCCM_PREV_PALLET_COUNTAndCCM_PREV_MACHINE_PALLET_COUNTWhereCCM_ID(CCM_ID);
                    }
                    catch (Exception ex)
                    {
                        Log.Error("Exception occured while updating the Previous values in ccm_pallet_count_details table ", ex);
                    }
                    Log.Debug("Updated the previous values");
                    return true;
                }

                Log.Debug("Values are synced and updated");
                return false;
            }
            catch (Exception ex)
            {
                Log.Error("Exception occurred in change() function ", ex);
            }

            Log.Debug("Error occured while running the logic");
            return false;
        }

        public string shiftCalculate()
        {
            string shiftName = "";
            DateTime now = DateTime.Now;
            ats_wms_master_shift_detailsDataTableDT = ats_wms_master_shift_detailsTableAdapterInstance.GetDataByCurrentShiftDataByCurrentTimeAndSHIFT_IS_DELETED(now.ToString("HH:mm:ss"), 0);
            if (ats_wms_master_shift_detailsDataTableDT != null && ats_wms_master_shift_detailsDataTableDT.Count > 0)
            {
                shiftName = ats_wms_master_shift_detailsDataTableDT[0].SHIFT_NAME;
                Log.Debug("Shift name: " + shiftName);
            }
            else
            {
                var data = ats_wms_master_shift_detailsTableAdapterInstance.GetShiftDataByStartTimeGreaterThanEndTimeAndSHIFT_IS_DELETED(0);
                if (data != null)
                {
                    shiftName = data[0].SHIFT_NAME;
                }
            }
            return shiftName;
        }

        //public void prevValueReset()
        //{
        //    try
        //    {
        //        ats_wms_ccm_pallet_count_detailsDataTableDT2 = ats_wms_ccm_pallet_count_detailsTableAdapterInstance.GetData();

        //        for (int i = 0; i < ats_wms_ccm_pallet_count_detailsDataTableDT2.Count; i++)
        //        {
        //            ats_wms_ccm_pallet_count_detailsDataTableGetValues = ats_wms_ccm_pallet_count_detailsTableAdapterInstance.GetDataByCCM_ID(ats_wms_ccm_pallet_count_detailsDataTableDT2[i].CCM_ID);
        //            var prevCCMCount = ats_wms_ccm_pallet_count_detailsDataTableDT1[0].CCM_PREV_PALLET_COUNT;
        //            var currentCCMCount = ats_wms_ccm_pallet_count_detailsDataTableDT1[0].CCM_PALLET_COUNT;
        //            var prevMachineCount = ats_wms_ccm_pallet_count_detailsDataTableDT1[0].CCM_PREV_MACHINE_PALLET_COUNT;
        //            var currentMachineCount = ats_wms_ccm_pallet_count_detailsDataTableDT1[0].CCM_MACHINE_PALLET_COUNT;

        //            if ((prevCCMCount > currentCCMCount) && (currentCCMCount == 0))
        //            {
        //                //Doubt about this operation! -update query
        //                ats_wms_ccm_pallet_count_detailsDataTableGetValues[0].CCM_PREV_PALLET_COUNT = 0;
        //            }

        //            if ((prevMachineCount > currentMachineCount) && (currentMachineCount == 0))
        //            {
        //                //Doubt about this operation! -update query
        //                ats_wms_ccm_pallet_count_detailsDataTableGetValues[0].CCM_PREV_MACHINE_PALLET_COUNT = 0;
        //            }
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        Log.Error("Exception occurred while updating the Previous and Current value in ccmPalletCount table ", ex);
        //    }
        //}

        public void insertNewEntry(int CCM_ID)
        {
            Log.Debug("Inside insertNewEntry()");
            try
            {
                ats_wms_ccm_pallet_count_detailsDataTableDT3 = ats_wms_ccm_pallet_count_detailsTableAdapterInstance.GetDataByCCM_ID(CCM_ID);
                var newEntry_Pallet_Count = ats_wms_ccm_pallet_count_detailsDataTableDT3[0].CCM_PALLET_COUNT;
                var newEntry_Machine_Pallet_Count = ats_wms_ccm_pallet_count_detailsDataTableDT3[0].CCM_MACHINE_PALLET_COUNT;

                //Interlock if there is no Entry of particuler CCM_ID in the ccm_pallet_count_report_details table
                ats_wms_ccm_pallet_count_report_detailsDataTableDT = ats_wms_ccm_pallet_count_report_detailsTableAdapterInstance.GetLatestEntryWhereCCM_ID(CCM_ID);
                var prevEntry_Pallet_Count = 0;
                var prevEntry_Machine_Pallet_Count = 0;

                if (ats_wms_ccm_pallet_count_report_detailsDataTableDT != null && ats_wms_ccm_pallet_count_report_detailsDataTableDT.Count > 0){
                    prevEntry_Pallet_Count = ats_wms_ccm_pallet_count_report_detailsDataTableDT[0].CCM_PALLET_COUNT;
                    prevEntry_Machine_Pallet_Count = ats_wms_ccm_pallet_count_report_detailsDataTableDT[0].CCM_MACHINE_PALLET_COUNT;
                }

                Log.Debug($"newEntry_Pallet_Count = { newEntry_Pallet_Count}, newEntry_Machine_Pallet_Count = {newEntry_Machine_Pallet_Count}");
                Log.Debug($"prevEntry_Pallet_Count = {prevEntry_Pallet_Count}, prevEntry_Machine_Pallet_Count = {prevEntry_Machine_Pallet_Count}");

                if((newEntry_Pallet_Count == prevEntry_Pallet_Count) && (newEntry_Machine_Pallet_Count == prevEntry_Machine_Pallet_Count))
                {
                    Log.Debug("The values are synced and upto date in ccm_pallet_count table and ccm_pallet_count_report table");
                    return;
                }

                try
                {
                    var currentShiftName = shiftCalculate();

                    ats_wms_ccm_pallet_count_report_detailsTableAdapterInstance.Insert(
                                CCM_ID,
                                ats_wms_ccm_pallet_count_detailsDataTableDT3[0].CCM_NAME,
                                ats_wms_ccm_pallet_count_detailsDataTableDT3[0].CCM_PALLET_COUNT,
                                ats_wms_ccm_pallet_count_detailsDataTableDT3[0].CCM_MACHINE_PALLET_COUNT,
                                currentShiftName,
                                DateTime.Now
                                );
                }
                catch (Exception ex)
                {
                    Log.Error("Exception Occured while entering new entry into CCM Pallet Count Report table", ex);
                }
                    Log.Debug("New entry inserted in Table");


            }
            catch (Exception ex)
            {
                Log.Error("Exception Occured in newEntryInsert() ", ex);
            }
        }
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

                    if(ats_wms_master_plc_connection_detailsDataTableDT != null && ats_wms_master_plc_connection_detailsDataTableDT.Count != 0)
                    {
                        plcServerConnectionString = ats_wms_master_plc_connection_detailsDataTableDT[0].PLC_CONNECTION_URL;

                        try
                        {
                            connectionStatus = OnConnectPLC();
                        }
                        catch (Exception ex)
                        {
                            Log.Error("2.1 :: Exception Occured while getting Setting Connection with PLC", ex);
                        }

                        if (connectionStatus)
                        {
                            loadCount();
              
                            ats_wms_ccm_pallet_count_detailsDataTableDT = ats_wms_ccm_pallet_count_detailsTableAdapterInstance.GetData();
                            for(int i=0; i< ats_wms_ccm_pallet_count_detailsDataTableDT.Count; i++)
                            {
                                if( !change(ats_wms_ccm_pallet_count_detailsDataTableDT[i].CCM_ID))
                                {
                                    Log.Debug("No change occured in the values of tags");
                                    await Task.Delay(1000, cancellationToken);
                                   // Log.Debug("Operation finished successfully.");
                                    continue;
                                }

                                insertNewEntry(ats_wms_ccm_pallet_count_detailsDataTableDT[i].CCM_ID);
                            }


                        }
                        else
                        {
                            Log.Debug("Connection not established.");
                        }
                    }
                    else
                    {
                        Log.Error("Entry not found in Master plc table ");
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

        private bool OnConnectPLC()
        {

            Log.Debug("OnConnectPLC :: inside OnConnectPLC");

            try
            {
                // Connection url
                if (!((ConnectedOpc.ServerState.ToString()).Equals("1")))
                {
                    ConnectedOpc.Connect(plcServerConnectionString, "");
                    Log.Debug("OnConnectPLC :: PLC connection successful and OPC server state is :: " + ConnectedOpc.ServerState.ToString());
                    return true;
                }
                else
                {
                    Log.Debug("OnConnectPLC :: Already connected with the plc.");
                    return true;
                }

            }
            catch (Exception ex)
            {
                Log.Error("OnConnectPLC :: Exception while connecting to plc :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
                return false; 
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
