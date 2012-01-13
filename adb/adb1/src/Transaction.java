import java.io.*;
import java.util.*;

public class Transaction implements Runnable {
   TransactionManager tm;
   String transactionId;
   String transOperation;
   boolean isReadOnly;
   public static volatile boolean isProcess=false;
   boolean die = false;
   int ageOfTransaction; // Value of Ticker when the Transaction Started
   Hashtable <Integer, Integer> sitesRead;      // SiteID and Last Read Time
   Hashtable <Integer, Integer> sitesWritten;   // SiteID and Last Write Time
   Vector <String> inputBuffer;
   boolean isOperationRO;

   public Transaction (String transactionId, boolean isReadOnly, TransactionManager tm) {
      this.tm = tm;
      this.transactionId = transactionId;
      this.isReadOnly = isReadOnly;
      ageOfTransaction = tm.ticker;
      if (ageOfTransaction == tm.ageOfLastTrans) {
         tm.advanceTicker();
         ageOfTransaction = tm.ticker;
         tm.ageOfLastTrans = ageOfTransaction;
      }
      sitesRead = new Hashtable <Integer, Integer> ();
      sitesWritten = new Hashtable <Integer, Integer> ();
      inputBuffer = new Vector <String> ();
      tm.dispMsg (transactionId, "Transaction Created " +this+ " (Sarting Ticker = " +ageOfTransaction+ ")" );
      if (isReadOnly) {
         for (int s1=1; s1 < tm.NOS; s1++) {
            Site site1 = tm.sites [s1];
            for (int v1=1; v1<tm.NOV; v1++) {
               site1.variables[v1].variableCopies.put (transactionId, site1.variables[v1].commitValue);
            }
            tm.dispMsg (transactionId, "Snapshot taken for Site" +s1);
         }
      }
   }

   public void run () {
      tm.dispMsg (transactionId, "Lock Manager Started");
      while (true) {
         if (die) {
            return;
         }
         synchronized (inputBuffer) {
            while (inputBuffer.size() == 0) {
               try {
                  tm.debugMsg (transactionId, "Waiting for Command in Input Buffer");
                  inputBuffer.wait();
               } catch (InterruptedException ie) {
                  tm.dispMsg (transactionId, "While Waiting for Input Buffer : " +ie);
               }
            }
            transOperation = inputBuffer.get (0);
            inputBuffer.remove(0);
            inputBuffer.notify();
         }
         isProcess=true;
         if (tm.DEBUG) {
            tm.dispMsg (transactionId, "Received Operation in the Input Buffer = " +transOperation);
         }
         
         performOperation (transOperation);
         isProcess=false;
      }
   }

   public void performOperation (String transOperation) {
      tm.debugMsg (transactionId, "Operation = " + transOperation);
      String operation = transOperation.substring (0, transOperation.indexOf ("("));
      transOperation = transOperation.substring (transOperation.indexOf ("(") +1);
      if (operation.equals ("r")) {
         boolean operationSuccess = false;
         transOperation = transOperation.substring (transOperation.indexOf (",") +1);
         int variableIndex = Integer.parseInt (transOperation.substring (1, transOperation.indexOf (")")));
         for (int s1=1; s1<tm.NOS; s1++) {   // Look for FIRST Definition of Variable from ANY Site (Site 0 onwards) until it is found
            if (tm.variableDefinedAtSite [variableIndex] [s1]) {  // check if variable is defined in the Site
               Site site1 = tm.sites [s1];
               int siteID = site1.siteID;
               if (site1.canRead (variableIndex, isReadOnly, transactionId)) {
                  if ( ! sitesRead.contains (s1) ) {
                     sitesRead.put (s1, tm.ticker);
                  }
                  Variable transVar = site1.variables [variableIndex];
                  if (isReadOnly) {
                     tm.dispMsg (transactionId, "Site" +siteID+ ".X" +variableIndex+ " = " +transVar.variableCopies.get(transactionId) + " Write Locked by ("+transVar.isWriteLockedBy+ ")");
                  } else {
                     int value;
                     String lockedBy = transVar.isWriteLockedBy;
                     if (lockedBy.equals (transactionId)) {
                        tm.debugMsg (transactionId, "has locked X" +variableIndex+ " - Reading Un-Committed value");
                        value = transVar.variableValue;
                     } else {
                    	 transVar.lockState = 1;
                    	 transVar.isReadLockedBy.add(transactionId);
                    	 System.out.println("lockstate at transaction level is "+site1.variables [variableIndex].lockState);
                        tm.debugMsg (transactionId, "has NOT locked X" +variableIndex+ " - Reading committed value");
                        value = transVar.commitValue;
                     }
                     tm.dispMsg (transactionId, "Site" +siteID+ ".X" +variableIndex+ " = " +transVar.commitValue+ " / " +value+ " = Write Locked by (" +lockedBy+ ")");
                  }
                  operationSuccess = true;
                  break;                                       // if variable is found in a site, stop looking further
               }
            }
         }
         if (operationSuccess) {
            tm.sleep (1, "Waiting for the FIRST Site to read variable", "While Waiting for the FIRST Site to read variable : ");
         } else {
            tm.dispMsg (transactionId, "Variable X" +variableIndex+ " NOT Available for READ !");
         }
      } else if (operation.equals ("w")) {
         if (isReadOnly) {
            tm.dispMsg (transactionId, "RO Transaction Trying to WRITE - But, NOT Allowed !");
         } else {

            String temp = transOperation.substring (0, transOperation.indexOf (","));
            transOperation = transOperation.substring (transOperation.indexOf (",") +1);
            int variableIndex = Integer.parseInt (transOperation.substring (1, transOperation.indexOf (",")));
            Integer lockVar = tm.allVariables [variableIndex];
            transOperation = transOperation.substring (transOperation.indexOf (",") +1);
            int value = Integer.parseInt (transOperation.substring (0, transOperation.indexOf (")")));
            tm.debugMsg (transactionId, "TransOperation = " +transOperation+ " variableIndex = " +variableIndex+ " lockVar = " +lockVar+ " value = " +value);
            synchronized (lockVar) {
               while (lockVar < 0) {   // Value : + means free, - means locked
                  try {
                     tm.debugMsg (transactionId, "Waiting to Update Variable X" +variableIndex);
                     lockVar.wait();
                  } catch (InterruptedException ie) {
                     tm.dispMsg (transactionId, "While Waiting to Update variable X" +variableIndex);
                  }
               }
               lockVar = -lockVar;   // Value : + means free, - means locked
               tm.debugMsg (transactionId, "Trying to Update Variable X" +variableIndex);
               for (int s1=1; s1 < tm.NOS; s1++) {
                  if ( ! tm.variableDefinedAtSite [variableIndex] [s1] ) {
                     continue;
                  }
                  Site site1 = tm.sites [s1];
                  Variable transVar = site1.variables [variableIndex];
                  if (site1.state != 4 && site1.state != 3) {
                     tm.dispMsg (transactionId, "Site" +s1+ " has failed !");
                  } else if (tm.variableDefinedAtSite [variableIndex] [s1]) {
                     if (site1.isVariableLockedByOther (transactionId, variableIndex, isReadOnly, value)) {
                        tm.dispMsg (transactionId, "Site" +s1+ ".X" +variableIndex+ " is Locked !");
                        tm.sleep (3000, transactionId+ " : Site" +s1+ ".X" +variableIndex+ " is Locked !",
                                        transactionId+ " : Site" +s1+ ".X" +variableIndex+ " is Locked !");
                        return;       // Skip, so that WRITE operation is repeated
                     } else {
                    	 
                    	if(!site1.activeTransactions.contains(transactionId))
                    		{site1.activeTransactions.add(transactionId);}
                        transVar.variableValue = value;
                        transVar.isWriteLockedBy = transactionId;
                        transVar.lockState = 2;
                        if ( ! sitesWritten.contains (s1) ) {    // site1 Testing
                           sitesWritten.put (s1, tm.ticker);
                        }
                        tm.dispMsg (transactionId, "Site" +s1+ ".X" +variableIndex+ " Updated");
                     }
                  }
               }
               lockVar = -lockVar;   // Value : + means free, - means locked
               lockVar.notifyAll();
            }
         }
      } else if (operation.equals ("end")) {
         boolean allSitesRunning = true;
         boolean committed = true;
   		Enumeration <Integer> writtenSites = sitesWritten.keys();
   		while (writtenSites.hasMoreElements()) {
			   int writtenSiteId = (Integer) writtenSites.nextElement();
			   Site writtenSite = tm.sites [writtenSiteId];
			   if (writtenSite.state != 3 &&  writtenSite.state != 4) {
               tm.dispMsg (transactionId, "Affected site" +writtenSiteId+ " NOT Running / Recovering");
 				   allSitesRunning = false;
               committed = false;
 				   break;
   			}
   		}
         int siteRunningId = -1;
         int sitesRecoveringNum = -1;         Site [] sitesRecovering = new Site [tm.NOS];
         for (int v1=1; v1<tm.NOV; v1++) {
            Integer lockVar = tm.allVariables [v1];
            synchronized (lockVar) {
               while (lockVar < 0) {   // Value : + means free, - means locked
                  try {
                     tm.debugMsg (transactionId, "Waiting for WRITE LOCK on Variable X" +v1);
                     lockVar.wait();
                  } catch (InterruptedException ie) {
                     tm.dispMsg (transactionId, "Interrupted while waiting for WRITE LOCK on variable X" +v1);
                  }
               }
               lockVar = -lockVar;   // Value : + means free, - means locked
               for (int s1=1; s1<tm.NOS; s1++) {
                  Site site1 = tm.sites [s1];
                  if (sitesWritten.contains (s1)                             // if affected site
                  &&  site1.state != 3 && site1.state != 4) {                // is failed
                     allSitesRunning = false;
                     if (committed) tm.dispMsg (transactionId, "Affected Site" +s1+ " NOT Recovering / Running");
                     committed = false;
                     continue;                                               //    skip the site
                  }
                  Variable transVar = site1.variables [v1];
                  if (isOperationRO) {                                          // if the ending transaction is READ ONLY
                     if (transVar != null)
                        transVar.variableCopies.remove (transactionId);         //    remove READ Copies of Variables
                  } else {
                     if (allSitesRunning) {
                        if (site1.state == 4) {                                    // if the site is Running
                           if (siteRunningId == -1) {
                              siteRunningId = s1;
                              //    remember (One / First) site from where to copy for recovery
                           }
                           if (transVar.isWriteLockedBy.equals (transactionId)) {     // if  the variable is locked by the THIS (ending) transaction
                              transVar.commitValue = transVar.variableValue;          //     commit changes - if Some sites are NOT available
                              transVar.isWriteLockedBy = new String ("");             //     release locks
                              transVar.lockState = 0;    
                              
                              //     reset lock state
                           }
                        } else if (site1.state == 3) {                                // if the site is in recovery state
                           sitesRecoveringNum++;                                      //    increment count of Recovering Sites
                           sitesRecovering [sitesRecoveringNum] = site1; 
                           
                           //    remeber Recovering Sites
                        }
                     } else {
                        if (transVar == null) {
                           tm.debugMsg (transactionId, "Test 1");
                           continue;
                           
                        }
                        if (transVar.isWriteLockedBy == null ) {
                           tm.debugMsg (transactionId, "Test 2 - " +transVar);
                           continue;
                        }
                        if (transVar.isWriteLockedBy.equals (transactionId)) {     // if  the variable is locked by the THIS (ending) transaction
                           transVar.variableValue = transVar.commitValue;          //     rollback changes - if Some sites are NOT available
                           transVar.isWriteLockedBy = new String ("");             //     release locks
                           transVar.lockState = 0;                                 //     reset lock state
                           committed = false;
                          }
                     }
                  }
               }
               
//             Here, ALL Running Sites are committed; now we take Recovering Sites
               if (allSitesRunning && committed && siteRunningId > -1) {  
            	   
            	   // if there are Sites Running for this transaction AND has to commit
                  if (sitesRecoveringNum > -1) {                              //    if there are Sites to be recovered
                	  System.out.println(transactionId+" : Committed !");
                     for (int s1 = 0; s1 <= sitesRecoveringNum; s1++) {
                        Site site1 = sitesRecovering [s1];
                        tm.putSiteOperation (site1, "COPY(" +siteRunningId+ ")");   // ask site to recover by Copying from Running Site
                        tm.sleep (2000, "Waiting for Site to Copy", "While Waiting for Site to Copy");
                     }
                  }
               }
               lockVar = -lockVar;   // Value : + means free, - means locked
               lockVar.notifyAll();
            }
         }
         if (committed) {
            tm.dispMsg (transactionId, "Committed !");
         } else {                                                      // No Site is Running with this transaction
            tm.dispMsg (transactionId, "Could NOT be Committed !");     // Actually Rolled back
         }
         tm.dispMsg (transactionId, "Ended !");
         tm.activeTransactions.remove (transactionId);
         die = true;
//         tm.sleep (1, transactionId+ " : Waiting to Die", transactionId+ " : While Waiting to Die");
      } else if (operation.equals ("ABORT")) {
         tm.dispMsg (transactionId, "Being Aborted !");
         for (int v1=1; v1<tm.NOV; v1++) {
            Integer lockVar = tm.allVariables [v1];
            synchronized (lockVar) {
               while (lockVar < 0) {   // Value : + means free, - means locked
                  try {
                     tm.debugMsg (transactionId, "Waiting for WRITE LOCK on Variable X" +v1+ " for abort");
                     lockVar.wait();
                  } catch (InterruptedException ie) {
                     tm.dispMsg (transactionId, "Interrupted while waiting for WRITE LOCK on variable X" +v1+ " for abort");
                  }
               }
               lockVar = -lockVar;   // Value : + means free, - means locked
               for (int s1=1; s1<tm.NOS; s1++) {
                  Site site1 = tm.sites [s1];
                  if (site1.state != 4 && site1.state != 3)               // if SIte is NOT Running
                     continue;                                            //    Skip the site
                  Variable transVar = site1.variables [v1];
                  if (isOperationRO) {                                    // if  the ending transaction is READ ONLY
                     transVar.variableCopies.remove (transactionId);      //     remove READ Copies of Variables
                  } else {                                                // if Transaction is NOT Read-Only
                     transVar.variableValue = transVar.commitValue;       //     Rollbak
                     transVar.isWriteLockedBy = new String ("");          //     release locks
                     transVar.lockState = 0;                              //     reset lock state
                  }
               }
               lockVar = -lockVar;   // Value : = means free, - means locked
               lockVar.notifyAll();
            }
         }
         tm.dispMsg (transactionId, "Rolled Back AND Aborted as a Site with Unique Variable has failed !");
         tm.activeTransactions.remove (transactionId);
         die = true;
//         tm.sleep (50, "While Waiting for Aborting Transaction " +transactionId,
//                       "While Waiting for Aborting Transaction " +transactionId);
      } else {
         tm.dispMsg (transactionId, "No Such Operation : " +operation);
      }
   }

}